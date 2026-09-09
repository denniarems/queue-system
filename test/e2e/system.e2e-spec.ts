import { INestApplication } from '@nestjs/common';
import request from 'supertest';
import { randomUUID } from 'node:crypto';
import { TracingService } from '../../src/tracing/tracing.service.js';
import { MockGatewayRegistry } from '../../src/gateway/mock-gateway.service.js';
import { RateLimiterRegistry } from '../../src/gateway/rate-limiter.registry.js';
import { createTestAppWithWebSocket, waitFor } from '../helpers/test-app.js';

/**
 * Ticket 08 — end-to-end verification: correlation propagation, OpenTelemetry
 * spans per lifecycle step, high-volume mixed-priority load across gateways
 * and chaos (rate limits + outages) with autonomous recovery + DLQ routing.
 */
describe('Ticket 08 — correlation propagation and OpenTelemetry spans', () => {
  let app: INestApplication;
  let http: ReturnType<INestApplication['getHttpServer']>;
  let tracing: TracingService;
  let gateways: MockGatewayRegistry;

  beforeAll(async () => {
    app = await createTestAppWithWebSocket((cfg) => {
      cfg.queue.backoffBaseMs = 30;
      cfg.queue.backoffJitter = 0;
      cfg.circuitBreaker = { windowMs: 5000, failureThreshold: 0.5, minSamples: 3, cooldownMs: 400 };
      return cfg;
    });
    http = app.getHttpServer();
    tracing = app.get(TracingService);
    gateways = app.get(MockGatewayRegistry);
    await app.listen(0);
  });

  afterAll(async () => {
    await app.close();
  });

  function payload(id: string, gatewayId: string, overrides: Record<string, unknown> = {}) {
    return { id, amount: 333, currency: 'USD', customerId: 'cust_e2e', gatewayId, ...overrides };
  }

  it('propagates X-Correlation-Id through HTTP, job, saga spans and the gateway call', async () => {
    const id = `pay_trace_${randomUUID()}`;
    const correlationId = `corr_e2e_${randomUUID()}`;

    const submit = await request(http)
      .post('/payments')
      .set('X-Correlation-Id', correlationId)
      .send(payload(id, 'stripe'))
      .expect(201);
    expect(submit.headers['x-correlation-id']).toBe(correlationId);

    await waitFor(async () => (await request(http).get(`/payments/${id}`)).body.status === 'completed', {
      label: 'traced payment completes',
    });

    // The correlation id travelled onto the stored record...
    const record = (await request(http).get(`/payments/${id}`)).body;
    expect(record.correlationId).toBe(correlationId);

    // ...onto the outbound gateway call...
    expect(gateways.get('stripe').stats.correlationIds).toContain(correlationId);

    // ...and onto every OpenTelemetry span of that payment's lifecycle.
    await waitFor(() => {
      const spans = tracing.listSpans();
      const lifecycle = ['payment.enqueue', 'payment.process', 'payment.saga.reserve', 'payment.saga.charge', 'payment.saga.settle'];
      return lifecycle.every((name) => spans.some((s) => s.name === name && s.attributes['paymentId'] === id));
    }, { label: 'lifecycle spans recorded' });

    const spans = tracing.listSpans().filter((s) => s.attributes['paymentId'] === id);
    for (const name of ['payment.enqueue', 'payment.process', 'payment.saga.reserve', 'payment.saga.charge', 'payment.saga.settle']) {
      const span = spans.find((s) => s.name === name);
      expect(span, `span ${name}`).toBeDefined();
      expect(span!.attributes['correlationId']).toBe(correlationId);
    }
  });

  it('recovers from chaos autonomously: outage -> DLQ -> heal -> breaker closes -> traffic flows', async () => {
    // ---- chaos 1: sustained outage on chaosgw ----
    gateways.configure('chaosgw', {
      latencyMinMs: 2,
      latencyMaxMs: 2,
      steps: [],
      after: { kind: 'fail', httpStatus: 503 },
    });

    const dead = `pay_chaos_dead_${randomUUID()}`;
    await request(http).post('/payments').send(payload(dead, 'chaosgw', { maxRetries: 2 })).expect(201);
    await waitFor(async () => (await request(http).get(`/payments/${dead}`)).body.status === 'dead_letter', {
      label: 'outage payment dead-letters',
      timeoutMs: 20_000,
    });

    // The DLQ route happens right after the record's terminal save; poll for it.
    await waitFor(async () => {
      const dlq = await request(http).get('/queues/dlq?gatewayId=chaosgw').expect(200);
      return dlq.body.entries.some((e: { paymentId: string }) => e.paymentId === dead);
    }, { label: 'dead letter visible in DLQ', timeoutMs: 10_000 });

    // The dead-lettering produced a compensation + DLQ span.
    await waitFor(() => {
      const spans = tracing.listSpans();
      return (
        spans.some((s) => s.name === 'payment.saga.compensation' && s.attributes['paymentId'] === dead) &&
        spans.some((s) => s.name === 'payment.dead_letter' && s.attributes['paymentId'] === dead)
      );
    }, { label: 'compensation + DLQ spans recorded' });

    // ---- chaos 2: heal the gateway ----
    gateways.configure('chaosgw', { latencyMinMs: 2, latencyMaxMs: 2, steps: [], after: { kind: 'ok' } });
    const healed = `pay_chaos_ok_${randomUUID()}`;
    await request(http).post('/payments').send(payload(healed, 'chaosgw', { maxRetries: 7 })).expect(201);
    await waitFor(async () => (await request(http).get(`/payments/${healed}`)).body.status === 'completed', {
      label: 'payment completes after outage heals (breaker probe passes)',
      timeoutMs: 30_000,
    });

    // ---- chaos 3: rate-limit burst throttles but loses nothing ----
    gateways.configure('rlchaos', { latencyMinMs: 3, latencyMaxMs: 3 });
    app.get(RateLimiterRegistry).configure('rlchaos', { nominalRps: 6, burstFactor: 0.5 });

    const burst = Array.from({ length: 8 }, () => `pay_burst_${randomUUID()}`);
    for (const id of burst) {
      await request(http).post('/payments').send(payload(id, 'rlchaos')).expect(201);
    }
    await waitFor(
      async () => {
        const statuses = await Promise.all(burst.map((id) => request(http).get(`/payments/${id}`)));
        return statuses.every((r) => r.body.status === 'completed');
      },
      { label: 'rate-limited burst completes', timeoutMs: 30_000 },
    );
    const dlqAfter = await request(http).get('/queues/dlq?gatewayId=rlchaos').expect(200);
    expect(dlqAfter.body.entries.length).toBe(0);

    // ---- load: 30 mixed-priority payments across three healthy gateways ----
    const load = Array.from({ length: 30 }, (_, i) => {
      const gatewayId = ['stripe', 'paypal', 'adyen'][i % 3];
      const priority = (['high', 'normal', 'low'] as const)[i % 3];
      return payload(`pay_load_${randomUUID()}`, gatewayId, { priority });
    });
    for (const body of load) {
      await request(http).post('/payments').send(body).expect(201);
    }
    await waitFor(
      async () => {
        const statuses = await Promise.all(load.map((b) => request(http).get(`/payments/${b.id}`)));
        return statuses.every((r) => r.body.status === 'completed');
      },
      { label: '30-payment mixed-priority load completes', timeoutMs: 45_000 },
    );

    const metrics = (await request(http).get('/queues/metrics').expect(200)).body;
    expect(metrics.attempts.ok).toBeGreaterThanOrEqual(40);
    expect(metrics.queueDepths.gateways.length).toBeGreaterThanOrEqual(4);
  }, 90_000);
});

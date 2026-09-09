import { INestApplication } from '@nestjs/common';
import request from 'supertest';
import { randomUUID } from 'node:crypto';
import { CircuitBreakerRegistry } from '../../src/gateway/circuit-breaker.registry.js';
import { MockGatewayRegistry } from '../../src/gateway/mock-gateway.service.js';
import { PaymentStore } from '../../src/payments/payment-store.service.js';
import { IdempotencyService } from '../../src/payments/idempotency.service.js';
import { createTestApp, waitFor } from '../helpers/test-app.js';

/**
 * Ticket 05 — error classification, retry policy and the Dead Letter Queue.
 * Permanent rejections never retry; transient failures retry with exponential
 * backoff up to maxRetries, then land in `payments:dlq` with the reason kept.
 */
describe('Ticket 05 — error classification and DLQ routing', () => {
  let app: INestApplication;
  let http: ReturnType<INestApplication['getHttpServer']>;
  let gateways: MockGatewayRegistry;
  let breakers: CircuitBreakerRegistry;
  let store: PaymentStore;
  let idempotency: IdempotencyService;

  beforeAll(async () => {
    app = await createTestApp((cfg) => {
      cfg.queue.backoffBaseMs = 60;
      cfg.queue.backoffJitter = 0;
      return cfg;
    });
    http = app.getHttpServer();
    gateways = app.get(MockGatewayRegistry);
    breakers = app.get(CircuitBreakerRegistry);
    store = app.get(PaymentStore);
    idempotency = app.get(IdempotencyService);
  });

  afterAll(async () => {
    await app.close();
  });

  function body(id: string, gatewayId: string, maxRetries = 3) {
    return { id, amount: 500, currency: 'EUR', customerId: 'cust_dlq', gatewayId, maxRetries };
  }

  it('permanent rejections (e.g. invalid card) fail immediately into the DLQ without retrying', async () => {
    const id = `pay_perm_${randomUUID()}`;
    gateways.configure('permw', {
      latencyMinMs: 2,
      latencyMaxMs: 2,
      steps: [],
      after: { kind: 'fail', httpStatus: 400, retryable: false },
    });

    await request(http).post('/payments').send(body(id, 'permw')).expect(201);
    await waitFor(async () => (await store.get(id))?.status === 'dead_letter', {
      label: 'permanent failure dead-letters',
      timeoutMs: 10_000,
    });

    const record = await store.get(id);
    expect(record?.status).toBe('dead_letter');
    expect(record?.failureReason).toContain('HTTP 400');
    expect(record?.retryCount).toBe(0);
    // Exactly one gateway call: no retries for permanent failures.
    expect(gateways.get('permw').stats.charges).toBe(1);

    // Business rejections are not provider degradation: breaker stays CLOSED.
    expect(breakers.get('permw').getState().state).toBe('CLOSED');
    expect(breakers.get('permw').getState().samples).toBe(0);

    // DLQ endpoint returns the dead letter for inspection.
    const dlq = await request(http).get(`/queues/dlq?gatewayId=permw`).expect(200);
    const entry = dlq.body.entries.find((e: { paymentId: string }) => e.paymentId === id);
    expect(entry).toBeDefined();
    expect(entry.reason).toContain('HTTP 400');
    expect(entry.payment.status).toBe('dead_letter');

    const idem = await idempotency.get(id);
    expect(idem?.state).toBe('FAILED');
  });

  it('transient failures retry up to maxRetries with backoff, then land in the DLQ with the reason preserved', async () => {
    const id = `pay_retr_${randomUUID()}`;
    gateways.configure('exgw', {
      latencyMinMs: 2,
      latencyMaxMs: 2,
      steps: [],
      after: { kind: 'fail', httpStatus: 503 },
    });

    await request(http).post('/payments').send(body(id, 'exgw', 2)).expect(201); // maxRetries=2 -> 3 attempts
    const startedAt = Date.now();
    await waitFor(async () => (await store.get(id))?.status === 'dead_letter', {
      label: 'exhausted retries dead-letter',
      timeoutMs: 15_000,
    });

    const record = await store.get(id);
    expect(record?.status).toBe('dead_letter');
    expect(record?.retryCount).toBe(2);
    expect(record?.failureReason).toContain('retries exhausted');
    expect(record?.failureReason).toContain('HTTP 503');
    // Attempt 1 + 2 backoff retries = exactly 3 gateway calls, never more.
    expect(gateways.get('exgw').stats.charges).toBe(3);
    expect(Date.now() - startedAt).toBeGreaterThanOrEqual(150); // exponential backoff applied

    const dlq = await request(http).get(`/queues/dlq?gatewayId=exgw`).expect(200);
    const entry = dlq.body.entries.find((e: { paymentId: string }) => e.paymentId === id);
    expect(entry).toBeDefined();
    expect(entry.reason).toContain('retries exhausted');
  }, 20_000);

  it('a transient failure that heals recovers inside the retry budget without touching the DLQ', async () => {
    const id = `pay_heal_${randomUUID()}`;
    gateways.configure('healgw', {
      latencyMinMs: 2,
      latencyMaxMs: 2,
      steps: [{ kind: 'fail', httpStatus: 500 }], // one transient blip...
      after: { kind: 'ok' }, // ...then healthy
    });

    await request(http).post('/payments').send(body(id, 'healgw')).expect(201);
    await waitFor(async () => (await store.get(id))?.status === 'completed', {
      label: 'payment heals and completes',
      timeoutMs: 10_000,
    });

    const record = await store.get(id);
    expect(record?.status).toBe('completed');
    expect(record?.retryCount).toBe(1); // one retry happened
    expect(record?.transactionId).toBeDefined();
    expect(gateways.get('healgw').stats.charges).toBe(2);

    const dlq = await request(http).get(`/queues/dlq?gatewayId=healgw`).expect(200);
    expect(dlq.body.entries.length).toBe(0);
  });
});

import { INestApplication } from '@nestjs/common';
import request from 'supertest';
import { randomUUID } from 'node:crypto';
import { CircuitBreaker } from '../../src/gateway/circuit-breaker.js';
import { CircuitBreakerParams } from '../../src/gateway/circuit-breaker.js';
import { CircuitBreakerRegistry } from '../../src/gateway/circuit-breaker.registry.js';
import { MockGatewayRegistry } from '../../src/gateway/mock-gateway.service.js';
import { EventBus } from '../../src/common/event-bus.js';
import { createTestApp, waitFor } from '../helpers/test-app.js';

function params(overrides: Partial<CircuitBreakerParams> = {}): CircuitBreakerParams {
  return { windowMs: 60_000, failureThreshold: 0.5, minSamples: 3, cooldownMs: 1000, ...overrides };
}

describe('Ticket 05 — CircuitBreaker (unit): state machine', () => {
  it('trips OPEN after the failure threshold is breached, then fast-fails', () => {
    const now = { t: 10_000 };
    const breaker = new CircuitBreaker(params(), {}, () => now.t);
    breaker.recordOutcome(false);
    breaker.recordOutcome(false);
    expect(breaker.allowCall().allowed).toBe(true); // not enough samples yet
    breaker.recordOutcome(false);
    expect(breaker.getState().state).toBe('OPEN');
    expect(breaker.allowCall()).toEqual({ allowed: false, state: 'OPEN' });
    expect(breaker.getState().tripCount).toBe(1);
  });

  it('after the cooldown a single HALF_OPEN probe runs; success closes the breaker', () => {
    const now = { t: 10_000 };
    const breaker = new CircuitBreaker(params(), {}, () => now.t);
    breaker.recordOutcome(false);
    breaker.recordOutcome(false);
    breaker.recordOutcome(false); // OPEN
    now.t += 1001;
    expect(breaker.allowCall().state).toBe('HALF_OPEN');
    expect(breaker.allowCall().allowed).toBe(false); // probe already in flight
    breaker.recordOutcome(true);
    expect(breaker.getState().state).toBe('CLOSED');
    expect(breaker.allowCall().allowed).toBe(true);
  });

  it('a failed HALF_OPEN probe reopens the breaker', () => {
    const now = { t: 10_000 };
    const breaker = new CircuitBreaker(params(), {}, () => now.t);
    breaker.recordOutcome(false);
    breaker.recordOutcome(false);
    breaker.recordOutcome(false); // OPEN
    now.t += 1001;
    breaker.allowCall();
    breaker.recordOutcome(false); // probe failed
    expect(breaker.getState().state).toBe('OPEN');
    expect(breaker.getState().tripCount).toBe(2);
  });

  it('samples expire from the sliding window', () => {
    const now = { t: 10_000 };
    const breaker = new CircuitBreaker(params({ windowMs: 5000, minSamples: 3 }), {}, () => now.t);
    breaker.recordOutcome(false);
    now.t += 6000; // the failure above ages out of the window
    breaker.recordOutcome(false);
    breaker.recordOutcome(false);
    expect(breaker.getState().state).toBe('CLOSED'); // expired sample not counted
    expect(breaker.getState().samples).toBe(2);
    breaker.recordOutcome(false); // 3 fresh failures now breach the threshold
    expect(breaker.getState().state).toBe('OPEN');
  });
});

describe('Ticket 05 — breaker integration: trips on 5xx, fast-fails without gateway calls, recovers on HALF_OPEN probe', () => {
  let app: INestApplication;
  let http: ReturnType<INestApplication['getHttpServer']>;
  let breakers: CircuitBreakerRegistry;
  let gateways: MockGatewayRegistry;
  let opened: string[];
  let closed: string[];

  beforeAll(async () => {
    app = await createTestApp((cfg) => {
      cfg.queue.backoffBaseMs = 10;
      cfg.queue.backoffJitter = 0;
      cfg.circuitBreaker = { windowMs: 5000, failureThreshold: 0.5, minSamples: 3, cooldownMs: 500 };
      return cfg;
    });
    http = app.getHttpServer();
    breakers = app.get(CircuitBreakerRegistry);
    gateways = app.get(MockGatewayRegistry);
    const bus = app.get(EventBus);
    opened = [];
    closed = [];
    bus.on('circuit.opened', (e) => {
      if (e.type === 'circuit.opened') opened.push(e.gatewayId);
    });
    bus.on('circuit.closed', (e) => {
      if (e.type === 'circuit.closed') closed.push(e.gatewayId);
    });
    gateways.configure('brgw', {
      latencyMinMs: 2,
      latencyMaxMs: 2,
      steps: [],
      after: { kind: 'fail', httpStatus: 503 },
    });
    breakers.configure('brgw', { minSamples: 3, failureThreshold: 0.5, cooldownMs: 500, windowMs: 5000 });
  });

  afterAll(async () => {
    await app.close();
  });

  function paymentBody(id: string, maxRetries: number) {
    return { id, amount: 999, currency: 'USD', customerId: 'cust_cb', gatewayId: 'brgw', maxRetries };
  }

  it('dead-letters after outage + retries, fast-failing most attempts without touching the gateway', async () => {
    const p = `pay_br_p_${randomUUID()}`;
    await request(http).post('/payments').send(paymentBody(p, 7)).expect(201); // attempts = 8

    await waitFor(
      async () => (await request(http).get(`/payments/${p}`)).body.status === 'dead_letter',
      { label: 'payment dead-lettered after sustained 503 outage', timeoutMs: 20_000 },
    );

    const record = (await request(http).get(`/payments/${p}`)).body;
    expect(record.failureReason).toContain('retries exhausted');
    // Backoff 10ms doubles per attempt; only ~5 gateway calls should have
    // happened (3 to trip + 2 HALF_OPEN probes) while ~3 attempts fast-failed.
    const stats = gateways.get('brgw').stats;
    expect(stats.httpStatusHistogram[503]).toBe(5);
    expect(stats.charges).toBe(5);
    expect(opened.length).toBeGreaterThanOrEqual(3);
  }, 25_000);

  it('recovers once the gateway heals: HALF_OPEN probe succeeds and the breaker closes', async () => {
    gateways.configure('brgw', { latencyMinMs: 2, latencyMaxMs: 2, steps: [], after: { kind: 'ok' } });

    const q = `pay_br_q_${randomUUID()}`;
    await request(http).post('/payments').send(paymentBody(q, 7)).expect(201);

    await waitFor(
      async () => (await request(http).get(`/payments/${q}`)).body.status === 'completed',
      { label: 'payment completes after recovery', timeoutMs: 20_000 },
    );
    await waitFor(() => breakers.get('brgw').getState().state === 'CLOSED', {
      label: 'breaker closes after probe success',
    });

    const stats = gateways.get('brgw').stats;
    expect(stats.httpStatusHistogram[200]).toBe(1); // exactly one probe charge
    expect(stats.httpStatusHistogram[503]).toBe(5); // outage calls unchanged
    expect(closed.length).toBeGreaterThanOrEqual(1);
  }, 25_000);
});

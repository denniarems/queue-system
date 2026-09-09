import { INestApplication } from '@nestjs/common';
import request from 'supertest';
import { randomUUID } from 'node:crypto';
import { AdaptiveTokenBucket, TokenBucketParams } from '../../src/gateway/adaptive-token-bucket.js';
import { RateLimiterRegistry } from '../../src/gateway/rate-limiter.registry.js';
import { MockGatewayRegistry } from '../../src/gateway/mock-gateway.service.js';
import { createTestApp, waitFor } from '../helpers/test-app.js';

function params(overrides: Partial<TokenBucketParams> = {}): TokenBucketParams {
  return {
    nominalRps: 8,
    burstFactor: 1,
    minRateFactor: 0.25,
    aiStepRps: 1,
    tokenWaitMs: 15_000,
    ...overrides,
  };
}

describe('Ticket 04 — AdaptiveTokenBucket (unit): AIMD mechanics', () => {
  it('enforces the burst ceiling and refills over time', async () => {
    const bucket = new AdaptiveTokenBucket(params({ nominalRps: 4, burstFactor: 1 }));
    // burst = nominalRps * burstFactor = 4 tokens.
    expect(bucket.tryAcquire()).toBe(true);
    expect(bucket.tryAcquire()).toBe(true);
    expect(bucket.tryAcquire()).toBe(true);
    expect(bucket.tryAcquire()).toBe(true);
    expect(bucket.tryAcquire()).toBe(false);
    await new Promise((r) => setTimeout(r, 450)); // ~1.8 tokens refilled at 4/s
    expect(await bucket.waitForToken(3000)).toBe(true);
  });

  it('multiplicative decrease halves the rate and floors at minRateFactor', () => {
    const bucket = new AdaptiveTokenBucket(params({ nominalRps: 8, minRateFactor: 0.25 }));
    expect(bucket.getState().rate).toBe(8);
    bucket.onThrottled(); // 8 -> 4
    bucket.onThrottled(); // 4 -> 2
    bucket.onThrottled(); // 2 -> max(2, 1) = 2 (floor)
    const state = bucket.getState();
    expect(state.rate).toBe(2);
    expect(state.throttleCount).toBe(3);
  });

  it('additive increase recovers the rate back to nominal on consecutive successes', () => {
    const bucket = new AdaptiveTokenBucket(params({ nominalRps: 8, aiStepRps: 2, burstFactor: 2 }));
    bucket.onThrottled(); // 8 -> 4
    expect(bucket.getState().rate).toBe(4);
    bucket.onSuccess(); // 6
    bucket.onSuccess(); // 8 (capped at nominal)
    bucket.onSuccess(); // stays 8
    const state = bucket.getState();
    expect(state.rate).toBe(8);
    expect(state.burst).toBe(16); // burst ceiling restored too
  });
});

describe('Ticket 04 — rate limiter gates dispatch without losing jobs', () => {
  let app: INestApplication;
  let http: ReturnType<INestApplication['getHttpServer']>;

  beforeAll(async () => {
    app = await createTestApp((cfg) => {
      cfg.queue.backoffBaseMs = 40;
      return cfg;
    });
    http = app.getHttpServer();
    const gateways = app.get(MockGatewayRegistry);
    const limiters = app.get(RateLimiterRegistry);
    gateways.configure('rlimA', { latencyMinMs: 3, latencyMaxMs: 3 });
    limiters.configure('rlimA', { nominalRps: 2, burstFactor: 0.5 });
  });

  afterAll(async () => {
    await app.close();
  });

  it('spaces a 6-payment burst to ~1 per 500ms and completes every job', async () => {
    const ids = Array.from({ length: 6 }, () => `pay_rlim_${randomUUID()}`);
    const startedAt = Date.now();
    for (const id of ids) {
      await request(http)
        .post('/payments')
        .send({ id, amount: 300, currency: 'USD', customerId: 'cust_r', gatewayId: 'rlimA' })
        .expect(201);
    }

    await waitFor(
      async () => {
        const statuses = await Promise.all(ids.map((id) => request(http).get(`/payments/${id}`).then((r) => r.body.status)));
        return statuses.every((s) => s === 'completed');
      },
      { label: 'all rate-limited payments complete', timeoutMs: 25_000 },
    );

    // 6 payments at 2 tokens/s with a 1-token burst: the last one needs ~2.5s.
    const elapsed = Date.now() - startedAt;
    expect(elapsed).toBeGreaterThanOrEqual(2200);

    const dlq = await request(http).get('/queues/dlq').expect(200);
    expect(dlq.body.count).toBe(0);
  }, 30_000);
});

describe('Ticket 04 — 429 responses trigger MD, consecutive successes trigger AI recovery', () => {
  let app: INestApplication;
  let http: ReturnType<INestApplication['getHttpServer']>;
  let limiters: RateLimiterRegistry;
  let gateways: MockGatewayRegistry;

  beforeAll(async () => {
    app = await createTestApp((cfg) => {
      cfg.queue.backoffBaseMs = 40;
      return cfg;
    });
    http = app.getHttpServer();
    limiters = app.get(RateLimiterRegistry);
    gateways = app.get(MockGatewayRegistry);
    gateways.configure('rlimB', {
      latencyMinMs: 2,
      latencyMaxMs: 2,
      steps: [
        { kind: 'fail', httpStatus: 429 },
        { kind: 'fail', httpStatus: 429 },
      ],
      after: { kind: 'ok' },
    });
    limiters.configure('rlimB', { nominalRps: 8, burstFactor: 4, aiStepRps: 1 });
  });

  afterAll(async () => {
    await app.close();
  });

  it('two 429 responses halve the refill rate; successes restore it to nominal', async () => {
    const ids = Array.from({ length: 10 }, () => `pay_aimd_${randomUUID()}`);
    for (const id of ids) {
      await request(http)
        .post('/payments')
        .send({ id, amount: 300, currency: 'USD', customerId: 'cust_a', gatewayId: 'rlimB' })
        .expect(201);
    }
    await waitFor(
      async () => {
        const statuses = await Promise.all(ids.map((id) => request(http).get(`/payments/${id}`).then((r) => r.body.status)));
        return statuses.every((s) => s === 'completed');
      },
      { label: 'AIMD payments complete', timeoutMs: 30_000 },
    );

    const bucket = limiters.get('rlimB').getState();
    expect(bucket.throttleCount).toBe(2); // exactly the two scripted 429s
    expect(gateways.get('rlimB').stats.httpStatusHistogram[429]).toBe(2);
    expect(bucket.rate).toBe(8); // additive increase recovered to nominal
  }, 35_000);
});

describe('Ticket 04 — one gateway throttling never affects another gateway', () => {
  let app: INestApplication;
  let http: ReturnType<INestApplication['getHttpServer']>;
  let limiters: RateLimiterRegistry;
  let gateways: MockGatewayRegistry;

  beforeAll(async () => {
    app = await createTestApp();
    http = app.getHttpServer();
    limiters = app.get(RateLimiterRegistry);
    gateways = app.get(MockGatewayRegistry);
    gateways.configure('limC', { latencyMinMs: 5, latencyMaxMs: 5 });
    gateways.configure('limD', { latencyMinMs: 5, latencyMaxMs: 5 });
    limiters.configure('limC', { nominalRps: 1, burstFactor: 1 });
    limiters.configure('limD', { nominalRps: 40, burstFactor: 20 });
  });

  afterAll(async () => {
    await app.close();
  });

  it('unthrottled gateway D finishes while throttled gateway C still queues its second job', async () => {
    const [c1, c2] = [`pay_c_${randomUUID()}`, `pay_c_${randomUUID()}`];
    const ds = Array.from({ length: 4 }, () => `pay_d_${randomUUID()}`);
    for (const id of [c1, c2, ...ds]) {
      await request(http)
        .post('/payments')
        .send({ id, amount: 100, currency: 'EUR', customerId: 'cust_iso', gatewayId: id.startsWith('pay_c_') ? 'limC' : 'limD' })
        .expect(201);
    }

    await waitFor(
      async () => {
        const statuses = await Promise.all(ds.map((id) => request(http).get(`/payments/${id}`).then((r) => r.body.status)));
        return statuses.every((s) => s === 'completed');
      },
      { label: 'gateway D completes quickly', timeoutMs: 10_000 },
    );

    // limC is capped at 1 token/s: its second job cannot be done yet (~1s wait).
    expect((await request(http).get(`/payments/${c2}`)).body.status).not.toBe('completed');

    await waitFor(
      async () => {
        const statuses = await Promise.all([c1, c2].map((id) => request(http).get(`/payments/${id}`).then((r) => r.body.status)));
        return statuses.every((s) => s === 'completed');
      },
      { label: 'gateway C eventually drains', timeoutMs: 10_000 },
    );

    const dDone = Date.parse((await request(http).get(`/payments/${ds[3]}`)).body.completedAt);
    const c2Done = Date.parse((await request(http).get(`/payments/${c2}`)).body.completedAt);
    expect(c2Done - dDone).toBeGreaterThanOrEqual(500);
  }, 20_000);
});

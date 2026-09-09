import { INestApplication } from '@nestjs/common';
import request from 'supertest';
import { randomUUID } from 'node:crypto';
import { Redis } from 'ioredis';
import { APP_CONFIG, AppConfig } from '../../src/config/app-config.js';
import { MockGatewayRegistry } from '../../src/gateway/mock-gateway.service.js';
import { QueueManager } from '../../src/queue/queue-manager.service.js';
import { createTestApp, waitFor } from '../helpers/test-app.js';

type Payload = { id: string; amount: number; currency: string; customerId: string; gatewayId: string };

function payload(id: string, gatewayId: string): Payload {
  return { id, amount: 700, currency: 'USD', customerId: 'cust_q', gatewayId };
}

async function getPayment(app: INestApplication, id: string): Promise<{ status: string; transactionId?: string; completedAt?: string }> {
  const res = await request(app.getHttpServer()).get(`/payments/${id}`).expect(200);
  return res.body;
}

/** Extract the per-gateway charge sequence number embedded in mock txn ids. */
function chargeIndex(transactionId: string): number {
  return Number(transactionId.split('_').at(-1));
}

describe('Ticket 02 — priorities: high overtakes normal/low within a gateway queue', () => {
  let app: INestApplication;
  let manager: QueueManager;

  beforeAll(async () => {
    // Start with an empty worker pool: nothing may process until we scale up.
    app = await createTestApp((cfg) => {
      cfg.queue.workerPoolSize = 0;
      return cfg;
    });
    manager = app.get(QueueManager);
    app.get(MockGatewayRegistry).configure('priogw', { latencyMinMs: 15, latencyMaxMs: 15 });
  });

  afterAll(async () => {
    await app.close();
  });

  it('processes high before normal before low although submitted last-first', async () => {
    const low = `pay_low_${randomUUID()}`;
    const normal = `pay_norm_${randomUUID()}`;
    const high = `pay_high_${randomUUID()}`;
    for (const [id, priority] of [
      [low, 'low'],
      [normal, 'normal'],
      [high, 'high'],
    ] as const) {
      await request(app.getHttpServer()).post('/payments').send({ ...payload(id, 'priogw'), priority }).expect(201);
    }

    // No workers yet: everything must still be queued.
    await new Promise((r) => setTimeout(r, 300));
    for (const id of [low, normal, high]) {
      expect((await getPayment(app, id)).status).toBe('queued');
    }

    await manager.setWorkerPoolSize('priogw', 1);
    await waitFor(async () => {
      const statuses = await Promise.all([low, normal, high].map((id) => getPayment(app, id)));
      return statuses.every((s) => s.status === 'completed');
    }, { label: 'all priority payments complete' });

    const lowOrder = chargeIndex((await getPayment(app, low)).transactionId!);
    const normalOrder = chargeIndex((await getPayment(app, normal)).transactionId!);
    const highOrder = chargeIndex((await getPayment(app, high)).transactionId!);
    expect(highOrder).toBeLessThan(normalOrder);
    expect(normalOrder).toBeLessThan(lowOrder);
  });
});

describe('Ticket 02 — scheduling: delayed and future-dated payments stay deferred', () => {
  let app: INestApplication;
  let http: ReturnType<INestApplication['getHttpServer']>;

  beforeAll(async () => {
    app = await createTestApp();
    http = app.getHttpServer();
  });

  afterAll(async () => {
    await app.close();
  });

  it('delayMs defers execution until the target timestamp', async () => {
    const id = `pay_delay_${randomUUID()}`;
    const startedAt = Date.now();
    await request(http)
      .post('/payments/scheduled')
      .send({ ...payload(id, 'stripe'), delayMs: 1200 })
      .expect(201);

    await new Promise((r) => setTimeout(r, 350));
    expect((await getPayment(app, id)).status).toBe('queued');

    await waitFor(async () => (await getPayment(app, id)).status === 'completed', {
      label: 'delayed payment completes',
      timeoutMs: 10_000,
    });
    const elapsed = Date.now() - startedAt;
    expect(elapsed).toBeGreaterThanOrEqual(1100);
  });

  it('scheduledAt (ISO) executes at the requested wall-clock time', async () => {
    const id = `pay_sched_${randomUUID()}`;
    const scheduledAt = new Date(Date.now() + 800).toISOString();
    const res = await request(http)
      .post('/payments/scheduled')
      .send({ ...payload(id, 'paypal'), scheduledAt })
      .expect(201);
    expect(new Date(res.body.scheduledFor).getTime()).toBeGreaterThan(Date.now());

    await new Promise((r) => setTimeout(r, 250));
    expect((await getPayment(app, id)).status).toBe('queued');

    await waitFor(async () => (await getPayment(app, id)).status === 'completed', {
      label: 'scheduledAt payment completes',
      timeoutMs: 10_000,
    });
  });
});

describe('Ticket 02 — gateway isolation: a slow gateway never blocks healthy ones', () => {
  let app: INestApplication;
  let http: ReturnType<INestApplication['getHttpServer']>;

  beforeAll(async () => {
    app = await createTestApp();
    http = app.getHttpServer();
    const registry = app.get(MockGatewayRegistry);
    registry.configure('slowgw', { latencyMinMs: 400, latencyMaxMs: 400 });
    registry.configure('fastgw', { latencyMinMs: 5, latencyMaxMs: 5 });
  });

  afterAll(async () => {
    await app.close();
  });

  it('fast gateway payment completes while the slow gateway payment is still in flight', async () => {
    const slow = `pay_slow_${randomUUID()}`;
    const fast = `pay_fast_${randomUUID()}`;
    await request(http).post('/payments').send(payload(slow, 'slowgw')).expect(201);
    await request(http).post('/payments').send(payload(fast, 'fastgw')).expect(201);

    await waitFor(async () => (await getPayment(app, fast)).status === 'completed', {
      label: 'fast gateway completes',
      timeoutMs: 10_000,
    });
    // Slow gateway needs ~400ms minimum; the fast one finished already.
    expect((await getPayment(app, slow)).status).not.toBe('completed');

    await waitFor(async () => (await getPayment(app, slow)).status === 'completed', {
      label: 'slow gateway eventually completes',
      timeoutMs: 10_000,
    });
    const fastDone = Date.parse((await getPayment(app, fast)).completedAt!);
    const slowDone = Date.parse((await getPayment(app, slow)).completedAt!);
    expect(fastDone).toBeLessThan(slowDone);
  });
});

describe('Ticket 02 — dynamic worker pools scale per gateway without losing jobs', () => {
  let app: INestApplication;
  let manager: QueueManager;
  let http: ReturnType<INestApplication['getHttpServer']>;

  beforeAll(async () => {
    app = await createTestApp((cfg) => {
      cfg.queue.workerPoolSize = 0;
      return cfg;
    });
    manager = app.get(QueueManager);
    http = app.getHttpServer();
    app.get(MockGatewayRegistry).configure('poolgw', { latencyMinMs: 30, latencyMaxMs: 30 });
  });

  afterAll(async () => {
    await app.close();
  });

  it('scale up drains the backlog, scale down to zero pauses consumption', async () => {
    const ids = Array.from({ length: 5 }, () => `pay_pool_${randomUUID()}`);
    for (const id of ids) {
      await request(http).post('/payments').send(payload(id, 'poolgw')).expect(201);
    }
    await new Promise((r) => setTimeout(r, 200));
    for (const id of ids) {
      expect((await getPayment(app, id)).status).toBe('queued');
    }

    expect(manager.getWorkerPoolSize('poolgw')).toBe(0);
    await manager.setWorkerPoolSize('poolgw', 3);
    expect(manager.getWorkerPoolSize('poolgw')).toBe(3);

    await waitFor(
      async () => {
        const statuses = await Promise.all(ids.map((id) => getPayment(app, id)));
        return statuses.every((s) => s.status === 'completed');
      },
      { label: 'backlog drained after scaling up', timeoutMs: 15_000 },
    );

    // Scale back to zero: new jobs stay queued but are not lost.
    await manager.setWorkerPoolSize('poolgw', 0);
    const extra = `pay_pool_extra_${randomUUID()}`;
    await request(http).post('/payments').send(payload(extra, 'poolgw')).expect(201);
    await new Promise((r) => setTimeout(r, 250));
    expect((await getPayment(app, extra)).status).toBe('queued');

    await manager.setWorkerPoolSize('poolgw', 1);
    await waitFor(async () => (await getPayment(app, extra)).status === 'completed', {
      label: 'job resumes once a worker returns',
      timeoutMs: 10_000,
    });
  });
});

describe('Ticket 02 — graceful shutdown lets in-flight jobs finish', () => {
  let app: INestApplication;
  let config: AppConfig;

  beforeAll(async () => {
    app = await createTestApp((cfg) => {
      cfg.queue.workerPoolSize = 1;
      return cfg;
    });
    config = app.get(APP_CONFIG);
    app.get(MockGatewayRegistry).configure('gracegw', { latencyMinMs: 300, latencyMaxMs: 300 });
  });

  it('closing the application waits for the active job instead of dropping it', async () => {
    const id = `pay_grace_${randomUUID()}`;
    await request(app.getHttpServer()).post('/payments').send(payload(id, 'gracegw')).expect(201);
    // 120ms in: the charge (~300ms) must be in flight.
    await new Promise((r) => setTimeout(r, 120));

    const closeStarted = Date.now();
    await app.close();
    expect(Date.now() - closeStarted).toBeGreaterThanOrEqual(100);

    // The worker had to finish the in-flight payment before shutting down.
    const redis = new Redis(config.redis.url, { maxRetriesPerRequest: null });
    try {
      const doc = await redis.hget(`payment:${id}`, 'doc');
      const record = JSON.parse(doc!) as { status: string; completedAt?: string };
      expect(record.status).toBe('completed');
      expect(record.completedAt).toBeDefined();
    } finally {
      redis.disconnect();
    }
  }, 20_000);
});

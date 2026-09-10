import { INestApplication } from '@nestjs/common';
import request from 'supertest';
import { randomUUID } from 'node:crypto';
import { IdempotencyService } from '../../src/payments/idempotency.service.js';
import { PaymentStore } from '../../src/payments/payment-store.service.js';
import { QueueManager } from '../../src/queue/queue-manager.service.js';
import { MockGatewayRegistry } from '../../src/gateway/mock-gateway.service.js';
import { createTestApp, waitFor } from '../helpers/test-app.js';

/**
 * Ticket 03 — two-phase Redis idempotency (ADR 0002). All internal services
 * run as real code; only gateway behavior is scripted. Worker pool starts at 0
 * so tests can control exactly when processing begins.
 */
describe('Ticket 03 — two-phase Redis idempotency', () => {
  let app: INestApplication;
  let http: ReturnType<INestApplication['getHttpServer']>;
  let idempotency: IdempotencyService;
  let store: PaymentStore;
  let manager: QueueManager;
  let registry: MockGatewayRegistry;

  beforeAll(async () => {
    app = await createTestApp((cfg) => {
      cfg.queue.workerPoolSize = 0;
      cfg.idempotency.leaseTtlSeconds = 1;
      return cfg;
    });
    http = app.getHttpServer();
    idempotency = app.get(IdempotencyService);
    store = app.get(PaymentStore);
    manager = app.get(QueueManager);
    registry = app.get(MockGatewayRegistry);
  });

  afterAll(async () => {
    await app.close();
  });

  function payload(id: string, gatewayId: string) {
    return { id, amount: 250, currency: 'EUR', customerId: 'cust_i', gatewayId };
  }

  function submit(id: string, gatewayId: string) {
    return request(http).post('/payments').send(payload(id, gatewayId));
  }

  it('exactly one concurrent claim wins the atomic SET NX EX lease', async () => {
    const id = `idem_race_${randomUUID()}`;
    const [a, b] = await Promise.all([idempotency.claim(id), idempotency.claim(id)]);
    const winners = [a, b].filter((r) => r.status === 'new').length;
    expect(winners).toBe(1);
    expect([a, b].filter((r) => r.status === 'conflict').length).toBe(1);
    await idempotency.release(id);
  });

  it('conflicts while in flight; replays the completed record without a second charge', async () => {
    const id = `idem_replay_${randomUUID()}`;
    const gatewayId = 'idemgw1';
    registry.configure(gatewayId, { latencyMinMs: 10, latencyMaxMs: 10 });

    await submit(id, gatewayId).expect(201);
    // No workers yet: the PROCESSING lease guarantees a conflict, not a replay.
    await submit(id, gatewayId).expect(409);

    await manager.setWorkerPoolSize(gatewayId, 1);
    await waitFor(async () => {
      const rec = await store.get(id);
      return rec?.status === 'completed';
    }, { label: 'payment completes' });

    // Re-submission after completion: HTTP 200 with the original record.
    const replay = await submit(id, gatewayId).expect(200);
    expect(replay.body.replayed).toBe(true);
    expect(replay.body.status).toBe('completed');
    expect(replay.body.payment.id).toBe(id);

    // The charge executed exactly once across claim, conflict and replay.
    expect(registry.get(gatewayId).stats.charges).toBe(1);

    // Final idempotency record carries COMPLETED + transaction metadata + 24h TTL.
    const record = await idempotency.get(id);
    expect(record?.state).toBe('COMPLETED');
    expect(record?.transactionId).toMatch(new RegExp(`^${gatewayId}_txn_`));
    expect(record?.paymentStatus).toBe('completed');

    const ttlMs = await idempotency.getTtlMs(id);
    expect(ttlMs).toBeGreaterThan(0);
    expect(ttlMs).toBeLessThanOrEqual(86_400_000);
  });

  it('an expired lease (crashed worker) allows a fresh claim, and the queued job still completes exactly once', async () => {
    const id = `idem_crash_${randomUUID()}`;
    const gatewayId = 'idemgw2';
    registry.configure(gatewayId, { latencyMinMs: 10, latencyMaxMs: 10 });

    await submit(id, gatewayId).expect(201);
    expect((await idempotency.get(id))?.state).toBe('PROCESSING');

    // Simulate a worker crash: lease disappears without a final state.
    await idempotency.release(id);
    expect(await idempotency.get(id)).toBeNull();

    // A client resubmits after the lease vanished -> fresh claim, 201 again.
    // (No second job is created because BullMQ dedupes on jobId == paymentId.)
    const resubmit = await submit(id, gatewayId).expect(201);
    expect(resubmit.body.status).toBe('queued');

    // The original queued job is processed and completes.
    await manager.setWorkerPoolSize(gatewayId, 1);
    await waitFor(async () => {
      const rec = await store.get(id);
      return rec?.status === 'completed';
    }, { label: 'payment completes after lease expiry' });

    expect(registry.get(gatewayId).stats.charges).toBe(1);
    expect((await idempotency.get(id))?.state).toBe('COMPLETED');
  }, 20_000);

  it('the processing lease expires on its own if nothing finalizes it', async () => {
    const id = `idem_ttl_${randomUUID()}`;
    const first = await idempotency.claim(id);
    expect(first.status).toBe('new');
    // Still in-flight shortly after claiming.
    expect((await idempotency.claim(id)).status).toBe('conflict');

    await new Promise((r) => setTimeout(r, 1300)); // lease TTL = 1s
    const after = await idempotency.claim(id);
    expect(after.status).toBe('new');
    await idempotency.release(id);
  }, 10_000);
});

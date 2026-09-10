import { INestApplication } from '@nestjs/common';
import request from 'supertest';
import { randomUUID } from 'node:crypto';
import { AuditLogService } from '../../src/payments/audit-log.service.js';
import { PaymentStore } from '../../src/payments/payment-store.service.js';
import { SettlementLedger } from '../../src/payments/settlement-ledger.service.js';
import { MockGatewayRegistry } from '../../src/gateway/mock-gateway.service.js';
import { EventBus } from '../../src/common/event-bus.js';
import { createTestApp, waitFor } from '../helpers/test-app.js';

/**
 * Ticket 06 — the 3-step Payment Saga (Reserve -> Charge -> Settle) with
 * automated compensation:
 *  - charge fails  -> release the fund reservation
 *  - settle fails  -> refund/void the gateway charge + release the reservation
 * Every transition is persisted on the payment record and appended to the
 * immutable audit log.
 */
describe('Ticket 06 — saga happy path persists Reserve -> Charge -> Settle + audit trail', () => {
  let app: INestApplication;
  let http: ReturnType<INestApplication['getHttpServer']>;
  let store: PaymentStore;
  let audit: AuditLogService;

  beforeAll(async () => {
    app = await createTestApp();
    http = app.getHttpServer();
    store = app.get(PaymentStore);
    audit = app.get(AuditLogService);
  });

  afterAll(async () => {
    await app.close();
  });

  it('executes the three saga phases in order and persists each transition', async () => {
    const id = `pay_saga_${randomUUID()}`;
    await request(http)
      .post('/payments')
      .send({ id, amount: 4200, currency: 'USD', customerId: 'cust_saga', gatewayId: 'stripe' })
      .expect(201);

    await waitFor(async () => (await store.get(id))?.status === 'completed', {
      label: 'saga payment completes',
    });

    const record = await store.get(id);
    expect(record?.sagaState).toBe('settled');
    expect(record?.reservationId).toMatch(/^res_/);
    expect(record?.transactionId).toMatch(/^stripe_txn_/);

    // The final three history entries are the discrete saga steps in order.
    const tail = record!.history.slice(-3);
    expect(tail.map((h) => h.phase)).toEqual(['reserve', 'charge', 'settle']);
    expect(tail.every((h) => h.event === 'ok')).toBe(true);

    // Audit log records the saga transitions; entries are append-only.
    const entries = await audit.list(id);
    const types = entries.map((e) => e.type);
    expect(types[0]).toBe('payment.submitted');
    expect(types[types.length - 1]).toBe('payment.completed');
    expect(types).toEqual(
      expect.arrayContaining(['saga.reserve', 'saga.charge', 'saga.settle']),
    );
  });
});

describe('Ticket 06 — compensation: permanent charge failure releases the reservation', () => {
  let app: INestApplication;
  let http: ReturnType<INestApplication['getHttpServer']>;
  let store: PaymentStore;
  let audit: AuditLogService;

  beforeAll(async () => {
    app = await createTestApp();
    http = app.getHttpServer();
    store = app.get(PaymentStore);
    audit = app.get(AuditLogService);
    app.get(MockGatewayRegistry).configure('sagaperm', {
      latencyMinMs: 2,
      latencyMaxMs: 2,
      steps: [],
      after: { kind: 'fail', httpStatus: 400, retryable: false },
    });
  });

  afterAll(async () => {
    await app.close();
  });

  it('releases the reservation after a rejected charge and records the compensation', async () => {
    const id = `pay_sagaperm_${randomUUID()}`;
    await request(http)
      .post('/payments')
      .send({ id, amount: 100, currency: 'EUR', customerId: 'cust_x', gatewayId: 'sagaperm' })
      .expect(201);

    await waitFor(async () => (await store.get(id))?.status === 'dead_letter', {
      label: 'permanent charge failure dead-letters',
    });

    const record = (await store.get(id))!;
    expect(record.reservationId).toBeUndefined(); // never reached the charge step's success path
    const history = record.history;
    expect(history.some((h) => h.phase === 'charge' && h.event === 'failure')).toBe(true);
    expect(history.some((h) => h.phase === 'compensation' && h.event === 'release_funds')).toBe(true);

    const types = (await audit.list(id)).map((e) => e.type);
    expect(types).toEqual(expect.arrayContaining(['saga.reserve', 'saga.compensate.release_funds', 'payment.dead_lettered']));
  });
});

describe('Ticket 06 — compensation: a settle failure refunds the charge and releases the reservation', () => {
  let app: INestApplication;
  let http: ReturnType<INestApplication['getHttpServer']>;
  let store: PaymentStore;
  let audit: AuditLogService;
  let gateways: MockGatewayRegistry;

  beforeAll(async () => {
    // Controlled seam: settle (ledger write) fails downstream of the gateway.
    app = await createTestApp(undefined, [
      {
        provide: SettlementLedger,
        useValue: {
          settle: async () => {
            throw new Error('settlement ledger unavailable');
          },
          list: async () => [],
        },
      },
    ]);
    http = app.getHttpServer();
    store = app.get(PaymentStore);
    audit = app.get(AuditLogService);
    gateways = app.get(MockGatewayRegistry);
    gateways.configure('sagaset', { latencyMinMs: 2, latencyMaxMs: 2 });
  });

  afterAll(async () => {
    await app.close();
  });

  it('charges once, refunds the charge, releases the reservation and dead-letters with a clear reason', async () => {
    const id = `pay_sagaset_${randomUUID()}`;
    await request(http)
      .post('/payments')
      .send({ id, amount: 700, currency: 'USD', customerId: 'cust_y', gatewayId: 'sagaset' })
      .expect(201);

    await waitFor(async () => (await store.get(id))?.status === 'dead_letter', {
      label: 'settle failure dead-letters',
    });

    const record = (await store.get(id))!;
    expect(record.sagaState).toBe('compensated');
    expect(record.failureReason).toContain('refunded');
    expect(record.refundId).toMatch(/^refund_/);
    expect(record.transactionId).toMatch(/^sagaset_txn_/);

    const history = record.history;
    const phases = history.map((h) => `${h.phase}:${h.event}`);
    expect(phases).toEqual(
      expect.arrayContaining([
        'reserve:ok',
        'charge:ok',
        'settle:failure',
        'compensation:refund_charge',
        'compensation:release_funds',
      ]),
    );
    // Exactly one charge + one refund hit the gateway.
    expect(gateways.get('sagaset').stats.charges).toBe(1);
    expect(gateways.get('sagaset').stats.refunds).toBe(1);

    // The audit entry is appended after the record's terminal state is saved.
    await waitFor(async () => {
      const current = await audit.list(id);
      return current.some((e) => e.type === 'payment.dead_lettered');
    }, { label: 'dead-letter audit entry written' });

    const entries = await audit.list(id);
    const types = entries.map((e) => e.type);
    expect(types).toEqual(
      expect.arrayContaining(['saga.charge', 'saga.compensate.refund_charge', 'saga.compensate.release_funds', 'payment.dead_lettered']),
    );
    const refundAudit = entries.find((e) => e.type === 'saga.compensate.refund_charge');
    expect(refundAudit?.detail).toMatchObject({ ok: true });
  });

  it('emits a critical alert when refund compensation fails', async () => {
    gateways.configure('sagaset_fail', {
      latencyMinMs: 2,
      latencyMaxMs: 2,
      refundSteps: [{ kind: 'fail', httpStatus: 500 }],
    });
    const events = app.get(EventBus);
    const alerts: unknown[] = [];
    events.on('metrics.alert', (e) => {
      if (e.type === 'metrics.alert') alerts.push(e.alert);
    });

    const id = `pay_sagafail_${randomUUID()}`;
    await request(http)
      .post('/payments')
      .send({ id, amount: 700, currency: 'USD', customerId: 'cust_fail', gatewayId: 'sagaset_fail' })
      .expect(201);

    await waitFor(async () => (await store.get(id))?.status === 'dead_letter', {
      label: 'settle failure with failed refund dead-letters',
    });

    expect(alerts.length).toBeGreaterThanOrEqual(1);
    expect((alerts[0] as { severity: string }).severity).toBe('critical');
  });
});

describe('Ticket 06 — compensation on transient charge failure, then full re-execution on retry', () => {
  let app: INestApplication;
  let http: ReturnType<INestApplication['getHttpServer']>;
  let store: PaymentStore;

  beforeAll(async () => {
    app = await createTestApp((cfg) => {
      cfg.queue.backoffBaseMs = 50;
      cfg.queue.backoffJitter = 0;
      return cfg;
    });
    http = app.getHttpServer();
    store = app.get(PaymentStore);
    app.get(MockGatewayRegistry).configure('sagatrans', {
      latencyMinMs: 2,
      latencyMaxMs: 2,
      steps: [{ kind: 'fail', httpStatus: 503 }],
      after: { kind: 'ok' },
    });
  });

  afterAll(async () => {
    await app.close();
  });

  it('releases the reservation on the failed attempt, then completes on the retry', async () => {
    const id = `pay_sagatrans_${randomUUID()}`;
    await request(http)
      .post('/payments')
      .send({ id, amount: 550, currency: 'GBP', customerId: 'cust_z', gatewayId: 'sagatrans' })
      .expect(201);

    await waitFor(async () => (await store.get(id))?.status === 'completed', {
      label: 'transient charge failure recovers',
      timeoutMs: 10_000,
    });

    const record = (await store.get(id))!;
    expect(record.retryCount).toBe(1);
    expect(record.sagaState).toBe('settled');
    const events = record.history.map((h) => `${h.phase}:${h.event}`);
    // Attempt 1 released the reservation; attempt 2 re-ran the saga to completion.
    expect(events).toEqual(
      expect.arrayContaining(['charge:failure', 'compensation:release_funds', 'charge:ok', 'settle:ok']),
    );
  });
});

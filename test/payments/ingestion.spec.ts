import { INestApplication } from '@nestjs/common';
import request from 'supertest';
import { randomUUID } from 'node:crypto';
import { createTestApp, waitFor } from '../helpers/test-app.js';

describe('Ticket 01 — ingestion pipeline (POST /payments -> worker -> completed)', () => {
  let app: INestApplication;
  let http: ReturnType<INestApplication['getHttpServer']>;

  beforeAll(async () => {
    app = await createTestApp();
    http = app.getHttpServer();
  });

  afterAll(async () => {
    await app.close();
  });

  it('submits a payment, the worker consumes it and the status becomes completed', async () => {
    const id = `pay_${randomUUID()}`;
    const response = await request(http)
      .post('/payments')
      .send({
        id,
        amount: 1299,
        currency: 'USD',
        customerId: 'cust_1',
        gatewayId: 'stripe',
        priority: 'high',
      })
      .expect(201);

    expect(response.body).toEqual({ id, status: 'queued' });

    await waitFor(async () => {
      const res = await request(http).get(`/payments/${id}`).expect(200);
      return res.body.status === 'completed';
    }, { label: 'payment to complete' });

    const done = await request(http).get(`/payments/${id}`).expect(200);
    const payment = done.body;
    expect(payment.status).toBe('completed');
    expect(payment.sagaState).toBe('settled');
    expect(payment.transactionId).toMatch(/^stripe_txn_/);
    expect(payment.history.length).toBeGreaterThanOrEqual(3);
    expect(payment.history.map((h: { event: string }) => h.event)).toEqual(
      expect.arrayContaining(['ok', 'ok', 'ok']),
    );
    expect(new Date(payment.completedAt).getTime()).toBeGreaterThanOrEqual(new Date(payment.createdAt).getTime());
  });

  it('a second gateway processes concurrently with the first', async () => {
    const [a, b] = [randomUUID(), randomUUID()];
    await request(http).post('/payments').send(payload(a, 'stripe')).expect(201);
    await request(http).post('/payments').send(payload(b, 'paypal')).expect(201);
    await waitFor(async () => {
      const [ra, rb] = await Promise.all([
        request(http).get(`/payments/${a}`),
        request(http).get(`/payments/${b}`),
      ]);
      return ra.body.status === 'completed' && rb.body.status === 'completed';
    }, { label: 'both gateways complete' });
  });

  it('rejects invalid payloads with 400', async () => {
    await request(http)
      .post('/payments')
      .send({ id: 'x', amount: -5, currency: 'USD', customerId: 'c', gatewayId: 'stripe' })
      .expect(400);
    await request(http)
      .post('/payments')
      .send({ id: 'x', amount: 100, currency: 'US', customerId: 'c', gatewayId: 'stripe' })
      .expect(400);
    await request(http)
      .post('/payments')
      .send({ id: 'x', amount: 100, currency: 'USD', customerId: 'c', gatewayId: 'stripe', priority: 'urgent' })
      .expect(400);
    await request(http).post('/payments').send({ amount: 100, currency: 'USD', customerId: 'c', gatewayId: 'stripe' }).expect(400);
  });

  it('returns 404 for an unknown payment', async () => {
    await request(http).get('/payments/does-not-exist').expect(404);
  });

  it('rejects a duplicate submission while the first payment is in flight', async () => {
    const id = `pay_dup_${randomUUID()}`;
    await request(http).post('/payments').send(payload(id, 'adyen')).expect(201);
    // Immediate resubmission while the lease is PROCESSING must conflict.
    await request(http).post('/payments').send(payload(id, 'adyen')).expect(409);
    await waitFor(async () => {
      const res = await request(http).get(`/payments/${id}`);
      return res.body.status === 'completed';
    }, { label: 'first payment completes' });
    // After completion the resubmission replays the stored record (200) — ticket 03.
    const replay = await request(http).post('/payments').send(payload(id, 'adyen')).expect(200);
    expect(replay.body.replayed).toBe(true);
    expect(replay.body.status).toBe('completed');
  });
});

function payload(id: string, gatewayId: string) {
  return { id, amount: 500, currency: 'EUR', customerId: 'cust_x', gatewayId };
}

import { INestApplication } from '@nestjs/common';
import request from 'supertest';
import { randomUUID } from 'node:crypto';
import { AddressInfo } from 'node:net';
import { io, Socket } from 'socket.io-client';
import { MockGatewayRegistry } from '../../src/gateway/mock-gateway.service.js';
import { QueueManager } from '../../src/queue/queue-manager.service.js';
import { createTestApp, createTestAppWithWebSocket, waitFor } from '../helpers/test-app.js';

/**
 * Ticket 07 — rolling-window metrics + real-time WebSocket streaming:
 * live TPS/error rate/P95/P99 in a 60s window, queue depths, threshold alerts
 * and `GET /queues/metrics`.
 */
describe('Ticket 07 — MetricsCollector: accuracy of TPS / error rate / percentiles via HTTP', () => {
  let app: INestApplication;
  let http: ReturnType<INestApplication['getHttpServer']>;

  beforeAll(async () => {
    app = await createTestApp();
    http = app.getHttpServer();
    app.get(MockGatewayRegistry).configure('metgw', { latencyMinMs: 40, latencyMaxMs: 40 });
  });

  afterAll(async () => {
    await app.close();
  });

  function body(id: string, gatewayId: string) {
    return { id, amount: 111, currency: 'USD', customerId: 'cust_m', gatewayId };
  }

  it('computes live TPS, error rate and P95/P99 from completed attempts', async () => {
    const ids = Array.from({ length: 5 }, () => `pay_met_${randomUUID()}`);
    for (const id of ids) {
      await request(http).post('/payments').send(body(id, 'metgw')).expect(201);
    }
    for (const id of ids) {
      await waitFor(async () => (await request(http).get(`/payments/${id}`)).body.status === 'completed', {
        label: 'payment completes',
      });
    }

    // The payment record is saved before the job.completed event reaches the
    // metrics collector, so wait for the attempt to be counted.
    await waitFor(async () => {
      const snap = (await request(http).get('/queues/metrics').expect(200)).body;
      return (snap.attempts?.ok ?? 0) >= 5;
    }, { label: 'metrics recorded all attempts' });

    const metrics = (await request(http).get('/queues/metrics').expect(200)).body;
    expect(metrics.timestamp).toBeDefined();
    expect(metrics.windowSeconds).toBe(60);
    expect(metrics.attempts.ok).toBeGreaterThanOrEqual(5);
    expect(metrics.errorRate).toBe(0);
    expect(metrics.tps).toBeGreaterThanOrEqual(1);
    expect(metrics.p95Ms).toBeGreaterThan(0);
    expect(metrics.p95Ms).toBeLessThan(500); // 40ms gateway latency + overhead
    expect(metrics.p99Ms).toBeGreaterThanOrEqual(metrics.p95Ms!);
    const metgw = metrics.queueDepths.gateways.find((g: { gatewayId: string }) => g.gatewayId === 'metgw');
    expect(metgw).toBeDefined();
    expect(metrics.queueDepths.dlq.gatewayId).toBe('dlq');
  });

  it('records failed attempts and raises an error-rate alert', async () => {
    app.get(MockGatewayRegistry).configure('errgw', {
      latencyMinMs: 2,
      latencyMaxMs: 2,
      steps: [],
      after: { kind: 'fail', httpStatus: 400 },
    });
    const ids = Array.from({ length: 2 }, () => `pay_err_${randomUUID()}`);
    for (const id of ids) {
      await request(http).post('/payments').send(body(id, 'errgw')).expect(201);
    }
    await waitFor(
      async () => {
        const done = await Promise.all(ids.map((id) => request(http).get(`/payments/${id}`)));
        return done.every((r) => r.body.status === 'dead_letter');
      },
      { label: 'payments dead-letter' },
    );

    await waitFor(async () => {
      const metrics = (await request(http).get('/queues/metrics').expect(200)).body;
      return metrics.alerts?.some((a: { id: string }) => a.id === 'error_rate_high');
    }, { label: 'error-rate alert raised' });

    const metrics = (await request(http).get('/queues/metrics').expect(200)).body;
    expect(metrics.attempts.failed).toBeGreaterThanOrEqual(2);
    expect(metrics.errorRate).toBeGreaterThan(0);
    const alert = metrics.alerts.find((a: { id: string }) => a.id === 'error_rate_high');
    expect(alert.severity).toBe('critical');
    expect(alert.message).toContain('%');
  });
});

describe('Ticket 07 — Socket.IO gateway streams metrics:snapshot, payment:event and alert:raised', () => {
  let app: INestApplication;
  let manager: QueueManager;
  let http: ReturnType<INestApplication['getHttpServer']>;
  let client: Socket;
  const snapshots: any[] = [];
  const paymentEvents: any[] = [];
  const alerts: any[] = [];

  beforeAll(async () => {
    app = await createTestAppWithWebSocket((cfg) => {
      cfg.queue.workerPoolSize = 0; // control processing manually
      cfg.metrics.broadcastIntervalMs = 300;
      cfg.metrics.queueDepthAlert = 0; // any waiting job raises the alert
      return cfg;
    });
    manager = app.get(QueueManager);
    http = app.getHttpServer();
    app.get(MockGatewayRegistry).configure('wsgw', { latencyMinMs: 5, latencyMaxMs: 5 });
    await app.listen(0);
    const port = (app.getHttpServer().address() as AddressInfo).port;

    client = io(`http://127.0.0.1:${port}`, { transports: ['websocket'] });
    client.on('connect', () => {});
    client.on('metrics:snapshot', (payload) => snapshots.push(payload));
    client.on('payment:event', (payload) => paymentEvents.push(payload));
    client.on('alert:raised', (payload) => alerts.push(payload));
    await new Promise<void>((resolve) => {
      if (client.connected) resolve();
      else client.once('connect', () => resolve());
    });
  });

  afterAll(async () => {
    client.disconnect();
    await app.close();
  });

  it('broadcasts periodic metrics snapshots with queue depths', async () => {
    await waitFor(() => snapshots.length > 0, { label: 'first metrics:snapshot', timeoutMs: 5000 });
    const snap = snapshots[snapshots.length - 1];
    expect(snap.timestamp).toBeDefined();
    expect(snap.queueDepths.gateways).toBeDefined();
    expect(snap.attempts).toBeDefined();
  });

  it('streams payment lifecycle events and raises a queue-depth alert for a backlog', async () => {
    const ids = Array.from({ length: 3 }, () => `pay_ws_${randomUUID()}`);
    for (const id of ids) {
      await request(http).post('/payments').send({ id, amount: 222, currency: 'EUR', customerId: 'cust_ws', gatewayId: 'wsgw' }).expect(201);
    }

    await waitFor(() => paymentEvents.filter((e) => e.type === 'payment.queued').length >= 3, {
      label: 'queued payment events streamed',
      timeoutMs: 5000,
    });
    await waitFor(() => alerts.some((a) => a.id.startsWith('queue_depth_high:wsgw')), {
      label: 'queue depth alert raised over websocket',
      timeoutMs: 5000,
    });

    // Scale workers up: the backlog drains and completion events stream in.
    await manager.setWorkerPoolSize('wsgw', 2);
    await waitFor(
      () => paymentEvents.filter((e) => e.type === 'payment.completed' && ids.includes(e.paymentId)).length >= 3,
      { label: 'completed payment events streamed', timeoutMs: 10_000 },
    );
  });
});

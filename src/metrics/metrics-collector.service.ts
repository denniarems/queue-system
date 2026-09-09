import { Inject, Injectable, Logger, OnModuleInit } from '@nestjs/common';
import { APP_CONFIG } from '../config/app-config.js';
import type { AppConfig } from '../config/app-config.js';
import { EventBus } from '../common/event-bus.js';
import type { MetricsAlertPayload } from '../common/event-bus.js';
import { QueueManager } from '../queue/queue-manager.service.js';

/** Alerts share one canonical shape with the websocket payload. */
export type ActiveAlert = MetricsAlertPayload;

export interface MetricsSnapshot {
  timestamp: string;
  windowSeconds: number;
  /** Attempt completions in the last second (live throughput). */
  tps: number;
  /** Failed attempts / total attempts inside the window. */
  errorRate: number;
  attempts: { ok: number; failed: number; total: number };
  /** Percentiles over in-window attempt durations (ms). */
  p95Ms: number | null;
  p99Ms: number | null;
  queueDepths: Awaited<ReturnType<QueueManager['collectQueueDepths']>>;
  alerts: ActiveAlert[];
}

interface AttemptSample {
  t: number;
  gatewayId: string;
  ok: boolean;
  durationMs: number;
}

const percentile = (sorted: number[], p: number): number | null => {
  if (sorted.length === 0) return null;
  const index = Math.min(sorted.length - 1, Math.max(0, Math.ceil(p * sorted.length) - 1));
  return sorted[index];
};

/**
 * In-memory rolling window (ADR 0004): live TPS, error rates and P95/P99
 * latencies derived from a 60s buffer of attempt completions, without
 * expensive Redis history queries. Queue depths are sampled live from BullMQ
 * on each snapshot. Threshold breaches raise alerts (emitted on the event bus
 * and broadcast by the Socket.IO gateway as `alert:raised`).
 */
@Injectable()
export class MetricsCollector implements OnModuleInit {
  private readonly logger = new Logger(MetricsCollector.name);
  private readonly samples: AttemptSample[] = [];
  private readonly alerts = new Map<string, ActiveAlert>();
  private readonly lastRaisedAt = new Map<string, number>();

  constructor(
    @Inject(APP_CONFIG) private readonly config: AppConfig,
    private readonly events: EventBus,
    private readonly queueManager: QueueManager,
  ) {}

  onModuleInit(): void {
    const safeRefresh = () => {
      void this.refresh().catch(() => undefined);
    };
    this.events.on('job.completed', (e) => {
      if (e.type === 'job.completed') {
        this.recordAttempt(e.gatewayId, e.ok, e.durationMs);
        safeRefresh();
      }
    });
    this.events.on('payment.queued', () => safeRefresh());
    this.events.on('circuit.opened', (e) => {
      if (e.type === 'circuit.opened') this.setCircuitAlert(e.gatewayId, true);
    });
    this.events.on('circuit.closed', (e) => {
      if (e.type === 'circuit.closed') this.setCircuitAlert(e.gatewayId, false);
    });
  }

  private recordAttempt(gatewayId: string, ok: boolean, durationMs: number): void {
    const sample: AttemptSample = { t: Date.now(), gatewayId, ok, durationMs };
    this.samples.push(sample);
    const cutoff = Date.now() - this.config.metrics.windowSeconds * 1000;
    while (this.samples.length > 0 && this.samples[0].t < cutoff) this.samples.shift();
    if (this.samples.length > 10_000) this.samples.splice(0, this.samples.length - 10_000);
  }

  async snapshot(): Promise<MetricsSnapshot> {
    const now = Date.now();
    const windowMs = this.config.metrics.windowSeconds * 1000;
    const cutoff = now - windowMs;
    const inWindow = this.samples.filter((s) => s.t >= cutoff);
    const ok = inWindow.filter((s) => s.ok).length;
    const total = inWindow.length;
    const failed = total - ok;
    const durations = inWindow.map((s) => s.durationMs).sort((a, b) => a - b);
    const recentSecond = inWindow.filter((s) => s.t >= now - 1000).length;
    const queueDepths = await this.queueManager.collectQueueDepths();

    return {
      timestamp: new Date(now).toISOString(),
      windowSeconds: this.config.metrics.windowSeconds,
      tps: recentSecond,
      errorRate: total === 0 ? 0 : failed / total,
      attempts: { ok, failed, total },
      p95Ms: percentile(durations, 0.95),
      p99Ms: percentile(durations, 0.99),
      queueDepths,
      alerts: [...this.alerts.values()],
    };
  }

  /**
   * Refresh thresholds against a fresh snapshot and return it. Called on every
   * ingested event and on the websocket broadcast tick, so backlog conditions
   * (e.g. queue depth while workers are scaled down) are still detected.
   */
  async refresh(): Promise<MetricsSnapshot> {
    try {
      const snapshot = await this.snapshot();
      const m = this.config.metrics;

      this.evaluate('error_rate_high', snapshot.errorRate > m.errorRateAlert, {
        severity: 'critical',
        message: `error rate ${(snapshot.errorRate * 100).toFixed(1)}% exceeds threshold ${(m.errorRateAlert * 100).toFixed(1)}%`,
        value: snapshot.errorRate,
        threshold: m.errorRateAlert,
      });
      this.evaluate('p95_latency_high', snapshot.p95Ms !== null && snapshot.p95Ms > m.p95LatencyAlertMs, {
        severity: 'warning',
        message: `P95 latency ${snapshot.p95Ms?.toFixed(0)}ms exceeds threshold ${m.p95LatencyAlertMs}ms`,
        value: snapshot.p95Ms ?? undefined,
        threshold: m.p95LatencyAlertMs,
      });
      for (const gateway of snapshot.queueDepths.gateways) {
        const id = `queue_depth_high:${gateway.gatewayId}`;
        this.evaluate(id, gateway.waiting > m.queueDepthAlert, {
          severity: 'warning',
          message: `gateway ${gateway.gatewayId} waiting depth ${gateway.waiting} exceeds threshold ${m.queueDepthAlert}`,
          value: gateway.waiting,
          threshold: m.queueDepthAlert,
        });
      }
      // Clear alerts whose condition vanished (e.g. depth drained). Deleting
      // entries while iterating a Map is safe.
      for (const id of this.alerts.keys()) {
        let stillActive = true;
        if (id === 'error_rate_high') {
          stillActive = snapshot.errorRate > m.errorRateAlert;
        } else if (id === 'p95_latency_high') {
          stillActive = snapshot.p95Ms !== null && snapshot.p95Ms > m.p95LatencyAlertMs;
        } else if (id.startsWith('queue_depth_high:')) {
          const gatewayId = id.slice('queue_depth_high:'.length);
          const depths = snapshot.queueDepths.gateways.find((g) => g.gatewayId === gatewayId);
          stillActive = (depths?.waiting ?? 0) > m.queueDepthAlert;
        }
        // circuit_open:* alerts stay active until a circuit.closed event.
        if (!stillActive) this.alerts.delete(id);
      }
      return snapshot;
    } catch (err) {
      this.logger.error(`metrics refresh failed`, err instanceof Error ? err.message : String(err));
      throw err;
    }
  }

  private evaluate(
    id: string,
    condition: boolean,
    spec: { severity: 'critical' | 'warning'; message: string; value?: number; threshold?: number },
  ): void {
    const now = Date.now();
    if (condition) {
      const cooldownMs = this.config.metrics.alertCooldownMs;
      const lastRaised = this.lastRaisedAt.get(id) ?? 0;
      const alreadyActive = this.alerts.has(id);
      if (!alreadyActive) {
        const alert: ActiveAlert = { id, ...spec, raisedAt: new Date(now).toISOString() };
        this.alerts.set(id, alert);
        this.lastRaisedAt.set(id, now);
        void this.events.emit({
          type: 'metrics.alert',
          alert,
          at: alert.raisedAt,
        });
      } else if (now - lastRaised >= cooldownMs) {
        this.lastRaisedAt.set(id, now);
        // re-notify so operators keep seeing the condition
        const alert = this.alerts.get(id)!;
        void this.events.emit({ type: 'metrics.alert', alert: { ...alert, raisedAt: alert.raisedAt }, at: new Date(now).toISOString() });
      }
    }
  }

  private setCircuitAlert(gatewayId: string, open: boolean): void {
    const id = `circuit_open:${gatewayId}`;
    if (open) {
      this.evaluate(id, true, {
        severity: 'critical',
        message: `circuit breaker OPEN for gateway ${gatewayId}`,
      });
    } else {
      this.alerts.delete(id);
    }
  }
}

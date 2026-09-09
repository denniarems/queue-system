import { Global, Injectable, Module } from '@nestjs/common';
import { AsyncLocalStorage } from 'node:async_hooks';
import type { PaymentStatus, SagaPhase } from '../domain/payment.js';
import { TraceLogger } from './trace-logger.js';

/**
 * In-process typed event bus. Feature modules are wired together through
 * domain events instead of cyclic module imports: the queue layer and the
 * payment processor emit; metrics/websocket modules subscribe.
 */
export type AppEvent =
  | {
      type: 'payment.queued' | 'payment.processing' | 'payment.completed' | 'payment.failed' | 'payment.dead_lettered';
      paymentId: string;
      gatewayId?: string;
      correlationId?: string;
      status?: PaymentStatus;
      at: string;
      detail?: Record<string, unknown>;
    }
  | { type: 'job.completed'; paymentId: string; gatewayId: string; ok: boolean; durationMs: number; at: string }
  | { type: 'job.failed'; paymentId: string; gatewayId: string; reason: string; at: string }
  | { type: 'circuit.opened' | 'circuit.closed'; gatewayId: string; at: string }
  | { type: 'rate_limit.throttled'; gatewayId: string; at: string }
  | { type: 'saga.phase'; paymentId: string; gatewayId: string; phase: SagaPhase; outcome: 'ok' | 'failed'; at: string }
  | { type: 'metrics.alert'; alert: MetricsAlertPayload; at: string };

export interface MetricsAlertPayload {
  id: string;
  severity: 'critical' | 'warning';
  message: string;
  value?: number;
  threshold?: number;
  raisedAt: string;
}

export type AppEventListener = (event: AppEvent) => void | Promise<void>;

@Injectable()
export class EventBus {
  private readonly listeners = new Map<AppEvent['type'], Set<AppEventListener>>();

  on(type: AppEvent['type'], listener: AppEventListener): () => void {
    let set = this.listeners.get(type);
    if (!set) {
      set = new Set();
      this.listeners.set(type, set);
    }
    set.add(listener);
    return () => set.delete(listener);
  }

  /** Deliver the event to all listeners, preserving order and swallowing errors. */
  async emit(event: AppEvent): Promise<void> {
    const set = this.listeners.get(event.type);
    if (!set || set.size === 0) return;
    // Snapshot so a listener unsubscribing mid-dispatch does not skip peers.
    for (const listener of Array.from(set)) {
      try {
        await listener(event);
      } catch (err) {
        // A failing subscriber (e.g. socket broadcast) must not break processing.
        console.error(`[event-bus] listener for ${event.type} failed`, err);
      }
    }
  }
}

@Injectable()
export class CorrelationService {
  private readonly storage = new AsyncLocalStorage<{ correlationId: string }>();

  /** Run `fn` with the correlation id bound to the async context. */
  enter<T>(correlationId: string | undefined, fn: () => T): T {
    const store = { correlationId: correlationId ?? newCorrelationId() };
    return this.storage.run(store, fn);
  }

  current(): string | undefined {
    return this.storage.getStore()?.correlationId;
  }
}

export function newCorrelationId(): string {
  return `corr_${Date.now().toString(36)}_${Math.random().toString(36).slice(2, 10)}`;
}

@Global()
@Module({
  providers: [
    EventBus,
    CorrelationService,
    { provide: TraceLogger, useFactory: (correlation: CorrelationService) => new TraceLogger(correlation), inject: [CorrelationService] },
  ],
  exports: [EventBus, CorrelationService, TraceLogger],
})
export class CommonModule {}

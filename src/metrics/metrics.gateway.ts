import { Inject, Injectable, Logger, OnModuleDestroy, OnModuleInit } from '@nestjs/common';
import { WebSocketGateway, WebSocketServer } from '@nestjs/websockets';
import { Server } from 'socket.io';
import { APP_CONFIG } from '../config/app-config.js';
import type { AppConfig } from '../config/app-config.js';
import { AppEvent, EventBus } from '../common/event-bus.js';
import { MetricsCollector } from './metrics-collector.service.js';

/**
 * Socket.IO gateway streaming live operational data (ADR 0004):
 *  - `metrics:snapshot` every broadcastIntervalMs,
 *  - `payment:event` on every payment lifecycle transition,
 *  - `alert:raised` the moment a threshold breach is detected.
 */
@WebSocketGateway({ cors: { origin: '*' } })
@Injectable()
export class MetricsGateway implements OnModuleInit, OnModuleDestroy {
  private readonly logger = new Logger(MetricsGateway.name);
  private readonly unsubscribe: Array<() => void> = [];
  private timer?: NodeJS.Timeout;

  @WebSocketServer()
  server?: Server;

  constructor(
    @Inject(APP_CONFIG) private readonly config: AppConfig,
    private readonly collector: MetricsCollector,
    private readonly events: EventBus,
  ) {}

  onModuleInit(): void {
    this.unsubscribe.push(
      this.events.on('payment.queued', (e) => this.broadcastPaymentEvent(e)),
      this.events.on('payment.processing', (e) => this.broadcastPaymentEvent(e)),
      this.events.on('payment.completed', (e) => this.broadcastPaymentEvent(e)),
      this.events.on('payment.failed', (e) => this.broadcastPaymentEvent(e)),
      this.events.on('payment.dead_lettered', (e) => this.broadcastPaymentEvent(e)),
      this.events.on('saga.phase', (e) => {
        if (e.type === 'saga.phase') {
          this.server?.emit('payment:flow', {
            paymentId: e.paymentId,
            gatewayId: e.gatewayId,
            phase: e.phase,
            outcome: e.outcome,
            at: e.at,
          });
        }
      }),
      this.events.on('metrics.alert', (e) => {
        if (e.type === 'metrics.alert') this.server?.emit('alert:raised', e.alert);
      }),
    );
    this.timer = setInterval(() => {
      void this.pushSnapshot();
    }, this.config.metrics.broadcastIntervalMs);
  }

  onModuleDestroy(): void {
    if (this.timer) clearInterval(this.timer);
    for (const unsubscribe of this.unsubscribe) unsubscribe();
  }

  private broadcastPaymentEvent(event: AppEvent): void {
    if (!this.server) return;
    switch (event.type) {
      case 'payment.queued':
      case 'payment.processing':
      case 'payment.completed':
      case 'payment.failed':
      case 'payment.dead_lettered':
        this.server.emit('payment:event', {
          type: event.type,
          paymentId: event.paymentId,
          gatewayId: event.gatewayId,
          status: event.status,
          at: event.at,
          detail: event.detail,
        });
        break;
      default:
        break;
    }
  }

  /** Periodic tick: refresh thresholds (may raise alerts) then broadcast. */
  private async pushSnapshot(): Promise<void> {
    if (!this.server) return;
    try {
      const snapshot = await this.collector.refresh();
      this.server.emit('metrics:snapshot', snapshot);
    } catch (err) {
      this.logger.error('failed to broadcast metrics snapshot', err instanceof Error ? err.message : String(err));
    }
  }
}

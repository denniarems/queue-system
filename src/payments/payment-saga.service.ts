import { Inject, Injectable } from '@nestjs/common';
import { APP_CONFIG } from '../config/app-config.js';
import type { AppConfig } from '../config/app-config.js';
import { EventBus } from '../common/event-bus.js';
import { TraceLogger } from '../common/trace-logger.js';
import { ERROR_CODES, PaymentProcessingError } from '../domain/errors.js';
import { PaymentRecord, SagaHistoryEntry } from '../domain/payment.js';
import { MockGatewayRegistry } from '../gateway/mock-gateway.service.js';
import { RateLimiterRegistry } from '../gateway/rate-limiter.registry.js';
import { CircuitBreakerRegistry } from '../gateway/circuit-breaker.registry.js';
import { SPAN_NAMES, TracingService } from '../tracing/tracing.service.js';
import { AuditLogService } from './audit-log.service.js';
import { PaymentStore } from './payment-store.service.js';
import { SettlementLedger } from './settlement-ledger.service.js';

export interface SagaOutcome {
  transactionId: string;
  reservationId: string;
}

/**
 * 3-step Payment Saga (ticket 06): Reserve -> Charge -> Settle, with
 * automated compensation on failure:
 *  - charge failure (transient or permanent)  -> release the reservation
 *  - settle failure after a successful charge -> refund the charge + release
 *
 * Every step transition and compensation is persisted in the payment record
 * and appended to the immutable audit log before the saga returns or throws.
 */
@Injectable()
export class PaymentSagaService {
  constructor(
    private readonly store: PaymentStore,
    private readonly audit: AuditLogService,
    private readonly gateways: MockGatewayRegistry,
    private readonly rateLimiters: RateLimiterRegistry,
    private readonly breakers: CircuitBreakerRegistry,
    private readonly ledger: SettlementLedger,
    private readonly events: EventBus,
    private readonly tracing: TracingService,
    private readonly traceLog: TraceLogger,
    @Inject(APP_CONFIG) private readonly config: AppConfig,
  ) {}

  /**
   * Execute the saga for one worker attempt. On success the record reflects
   * the settled state; on failure compensations have been applied and a
   * PaymentProcessingError is thrown for the caller to classify.
   */
  async execute(record: PaymentRecord): Promise<SagaOutcome> {
    const payment = record;

    // ---- Step 1: reserve funds (internal allocation / balance lock) ----
    const reservationId = await this.tracing.withSpan(
      SPAN_NAMES.RESERVE,
      { paymentId: payment.id, gatewayId: payment.gatewayId, correlationId: record.correlationId ?? '' },
      async () => {
        const reservationId = `res_${payment.id}_${Date.now().toString(36)}`;
        await this.history(record, 'reserve', 'ok', reservationId);
        return reservationId;
      },
    );
    record.sagaState = 'reserved';
    await this.store.save(record);
    await this.audit.record({
      paymentId: payment.id,
      type: 'saga.reserve',
      detail: { reservationId },
    });
    await this.events.emit({
      type: 'saga.phase',
      paymentId: payment.id,
      gatewayId: payment.gatewayId,
      phase: 'reserve',
      outcome: 'ok',
      at: new Date().toISOString(),
    });

    // ---- Step 2: charge through the payment gateway (rate limiter gated) ----
    const gateway = this.gateways.get(payment.gatewayId);
    const bucket = this.rateLimiters.get(payment.gatewayId);
    const hasToken = await bucket.waitForToken(this.config.rateLimiter.tokenWaitMs);
    if (!hasToken) {
      await this.history(record, 'charge', 'rate_limited', 'no token within tokenWaitMs');
      await this.releaseFunds(record, reservationId);
      await this.store.save(record);
      await this.events.emit({
        type: 'saga.phase',
        paymentId: payment.id,
        gatewayId: payment.gatewayId,
        phase: 'charge',
        outcome: 'failed',
        at: new Date().toISOString(),
      });
      throw new PaymentProcessingError(
        `gateway ${payment.gatewayId} rate limiter: no token within ${this.config.rateLimiter.tokenWaitMs}ms`,
        ERROR_CODES.RATE_LIMITED,
        true,
        429,
        payment.gatewayId,
      );
    }
    // Circuit breaker: fast-fail while OPEN without invoking the gateway.
    const breaker = this.breakers.get(payment.gatewayId);
    const gate = breaker.allowCall();
    if (!gate.allowed) {
      await this.history(record, 'charge', 'circuit_open', `breaker ${gate.state}; not calling gateway`);
      await this.withCompensation(record, 'release_funds', () => this.releaseFunds(record, reservationId));
      await this.store.save(record);
      await this.events.emit({
        type: 'saga.phase',
        paymentId: payment.id,
        gatewayId: payment.gatewayId,
        phase: 'charge',
        outcome: 'failed',
        at: new Date().toISOString(),
      });
      throw new PaymentProcessingError(
        `gateway ${payment.gatewayId} circuit is ${gate.state}; call fast-failed`,
        ERROR_CODES.CIRCUIT_OPEN,
        true,
        503,
        payment.gatewayId,
      );
    }

    const charge = await this.tracing.withSpan(
      SPAN_NAMES.CHARGE,
      { paymentId: payment.id, gatewayId: payment.gatewayId, correlationId: record.correlationId ?? '' },
      async () => gateway.charge(payment, { correlationId: record.correlationId }),
    );
    if (!charge.ok) {
      await this.history(record, 'charge', 'failure', `${charge.failure.code}: ${charge.failure.message}`);
      await this.withCompensation(record, 'release_funds', () => this.releaseFunds(record, reservationId));
      await this.store.save(record);
      await this.events.emit({
        type: 'saga.phase',
        paymentId: payment.id,
        gatewayId: payment.gatewayId,
        phase: 'charge',
        outcome: 'failed',
        at: new Date().toISOString(),
      });
      if (charge.failure.httpStatus === 429 || charge.failure.httpStatus === 503) {
        bucket.onThrottled();
      }
      // Only transient outcomes (5xx/429/network) sample provider health;
      // permanent business rejections are excluded from breaker statistics.
      if (charge.failure.retryable) {
        breaker.recordOutcome(false);
      }
      throw new PaymentProcessingError(
        charge.failure.message,
        charge.failure.code,
        charge.failure.retryable,
        charge.failure.httpStatus,
        payment.gatewayId,
      );
    }

    breaker.recordOutcome(true);
    bucket.onSuccess();
    record.transactionId = charge.transactionId;
    record.reservationId = reservationId;
    record.sagaState = 'charged';
    await this.history(record, 'charge', 'ok', charge.transactionId);
    await this.store.save(record);
    await this.audit.record({
      paymentId: payment.id,
      type: 'saga.charge',
      detail: { transactionId: charge.transactionId, httpStatus: charge.httpStatus },
    });
    await this.events.emit({
      type: 'saga.phase',
      paymentId: payment.id,
      gatewayId: payment.gatewayId,
      phase: 'charge',
      outcome: 'ok',
      at: new Date().toISOString(),
    });

    // ---- Step 3: settle + audit ----
    try {
      await this.tracing.withSpan(
        SPAN_NAMES.SETTLE,
        { paymentId: payment.id, gatewayId: payment.gatewayId, correlationId: record.correlationId ?? '' },
        async () => {
          await this.ledger.settle(payment, charge.transactionId);
        },
      );
    } catch (err) {
      const message = err instanceof Error ? err.message : String(err);
      await this.history(record, 'settle', 'failure', message);
      await this.withCompensation(record, 'refund_charge', () => this.refundCharge(record, charge.transactionId));
      await this.withCompensation(record, 'release_funds', () => this.releaseFunds(record, reservationId));
      record.sagaState = 'compensated';
      await this.store.save(record);
      await this.events.emit({
        type: 'saga.phase',
        paymentId: payment.id,
        gatewayId: payment.gatewayId,
        phase: 'settle',
        outcome: 'failed',
        at: new Date().toISOString(),
      });
      throw new PaymentProcessingError(
        `settle failed after successful charge (${message}); charge ${charge.transactionId} was refunded`,
        ERROR_CODES.SETTLE_FAILED,
        false,
        undefined,
        payment.gatewayId,
      );
    }

    record.sagaState = 'settled';
    await this.history(record, 'settle', 'ok', charge.transactionId);
    await this.audit.record({
      paymentId: payment.id,
      type: 'saga.settle',
      detail: { transactionId: charge.transactionId },
    });
    await this.events.emit({
      type: 'saga.phase',
      paymentId: payment.id,
      gatewayId: payment.gatewayId,
      phase: 'settle',
      outcome: 'ok',
      at: new Date().toISOString(),
    });

    return { transactionId: charge.transactionId, reservationId };
  }

  private async releaseFunds(record: PaymentRecord, reservationId: string): Promise<void> {
    await this.history(record, 'compensation', 'release_funds', reservationId);
    await this.audit.record({
      paymentId: record.id,
      type: 'saga.compensate.release_funds',
      detail: { reservationId },
    });
    this.traceLog.warn(`payment ${record.id}: reservation ${reservationId} released (compensation)`, PaymentSagaService.name);
  }

  private async refundCharge(record: PaymentRecord, transactionId: string): Promise<void> {
    const gateway = this.gateways.get(record.gatewayId);
    const refund = await gateway.refund(transactionId);
    if (!refund.ok) {
      await this.history(record, 'compensation', 'refund_failed', `${refund.failure.code}: ${refund.failure.message}`);
      await this.audit.record({
        paymentId: record.id,
        type: 'saga.compensate.refund_charge',
        detail: { transactionId, ok: false, failure: refund.failure.message },
      });
      return;
    }
    record.refundId = refund.refundId;
    await this.history(record, 'compensation', 'refund_charge', refund.refundId);
    await this.audit.record({
      paymentId: record.id,
      type: 'saga.compensate.refund_charge',
      detail: { transactionId, ok: true, refundId: refund.refundId },
    });
    this.traceLog.warn(
      `payment ${record.id}: charge ${transactionId} refunded as ${refund.refundId} (compensation)`,
      PaymentSagaService.name,
    );
  }

  private withCompensation<T>(record: PaymentRecord, phase: string, fn: () => Promise<T>): Promise<T> {
    return this.tracing.withSpan(
      SPAN_NAMES.COMPENSATION,
      { paymentId: record.id, gatewayId: record.gatewayId, correlationId: record.correlationId ?? '', phase },
      () => fn(),
    );
  }

  private async history(record: PaymentRecord, phase: SagaHistoryEntry['phase'], event: string, detail?: string) {
    record.history.push({ phase, event, detail, at: new Date().toISOString() });
  }
}

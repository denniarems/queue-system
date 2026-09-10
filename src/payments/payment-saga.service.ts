import { Injectable } from '@nestjs/common';
import { EventBus } from '../common/event-bus.js';
import { TraceLogger } from '../common/trace-logger.js';
import { ERROR_CODES, PaymentProcessingError } from '../domain/errors.js';
import { PaymentRecord, SagaHistoryEntry } from '../domain/payment.js';
import { GatewayGuard } from '../gateway/gateway-guard.js';
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
 * The outbound gateway attempt is not the saga's business: it crosses the
 * Gateway Guard (ADR 0005), which rate limits, breaker-gates, invokes the
 * Payment Gateway and classifies the outcome. Charge failures and gating
 * refusals arrive here as one shape, so this module never reads a provider
 * status code or a circuit state.
 *
 * Every step transition and compensation is persisted in the payment record
 * and appended to the immutable audit log before the saga returns or throws.
 */
@Injectable()
export class PaymentSagaService {
  constructor(
    private readonly store: PaymentStore,
    private readonly audit: AuditLogService,
    private readonly guard: GatewayGuard,
    private readonly ledger: SettlementLedger,
    private readonly events: EventBus,
    private readonly tracing: TracingService,
    private readonly traceLog: TraceLogger,
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

    // ---- Step 2: charge through the Gateway Guard (ADR 0005) ----
    // Token wait, breaker gate, the gateway call, failure classification and
    // the feedback into both controls all happen behind this one call. A
    // gating refusal and a gateway rejection are the same shape here.
    const outcome = await this.tracing.withSpan(
      SPAN_NAMES.CHARGE,
      { paymentId: payment.id, gatewayId: payment.gatewayId, correlationId: record.correlationId ?? '' },
      () => this.guard.call(payment, { correlationId: record.correlationId }),
    );
    if (!outcome.ok) {
      await this.history(record, 'charge', 'failure', outcome.error.code);
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
      // Thrown for the worker's benefit: whether to retry is the budget's call.
      throw outcome.error;
    }

    const transactionId = outcome.transactionId;
    record.transactionId = transactionId;
    record.reservationId = reservationId;
    record.sagaState = 'charged';
    await this.history(record, 'charge', 'ok', transactionId);
    await this.store.save(record);
    await this.audit.record({
      paymentId: payment.id,
      type: 'saga.charge',
      detail: { transactionId, httpStatus: outcome.httpStatus },
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
          await this.ledger.settle(payment, transactionId);
        },
      );
    } catch (err) {
      const message = err instanceof Error ? err.message : String(err);
      await this.history(record, 'settle', 'failure', message);
      await this.withCompensation(record, 'refund_charge', () => this.refundCharge(record, transactionId));
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
        `settle failed after successful charge (${message}); charge ${transactionId} was refunded`,
        ERROR_CODES.SETTLE_FAILED,
        undefined,
        payment.gatewayId,
      );
    }

    record.sagaState = 'settled';
    await this.history(record, 'settle', 'ok', transactionId);
    await this.audit.record({
      paymentId: payment.id,
      type: 'saga.settle',
      detail: { transactionId },
    });
    await this.events.emit({
      type: 'saga.phase',
      paymentId: payment.id,
      gatewayId: payment.gatewayId,
      phase: 'settle',
      outcome: 'ok',
      at: new Date().toISOString(),
    });

    return { transactionId, reservationId };
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
    // Compensation crosses the Guard too, but bypasses its gating (ADR 0005).
    const refund = await this.guard.refund(record.gatewayId, transactionId);
    if (!refund.ok) {
      await this.history(record, 'compensation', 'refund_failed', `${refund.error.code}: ${refund.error.message}`);
      await this.audit.record({
        paymentId: record.id,
        type: 'saga.compensate.refund_charge',
        detail: { transactionId, ok: false, failure: refund.error.message },
      });
      const alertMsg = `CRITICAL: Payment ${record.id} compensation refund failed for charge ${transactionId}: ${refund.error.message}`;
      this.traceLog.error(alertMsg, PaymentSagaService.name);
      await this.events.emit({
        type: 'metrics.alert',
        alert: {
          id: `saga.compensation_failed.${record.id}`,
          severity: 'critical',
          message: alertMsg,
          raisedAt: new Date().toISOString(),
        },
        at: new Date().toISOString(),
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

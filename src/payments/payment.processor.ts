import { Injectable } from '@nestjs/common';
import { Job } from 'bullmq';
import { EventBus } from '../common/event-bus.js';
import { TraceLogger } from '../common/trace-logger.js';
import { PaymentProcessingError } from '../domain/errors.js';
import {
  PaymentJobData,
  PaymentRecord,
  transitionToCompleted,
  transitionToDeadLetter,
  transitionToProcessing,
} from '../domain/payment.js';
import { QueueManager } from '../queue/queue-manager.service.js';
import { SPAN_NAMES, TracingService } from '../tracing/tracing.service.js';
import { AuditLogService } from './audit-log.service.js';
import { IdempotencyService } from './idempotency.service.js';
import { PaymentSagaService } from './payment-saga.service.js';
import { PaymentStore } from './payment-store.service.js';

export type ProcessResult =
  | { status: 'completed'; paymentId: string; transactionId?: string }
  | { status: 'dead_letter'; paymentId: string; failureReason: string }
  | { status: 'skipped'; paymentId: string; reason: string };

/**
 * BullMQ worker processor coordinating, per attempt:
 * idempotency verification -> saga (reserve/charge/settle) -> terminal state.
 *
 * Failure strategy (ticket 05):
 *  - permanent gateway rejections never throw: the payment is compensated,
 *    finalized as DEAD_LETTER and routed to the DLQ (job completes cleanly);
 *  - transient failures are rethrown so BullMQ retries with exponential
 *    backoff + jitter; when attempts are exhausted the worker 'failed' event
 *    (see handleJobFailure) routes the payment to the DLQ.
 */
@Injectable()
export class PaymentProcessor {
  constructor(
    private readonly store: PaymentStore,
    private readonly idempotency: IdempotencyService,
    private readonly audit: AuditLogService,
    private readonly saga: PaymentSagaService,
    private readonly events: EventBus,
    private readonly queueManager: QueueManager,
    private readonly tracing: TracingService,
    private readonly traceLog: TraceLogger,
  ) {}

  /** Traced entry point invoked by the BullMQ worker. */
  async process(job: Job<PaymentJobData>): Promise<ProcessResult> {
    const { paymentId, correlationId } = job.data;
    return this.tracing.withSpan(
      SPAN_NAMES.PROCESS,
      { paymentId, correlationId: correlationId ?? '', jobId: job.id },
      () => this.processJob(job),
    );
  }

  private async processJob(job: Job<PaymentJobData>): Promise<ProcessResult> {
    const { paymentId, correlationId } = job.data;
    const startedAt = Date.now();
    const startedIso = new Date(startedAt).toISOString();

    let record = await this.store.get(paymentId);
    if (!record) {
      // Should never happen (records are persisted before enqueue); the job
      // will exhaust retries and surface in the DLQ for inspection.
      throw new Error(`payment record ${paymentId} not found while processing job ${job.id}`);
    }

    const idem = await this.idempotency.get(paymentId);
    if (idem?.state === 'COMPLETED' || idem?.state === 'FAILED') {
      return { status: 'skipped', paymentId, reason: `already finalized as ${record.status}` };
    }
    const locked = await this.idempotency.acquireExecutionLock(paymentId);
    if (!locked) {
      return { status: 'skipped', paymentId, reason: 'concurrent execution lock held by another worker' };
    }

    try {
      transitionToProcessing(record, job.attemptsMade, startedIso);
      await this.store.save(record);
      await this.audit.record({ paymentId, type: 'payment.processing', detail: { attempt: job.attemptsMade + 1 } });
      await this.events.emit({
        type: 'payment.processing',
        paymentId,
        gatewayId: record.gatewayId,
        correlationId,
        status: 'processing',
        at: startedIso,
        detail: { attempt: job.attemptsMade + 1 },
      });

      try {
        const outcome = await this.saga.execute(record);
        transitionToCompleted(record, outcome.transactionId, new Date().toISOString());
        record.sagaState = 'settled';
        record.reservationId = outcome.reservationId;
        record.failureReason = undefined;
        await this.store.save(record);
        await this.idempotency.finalize(paymentId, {
          state: 'COMPLETED',
          paymentStatus: 'completed',
          transactionId: outcome.transactionId,
        });
        await this.audit.record({
          paymentId,
          type: 'payment.completed',
          detail: { transactionId: outcome.transactionId },
        });
        await this.events.emit({
          type: 'payment.completed',
          paymentId,
          gatewayId: record.gatewayId,
          correlationId,
          status: 'completed',
          at: new Date().toISOString(),
        });
        await this.events.emit({
          type: 'job.completed',
          paymentId,
          gatewayId: record.gatewayId,
          ok: true,
          durationMs: Date.now() - startedAt,
          at: new Date().toISOString(),
        });
        return { status: 'completed', paymentId, transactionId: outcome.transactionId };
      } catch (err) {
        const failure =
          err instanceof PaymentProcessingError
            ? err
            : new PaymentProcessingError(err instanceof Error ? err.message : String(err), 'unknown', true);
        await this.events.emit({
          type: 'job.completed',
          paymentId,
          gatewayId: record.gatewayId,
          ok: false,
          durationMs: Date.now() - startedAt,
          at: new Date().toISOString(),
        });
        if (!failure.retryable) {
          return await this.finalizeDeadLetter(record, failure.message, correlationId);
        }
        record.history.push({
          phase: 'charge',
          event: `transient failure (${failure.code}), will retry`,
          detail: failure.message,
          at: new Date().toISOString(),
        });
        await this.store.save(record);
        throw failure;
      }
    } finally {
      await this.idempotency.releaseExecutionLock(paymentId);
    }
  }

  /** Terminal handling for permanent failures and exhausted retries. */
  async finalizeDeadLetter(
    record: PaymentRecord,
    reason: string,
    correlationId?: string,
  ): Promise<ProcessResult> {
    const latest = (await this.store.get(record.id)) ?? record;
    if (latest.status === 'dead_letter' || latest.status === 'completed') {
      return { status: 'skipped', paymentId: record.id, reason: `already ${latest.status}` };
    }
    transitionToDeadLetter(latest, reason);
    await this.store.save(latest);
    await this.idempotency.finalize(record.id, {
      state: 'FAILED',
      paymentStatus: 'dead_letter',
      failureReason: reason,
    });
    await this.audit.record({ paymentId: record.id, type: 'payment.dead_lettered', detail: { reason } });
    await this.events.emit({
      type: 'payment.failed',
      paymentId: record.id,
      gatewayId: latest.gatewayId,
      correlationId,
      status: 'dead_letter',
      at: new Date().toISOString(),
      detail: { reason },
    });
    await this.events.emit({
      type: 'payment.dead_lettered',
      paymentId: record.id,
      gatewayId: latest.gatewayId,
      correlationId,
      status: 'dead_letter',
      at: new Date().toISOString(),
      detail: { reason },
    });
    await this.tracing.withSpan(
      SPAN_NAMES.DLQ,
      { paymentId: record.id, gatewayId: latest.gatewayId, correlationId: correlationId ?? '', reason },
      async () => {
        await this.queueManager.routeToDlq({
          paymentId: record.id,
          correlationId,
          reason,
          deadLetteredAt: new Date().toISOString(),
        });
      },
    );
    this.traceLog.warn(`payment ${record.id} dead-lettered: ${reason}`, PaymentProcessor.name);
    return { status: 'dead_letter', paymentId: record.id, failureReason: reason };
  }

  /** Worker 'failed' event handler: transient retries exhausted. */
  async handleJobFailure(jobData: PaymentJobData, error: Error): Promise<void> {
    const record = await this.store.get(jobData.paymentId);
    if (!record) {
      this.traceLog.error(`failure handler: payment ${jobData.paymentId} not found (${error.message})`, PaymentProcessor.name);
      return;
    }
    if (record.status === 'completed' || record.status === 'dead_letter') return;
    await this.finalizeDeadLetter(record, `retries exhausted: ${error.message}`, jobData.correlationId);
  }
}

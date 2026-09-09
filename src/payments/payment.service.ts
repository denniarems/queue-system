import { BadRequestException, ConflictException, Inject, Injectable, NotFoundException } from '@nestjs/common';
import { APP_CONFIG } from '../config/app-config.js';
import type { AppConfig } from '../config/app-config.js';
import { CorrelationService, EventBus } from '../common/event-bus.js';
import { TracingService, SPAN_NAMES } from '../tracing/tracing.service.js';
import {
  DEFAULT_MAX_RETRIES,
  createPaymentRecord,
  PaymentPriority,
  PAYMENT_PRIORITIES,
  PaymentRecord,
} from '../domain/payment.js';
import { QueueManager } from '../queue/queue-manager.service.js';
import { AuditLogService } from './audit-log.service.js';
import { IdempotencyService } from './idempotency.service.js';
import { PaymentStore } from './payment-store.service.js';

export interface CreatePaymentInput {
  id?: string;
  amount: number;
  currency: string;
  customerId: string;
  gatewayId: string;
  priority?: PaymentPriority;
  maxRetries?: number;
  metadata?: Record<string, unknown>;
  scheduledAt?: Date;
  delayMs?: number;
  correlationId?: string;
}

export type SubmitPaymentResult =
  | { kind: 'created'; payment: PaymentRecord }
  | { kind: 'replayed'; payment: PaymentRecord };

const MAX_DELAY_MS = 2_147_483_000;

function isPlainObject(v: unknown): v is Record<string, unknown> {
  return typeof v === 'object' && v !== null && !Array.isArray(v);
}

/**
 * Validates an inbound payment request. Lightweight manual validation keeps
 * the dependency surface small; every failure is answered with a 400.
 */
export function validateCreatePaymentInput(raw: unknown): CreatePaymentInput {
  if (!isPlainObject(raw)) throw new BadRequestException('body must be a JSON object');
  const body = raw;

  const amount = body['amount'];
  const currency = body['currency'];
  const customerId = body['customerId'];
  const gatewayId = body['gatewayId'];
  const id = body['id'];

  if (typeof id !== 'string' || id.length === 0 || id.length > 100) {
    throw new BadRequestException('id must be a non-empty string (<= 100 chars)');
  }
  if (typeof amount !== 'number' || !Number.isInteger(amount) || amount <= 0) {
    throw new BadRequestException('amount must be a positive integer (minor currency unit)');
  }
  if (typeof currency !== 'string' || !/^[A-Z]{3}$/.test(currency)) {
    throw new BadRequestException('currency must be a 3-letter ISO code');
  }
  if (typeof customerId !== 'string' || customerId.length === 0) {
    throw new BadRequestException('customerId must be a non-empty string');
  }
  if (typeof gatewayId !== 'string' || gatewayId.length === 0 || gatewayId.length > 100) {
    throw new BadRequestException('gatewayId must be a non-empty string (<= 100 chars)');
  }

  const rawPriority = body['priority'] ?? 'normal';
  if (typeof rawPriority !== 'string' || !PAYMENT_PRIORITIES.includes(rawPriority as PaymentPriority)) {
    throw new BadRequestException(`priority must be one of ${PAYMENT_PRIORITIES.join(', ')}`);
  }
  const priority = rawPriority as PaymentPriority;

  const maxRetries = body['maxRetries'] ?? DEFAULT_MAX_RETRIES;
  if (typeof maxRetries !== 'number' || !Number.isInteger(maxRetries) || maxRetries < 0 || maxRetries > 10) {
    throw new BadRequestException('maxRetries must be an integer between 0 and 10');
  }

  let metadata: Record<string, unknown> = {};
  if (body['metadata'] !== undefined) {
    if (!isPlainObject(body['metadata'])) throw new BadRequestException('metadata must be an object');
    metadata = body['metadata'] as Record<string, unknown>;
  }

  const input: CreatePaymentInput = { id, amount, currency, customerId, gatewayId, priority, maxRetries, metadata };

  const scheduledAt = body['scheduledAt'];
  const delayMs = body['delayMs'];
  if (scheduledAt !== undefined && typeof scheduledAt !== 'string' && !(scheduledAt instanceof Date)) {
    throw new BadRequestException('scheduledAt must be an ISO-8601 date string');
  }
  if (delayMs !== undefined && (typeof delayMs !== 'number' || !Number.isInteger(delayMs) || delayMs < 0)) {
    throw new BadRequestException('delayMs must be a non-negative integer');
  }
  if (scheduledAt !== undefined && delayMs !== undefined) {
    throw new BadRequestException('provide either scheduledAt or delayMs, not both');
  }
  if (scheduledAt !== undefined) {
    const at = new Date(scheduledAt as string | Date).getTime();
    if (Number.isNaN(at)) throw new BadRequestException('scheduledAt must be a valid ISO-8601 date string');
    const delta = at - Date.now();
    if (delta > MAX_DELAY_MS) throw new BadRequestException('scheduledAt is too far in the future');
    input.scheduledAt = new Date(at);
  }
  if (delayMs !== undefined) {
    if (delayMs > MAX_DELAY_MS) throw new BadRequestException('delayMs is too large');
    input.delayMs = delayMs;
  }
  return input;
}

/**
 * Application boundary for payments: submission, delayed scheduling, status
 * lookups and DLQ queries. Submissions go through the two-phase idempotency
 * lease, so concurrent/duplicate submissions can never double-charge.
 */
@Injectable()
export class PaymentService {
  constructor(
    private readonly store: PaymentStore,
    private readonly idempotency: IdempotencyService,
    private readonly audit: AuditLogService,
    private readonly queueManager: QueueManager,
    private readonly events: EventBus,
    private readonly correlation: CorrelationService,
    private readonly tracing: TracingService,
    @Inject(APP_CONFIG) private readonly config: AppConfig,
  ) {}

  async submit(input: CreatePaymentInput): Promise<SubmitPaymentResult> {
    const paymentId = input.id!;
    // HTTP middleware binds the request's X-Correlation-Id into the async
    // context; carry it onto the record and every downstream job/span.
    input.correlationId = input.correlationId ?? this.correlation.current();

    // Stale-lease recovery: a PROCESSING record without a stored payment means
    // the previous submit crashed before enqueuing — release and retry once.
    let claim = await this.idempotency.claim(paymentId);
    if (claim.status === 'conflict') {
      const existing = await this.store.get(paymentId);
      if (!existing) {
        await this.idempotency.release(paymentId);
        claim = await this.idempotency.claim(paymentId);
      }
    }

    if (claim.status === 'replayed') {
      const existing = await this.store.get(paymentId);
      if (existing) return { kind: 'replayed', payment: existing };
      throw new NotFoundException({
        message: 'payment already finalized but its record is missing (data inconsistency)',
        id: paymentId,
      });
    }
    if (claim.status === 'conflict') {
      const existing = await this.store.get(paymentId);
      throw new ConflictException({
        message: 'payment is already being processed',
        id: paymentId,
        status: existing?.status ?? 'processing',
      });
    }

    const record = createPaymentRecord({
      id: paymentId,
      amount: input.amount,
      currency: input.currency,
      customerId: input.customerId,
      gatewayId: input.gatewayId,
      priority: input.priority ?? 'normal',
      maxRetries: input.maxRetries,
      metadata: input.metadata ?? {},
      correlationId: input.correlationId,
      scheduledAt: input.scheduledAt?.toISOString(),
    });

    await this.audit.record({ paymentId, type: 'payment.submitted' });
    await this.store.save(record);
    await this.events.emit({
      type: 'payment.queued',
      paymentId,
      gatewayId: record.gatewayId,
      correlationId: record.correlationId,
      status: 'queued',
      at: new Date().toISOString(),
    });

    const delayMs =
      input.scheduledAt !== undefined
        ? Math.max(0, input.scheduledAt.getTime() - Date.now())
        : (input.delayMs ?? 0);

    try {
      await this.tracing.withSpan(
        SPAN_NAMES.ENQUEUE,
        { paymentId: record.id, gatewayId: record.gatewayId, correlationId: record.correlationId ?? '' },
        async () => {
          await this.queueManager.enqueue({
            gatewayId: record.gatewayId,
            paymentId: record.id,
            correlationId: record.correlationId,
            priority: record.priority,
            maxRetries: record.maxRetries,
            delayMs,
            jobId: record.id,
          });
        },
      );
    } catch (err) {
      // Enqueue failed: drop the claim so a resubmission can retry cleanly.
      await this.idempotency.release(paymentId);
      throw new BadRequestException(
        `payment could not be queued: ${err instanceof Error ? err.message : String(err)}`,
      );
    }

    return { kind: 'created', payment: record };
  }

  async getStatus(paymentId: string): Promise<PaymentRecord> {
    const record = await this.store.get(paymentId);
    if (!record) throw new NotFoundException({ message: 'payment not found', id: paymentId });
    return record;
  }

  async listDeadLetters(): Promise<Array<{ paymentId: string; reason: string; deadLetteredAt: string; payment: PaymentRecord | null }>> {
    const entries = await this.queueManager.listDlq();
    const payments = await Promise.all(entries.map((entry) => this.store.get(entry.paymentId)));
    return entries.map((entry, index) => ({
      paymentId: entry.paymentId,
      reason: entry.reason,
      deadLetteredAt: entry.deadLetteredAt,
      payment: payments[index],
    }));
  }
}

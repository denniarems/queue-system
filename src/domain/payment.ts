/**
 * Payment domain model. Vocabulary follows CONTEXT.md: a *Payment* is a request
 * to transfer funds from a customer through an external *Payment Gateway*.
 */

export const PAYMENT_PRIORITIES = ['high', 'normal', 'low'] as const;
export type PaymentPriority = (typeof PAYMENT_PRIORITIES)[number];

/** Numeric BullMQ priority per ADR 0001: high < normal < low (lower = sooner). */
export const PRIORITY_TO_NUMERIC: Record<PaymentPriority, number> = { high: 1, normal: 2, low: 3 };

export const PAYMENT_STATUSES = ['queued', 'processing', 'completed', 'failed', 'dead_letter'] as const;
export type PaymentStatus = (typeof PAYMENT_STATUSES)[number];

export const DEFAULT_MAX_RETRIES = 3;

export interface Payment {
  id: string;
  /** Amount in the currency's minor unit (e.g. cents) to avoid float drift. */
  amount: number;
  currency: string;
  customerId: string;
  gatewayId: string;
  priority: PaymentPriority;
  maxRetries: number;
  metadata: Record<string, unknown>;
  createdAt: string;
  /** When present the payment was scheduled for this timestamp. */
  scheduledAt?: string;
}

export type SagaPhase = 'reserve' | 'charge' | 'settle' | 'compensation';
export type SagaState =
  | 'idle'
  | 'reserved'
  | 'charged'
  | 'settled'
  | 'compensated'
  | 'failed';

export interface SagaHistoryEntry {
  phase: SagaPhase;
  /** What happened inside the phase. */
  event: string;
  at: string;
  detail?: string;
}

export interface PaymentRecord extends Payment {
  status: PaymentStatus;
  /** Number of failed worker attempts so far (mirrors the BullMQ job). */
  retryCount: number;
  sagaState: SagaState;
  history: SagaHistoryEntry[];
  jobId?: string;
  reservationId?: string;
  transactionId?: string;
  refundId?: string;
  failureReason?: string;
  correlationId?: string;
  processedAt?: string;
  completedAt?: string;
}

export function createPaymentRecord(input: {
  id: string;
  amount: number;
  currency: string;
  customerId: string;
  gatewayId: string;
  priority: PaymentPriority;
  maxRetries?: number;
  metadata?: Record<string, unknown>;
  scheduledAt?: string;
  correlationId?: string;
  createdAt?: string;
}): PaymentRecord {
  const now = input.createdAt ?? new Date().toISOString();
  return {
    ...input,
    maxRetries: input.maxRetries ?? DEFAULT_MAX_RETRIES,
    metadata: input.metadata ?? {},
    createdAt: now,
    status: 'queued',
    retryCount: 0,
    sagaState: 'idle',
    history: [],
  };
}

/** Two-phase idempotency record (ADR 0002): PROCESSING -> COMPLETED | FAILED. */
export const IDEMPOTENCY_STATES = ['PROCESSING', 'COMPLETED', 'FAILED'] as const;
export type IdempotencyState = (typeof IDEMPOTENCY_STATES)[number];

export interface IdempotencyRecord {
  paymentId: string;
  state: IdempotencyState;
  /** Snapshot of the payment status recorded when the record was finalized. */
  paymentStatus?: PaymentStatus;
  transactionId?: string;
  failureReason?: string;
  claimedAt?: string;
  updatedAt?: string;
}

export interface AuditEvent {
  paymentId: string;
  type:
    | 'payment.submitted'
    | 'payment.queued'
    | 'payment.processing'
    | 'payment.completed'
    | 'payment.failed'
    | 'payment.dead_lettered'
    | 'saga.reserve'
    | 'saga.charge'
    | 'saga.settle'
    | 'saga.compensate.release_funds'
    | 'saga.compensate.refund_charge'
    | 'idempotency.claimed'
    | 'idempotency.finalized';
  at: string;
  detail?: Record<string, unknown>;
}

/** Payload carried inside BullMQ jobs. */
export interface PaymentJobData {
  paymentId: string;
  correlationId?: string;
}

export const QUEUE_JOB_NAME = 'payment';
export const DLQ_QUEUE_NAME = 'dlq';
export const DLQ_JOB_NAME = 'dead-letter';

import { ApiProperty, ApiPropertyOptional } from '@nestjs/swagger';
import {
  PAYMENT_STATUSES,
  type PaymentStatus,
  type SagaState,
  type SagaPhase,
} from '../../domain/payment.js';

export class SagaHistoryEntryDto {
  @ApiProperty({ description: 'Saga execution phase', example: 'charge', enum: ['reserve', 'charge', 'settle', 'compensation'] })
  phase!: SagaPhase;

  @ApiProperty({ description: 'Event description', example: 'ok' })
  event!: string;

  @ApiProperty({ description: 'ISO 8601 timestamp of the event', example: '2026-09-10T12:00:01.000Z' })
  at!: string;

  @ApiPropertyOptional({ description: 'Additional detail or error message', example: 'tx_stripe_abc123' })
  detail?: string;
}

export class PaymentRecordDto {
  @ApiProperty({ description: 'Payment identifier', example: 'pay_live_001' })
  id!: string;

  @ApiProperty({ description: 'Payment amount in minor unit', example: 5000 })
  amount!: number;

  @ApiProperty({ description: '3-letter currency code', example: 'USD' })
  currency!: string;

  @ApiProperty({ description: 'Customer identifier', example: 'cust_12345' })
  customerId!: string;

  @ApiProperty({ description: 'Payment gateway identifier', example: 'stripe' })
  gatewayId!: string;

  @ApiProperty({ description: 'Priority level', example: 'high', enum: ['high', 'normal', 'low'] })
  priority!: string;

  @ApiProperty({ description: 'Maximum retry attempts allowed', example: 3 })
  maxRetries!: number;

  @ApiProperty({ description: 'Payment metadata', example: { orderId: 'ord_999' } })
  metadata!: Record<string, unknown>;

  @ApiProperty({ description: 'Creation ISO timestamp', example: '2026-09-10T12:00:00.000Z' })
  createdAt!: string;

  @ApiPropertyOptional({ description: 'Scheduled execution timestamp', example: '2026-09-10T15:00:00.000Z' })
  scheduledAt?: string;

  @ApiProperty({ description: 'Current lifecycle status', enum: PAYMENT_STATUSES, example: 'completed' })
  status!: PaymentStatus;

  @ApiProperty({ description: 'Number of worker attempts made so far', example: 1 })
  retryCount!: number;

  @ApiProperty({ description: 'Current saga coordinator state', example: 'settled', enum: ['idle', 'reserved', 'charged', 'settled', 'compensated', 'failed'] })
  sagaState!: SagaState;

  @ApiProperty({ description: 'Audit trail of saga phases', type: [SagaHistoryEntryDto] })
  history!: SagaHistoryEntryDto[];

  @ApiPropertyOptional({ description: 'BullMQ job identifier', example: 'pay_live_001' })
  jobId?: string;

  @ApiPropertyOptional({ description: 'Internal reservation identifier', example: 'res_pay_live_001_1725969600' })
  reservationId?: string;

  @ApiPropertyOptional({ description: 'External gateway transaction identifier', example: 'tx_stripe_abc123' })
  transactionId?: string;

  @ApiPropertyOptional({ description: 'External gateway refund identifier upon compensation', example: 'ref_stripe_xyz789' })
  refundId?: string;

  @ApiPropertyOptional({ description: 'Terminal failure reason', example: 'card_declined: Insufficient funds' })
  failureReason?: string;

  @ApiPropertyOptional({ description: 'Distributed tracing correlation identifier', example: 'corr_987654321' })
  correlationId?: string;
}

export class PaymentQueuedResponseDto {
  @ApiProperty({ description: 'Payment identifier', example: 'pay_live_001' })
  id!: string;

  @ApiProperty({ description: 'Enqueued state', example: 'queued' })
  status!: 'queued';
}

export class PaymentScheduledResponseDto {
  @ApiProperty({ description: 'Payment identifier', example: 'pay_live_001' })
  id!: string;

  @ApiProperty({ description: 'Lifecycle status', example: 'queued' })
  status!: string;

  @ApiProperty({ description: 'ISO timestamp when the job will become eligible for worker execution', example: '2026-09-10T15:00:00.000Z' })
  scheduledFor!: string;
}

export class PaymentReplayedResponseDto {
  @ApiProperty({ description: 'Payment identifier', example: 'pay_live_001' })
  id!: string;

  @ApiProperty({ description: 'Current payment status', example: 'completed' })
  status!: string;

  @ApiProperty({ description: 'Indicates this response was returned idempotently from cache', example: true })
  replayed!: true;

  @ApiProperty({ description: 'Full cached payment record', type: PaymentRecordDto })
  payment!: PaymentRecordDto;
}

export class DeadLetterEntryDto {
  @ApiProperty({ description: 'Payment identifier', example: 'pay_live_001' })
  paymentId!: string;

  @ApiPropertyOptional({ description: 'Correlation ID', example: 'corr_987654321' })
  correlationId?: string;

  @ApiProperty({ description: 'Terminal failure reason', example: 'Retries exhausted (3/3): gateway timeout' })
  reason!: string;

  @ApiProperty({ description: 'ISO timestamp when the job was moved to DLQ', example: '2026-09-10T12:05:00.000Z' })
  deadLetteredAt!: string;

  @ApiPropertyOptional({ description: 'Full payment record snapshot', type: PaymentRecordDto })
  payment?: PaymentRecordDto;
}

export class DeadLetterListResponseDto {
  @ApiProperty({ description: 'Total count of dead-lettered payments', example: 1 })
  count!: number;

  @ApiProperty({ description: 'List of dead-letter entries', type: [DeadLetterEntryDto] })
  entries!: DeadLetterEntryDto[];
}

export class QueueDepthDto {
  @ApiProperty({ description: 'Gateway identifier', example: 'stripe' })
  gatewayId!: string;

  @ApiProperty({ description: 'Waiting jobs count', example: 12 })
  waiting!: number;

  @ApiProperty({ description: 'Active jobs count', example: 4 })
  active!: number;

  @ApiProperty({ description: 'Delayed jobs count', example: 2 })
  delayed!: number;

  @ApiProperty({ description: 'Failed jobs count', example: 0 })
  failed!: number;
}

export class MetricsAlertDto {
  @ApiProperty({ description: 'Alert kind', example: 'error_rate_high', enum: ['error_rate_high', 'circuit_open', 'p95_high', 'queue_backlog', 'dlq_growth', 'compensation_failed'] })
  kind!: string;

  @ApiProperty({ description: 'Detailed alert message', example: 'Stripe circuit breaker OPEN' })
  message!: string;

  @ApiProperty({ description: 'Triggering metric value', example: 0.65 })
  value!: number;

  @ApiProperty({ description: 'Configured threshold', example: 0.5 })
  threshold!: number;

  @ApiProperty({ description: 'ISO timestamp when the alert fired', example: '2026-09-10T12:00:00.000Z' })
  raisedAt!: string;
}

export class MetricsSnapshotResponseDto {
  @ApiProperty({ description: 'Snapshot ISO timestamp', example: '2026-09-10T12:00:00.000Z' })
  timestamp!: string;

  @ApiProperty({ description: 'Rolling window size in seconds', example: 60 })
  windowSeconds!: number;

  @ApiProperty({ description: 'Current transactions per second (throughput)', example: 142.5 })
  tps!: number;

  @ApiProperty({ description: 'Error rate ratio (failed / total attempts)', example: 0.012 })
  errorRate!: number;

  @ApiProperty({
    description: 'Attempt counts within the window',
    example: { ok: 120, failed: 2, total: 122 },
  })
  attempts!: { ok: number; failed: number; total: number };

  @ApiPropertyOptional({ description: '95th percentile latency in milliseconds', example: 180 })
  p95Ms!: number | null;

  @ApiPropertyOptional({ description: '99th percentile latency in milliseconds', example: 350 })
  p99Ms!: number | null;

  @ApiProperty({ description: 'Queue depths per gateway', type: [QueueDepthDto] })
  queueDepths!: QueueDepthDto[];

  @ApiProperty({ description: 'Active system threshold alerts', type: [MetricsAlertDto] })
  alerts!: MetricsAlertDto[];
}

export class HealthResponseDto {
  @ApiProperty({ description: 'Service name', example: 'queue-system' })
  name!: string;

  @ApiProperty({ description: 'Operational health status', example: 'ok' })
  status!: string;
}

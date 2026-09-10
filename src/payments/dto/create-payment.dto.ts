import { ApiProperty, ApiPropertyOptional } from '@nestjs/swagger';
import { PAYMENT_PRIORITIES, type PaymentPriority } from '../../domain/payment.js';

export class CreatePaymentDto {
  @ApiProperty({
    description: 'Unique payment identifier (client-supplied for idempotency)',
    example: 'pay_live_001',
    maxLength: 100,
  })
  id!: string;

  @ApiProperty({
    description: 'Payment amount in minor currency unit (e.g. cents for USD)',
    example: 5000,
    minimum: 1,
  })
  amount!: number;

  @ApiProperty({
    description: '3-letter ISO currency code (case-insensitive)',
    example: 'USD',
    minLength: 3,
    maxLength: 3,
  })
  currency!: string;

  @ApiProperty({
    description: 'Customer identifier',
    example: 'cust_12345',
    maxLength: 100,
  })
  customerId!: string;

  @ApiProperty({
    description: 'Target payment gateway identifier',
    example: 'stripe',
    maxLength: 60,
  })
  gatewayId!: string;

  @ApiPropertyOptional({
    description: 'Payment priority level',
    enum: PAYMENT_PRIORITIES,
    default: 'normal',
    example: 'high',
  })
  priority?: PaymentPriority;

  @ApiPropertyOptional({
    description: 'Maximum retry attempts on transient gateway failure',
    minimum: 0,
    maximum: 10,
    default: 3,
    example: 3,
  })
  maxRetries?: number;

  @ApiPropertyOptional({
    description: 'Arbitrary key-value metadata (PANs and CVVs are strictly forbidden)',
    example: { orderId: 'ord_999', source: 'mobile_app' },
    type: 'object',
    additionalProperties: true,
  })
  metadata?: Record<string, unknown>;

  @ApiPropertyOptional({
    description: 'Delay in milliseconds before the job becomes eligible for worker execution',
    minimum: 0,
    example: 5000,
  })
  delayMs?: number;

  @ApiPropertyOptional({
    description: 'Target timestamp for scheduled payment processing',
    example: '2026-09-10T15:00:00.000Z',
  })
  scheduledAt?: string;

  @ApiPropertyOptional({
    description: 'Distributed tracing correlation identifier',
    example: 'corr_987654321',
  })
  correlationId?: string;
}

export class CreateScheduledPaymentDto extends CreatePaymentDto {
  @ApiPropertyOptional({
    description: 'Delay in milliseconds before the job is processed (must provide either delayMs or scheduledAt)',
    example: 10000,
  })
  override delayMs?: number = undefined;

  @ApiPropertyOptional({
    description: 'Target execution timestamp (ISO 8601 string or Date)',
    example: '2026-09-10T16:00:00.000Z',
  })
  override scheduledAt?: string = undefined;
}

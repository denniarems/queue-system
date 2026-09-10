import {
  Body,
  Controller,
  Get,
  HttpStatus,
  Param,
  Post,
  Query,
  Res,
} from '@nestjs/common';
import {
  ApiBody,
  ApiHeader,
  ApiOperation,
  ApiParam,
  ApiQuery,
  ApiResponse,
  ApiTags,
} from '@nestjs/swagger';
import type { Response } from 'express';
import { CreatePaymentDto, CreateScheduledPaymentDto } from './dto/create-payment.dto.js';
import {
  DeadLetterListResponseDto,
  PaymentQueuedResponseDto,
  PaymentRecordDto,
  PaymentReplayedResponseDto,
  PaymentScheduledResponseDto,
} from './dto/payment-response.dto.js';
import { PaymentService, validateCreatePaymentInput } from './payment.service.js';

@ApiTags('Payments')
@Controller('payments')
export class PaymentsController {
  constructor(private readonly payments: PaymentService) {}

  /** Submit an immediate payment. 201 when newly queued, 200 on idempotent replay. */
  @Post()
  @ApiOperation({
    summary: 'Submit an immediate payment',
    description: 'Ingests a payment request with atomic idempotency (ADR 0002). Returns 201 when newly queued, 200 on idempotent replay.',
  })
  @ApiHeader({
    name: 'X-Correlation-Id',
    required: false,
    description: 'Optional distributed tracing correlation ID. If omitted, a UUID is automatically generated.',
  })
  @ApiBody({ type: CreatePaymentDto })
  @ApiResponse({ status: 201, description: 'Payment accepted and enqueued', type: PaymentQueuedResponseDto })
  @ApiResponse({ status: 200, description: 'Idempotent replay: payment was already submitted', type: PaymentReplayedResponseDto })
  @ApiResponse({ status: 400, description: 'Validation error in request payload' })
  async submit(@Body() body: CreatePaymentDto | unknown, @Res({ passthrough: true }) res: Response) {
    const input = validateCreatePaymentInput(body);
    const result = await this.payments.submit(input);
    if (result.kind === 'replayed') {
      res.status(HttpStatus.OK);
      return { id: result.payment.id, status: result.payment.status, replayed: true, payment: result.payment };
    }
    res.status(HttpStatus.CREATED);
    return { id: result.payment.id, status: 'queued' };
  }

  /** Schedule a payment with delayMs or scheduledAt. */
  @Post('scheduled')
  @ApiOperation({
    summary: 'Schedule a payment for future execution',
    description: 'Enqueues a delayed payment job using BullMQ delay or scheduledAt timestamp.',
  })
  @ApiHeader({
    name: 'X-Correlation-Id',
    required: false,
    description: 'Optional distributed tracing correlation ID',
  })
  @ApiBody({ type: CreateScheduledPaymentDto })
  @ApiResponse({ status: 201, description: 'Payment scheduled and enqueued', type: PaymentScheduledResponseDto })
  @ApiResponse({ status: 200, description: 'Idempotent replay: payment was already submitted', type: PaymentReplayedResponseDto })
  @ApiResponse({ status: 400, description: 'Validation error in request payload' })
  async schedule(@Body() body: CreateScheduledPaymentDto | unknown, @Res({ passthrough: true }) res: Response) {
    const input = validateCreatePaymentInput(body);
    const result = await this.payments.submit(input);
    if (result.kind === 'replayed') {
      res.status(HttpStatus.OK);
      return { id: result.payment.id, status: result.payment.status, replayed: true, payment: result.payment };
    }
    res.status(HttpStatus.CREATED);
    return {
      id: result.payment.id,
      status: result.payment.status,
      scheduledFor: result.payment.scheduledAt ?? new Date(Date.now() + (input.delayMs ?? 0)).toISOString(),
    };
  }

  /** Current status, retry count, saga state and failure details. */
  @Get(':id')
  @ApiOperation({
    summary: 'Get payment status and execution history',
    description: 'Retrieves payment lifecycle state, retry counts, saga coordinator phase, and append-only audit trail.',
  })
  @ApiParam({ name: 'id', description: 'Payment identifier', example: 'pay_live_001' })
  @ApiResponse({ status: 200, description: 'Current payment state', type: PaymentRecordDto })
  @ApiResponse({ status: 404, description: 'Payment not found' })
  async getStatus(@Param('id') id: string) {
    return this.payments.getStatus(id);
  }
}

@ApiTags('Queues')
@Controller('queues')
export class QueuesController {
  constructor(private readonly payments: PaymentService) {}

  @Get('dlq')
  @ApiOperation({
    summary: 'List dead-lettered payments',
    description: 'Inspects terminal failures stored in the Dead Letter Queue (bull:payments:dlq).',
  })
  @ApiQuery({
    name: 'gatewayId',
    required: false,
    description: 'Filter dead-letter entries by gateway ID',
    example: 'stripe',
  })
  @ApiResponse({ status: 200, description: 'List of dead-letter entries', type: DeadLetterListResponseDto })
  async dlq(@Query('gatewayId') gatewayId?: string) {
    const entries = await this.payments.listDeadLetters();
    const filtered = gatewayId ? entries.filter((e) => e.payment?.gatewayId === gatewayId) : entries;
    return { count: filtered.length, entries: filtered };
  }
}

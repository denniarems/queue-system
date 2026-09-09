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
import type { Response } from 'express';
import { PaymentService, validateCreatePaymentInput } from './payment.service.js';

@Controller('payments')
export class PaymentsController {
  constructor(private readonly payments: PaymentService) {}

  /** Submit an immediate payment. 201 when newly queued, 200 on idempotent replay. */
  @Post()
  async submit(@Body() body: unknown, @Res({ passthrough: true }) res: Response) {
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
  async schedule(@Body() body: unknown, @Res({ passthrough: true }) res: Response) {
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
  async getStatus(@Param('id') id: string) {
    return this.payments.getStatus(id);
  }
}

@Controller('queues')
export class QueuesController {
  constructor(private readonly payments: PaymentService) {}

  @Get('dlq')
  async dlq(@Query('gatewayId') gatewayId?: string) {
    const entries = await this.payments.listDeadLetters();
    const filtered = gatewayId ? entries.filter((e) => e.payment?.gatewayId === gatewayId) : entries;
    return { count: filtered.length, entries: filtered };
  }
}

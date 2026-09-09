import { Inject, Injectable } from '@nestjs/common';
import { Redis } from 'ioredis';
import { REDIS_CLIENT } from '../redis/redis.module.js';
import { Payment } from '../domain/payment.js';

export interface SettlementEntry {
  paymentId: string;
  transactionId: string;
  amount: number;
  currency: string;
  customerId: string;
  gatewayId: string;
  settledAt: string;
}

/**
 * Saga step 3: settle + audit. Persists an append-only settlement entry in
 * Redis. This class is the controlled seam used to exercise settle-failure
 * compensation paths in tests (override the provider with a failing one).
 */
@Injectable()
export class SettlementLedger {
  constructor(@Inject(REDIS_CLIENT) private readonly redis: Redis) {}

  async settle(payment: Payment, transactionId: string): Promise<SettlementEntry> {
    const entry: SettlementEntry = {
      paymentId: payment.id,
      transactionId,
      amount: payment.amount,
      currency: payment.currency,
      customerId: payment.customerId,
      gatewayId: payment.gatewayId,
      settledAt: new Date().toISOString(),
    };
    await this.redis.rpush('ledger:settlements', JSON.stringify(entry));
    return entry;
  }

  async list(): Promise<SettlementEntry[]> {
    const raw = await this.redis.lrange('ledger:settlements', 0, -1);
    return raw.map((line) => JSON.parse(line) as SettlementEntry);
  }
}

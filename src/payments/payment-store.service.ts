import { Inject, Injectable } from '@nestjs/common';
import { Redis } from 'ioredis';
import { REDIS_CLIENT } from '../redis/redis.module.js';
import { PaymentRecord } from '../domain/payment.js';

const keyFor = (paymentId: string) => `payment:${paymentId}`;

/**
 * Ledger of payment records (Redis hash per payment). Every state transition
 * of the saga is persisted here, so `GET /payments/:id` can reconstruct the
 * full lifecycle. Redis is the system of record for this implementation
 * (see spec "Out of Scope": no relational ledger sharding).
 */
@Injectable()
export class PaymentStore {
  constructor(@Inject(REDIS_CLIENT) private readonly redis: Redis) {}

  async save(record: PaymentRecord): Promise<void> {
    await this.redis.hset(keyFor(record.id), 'doc', JSON.stringify(record));
  }

  async get(paymentId: string): Promise<PaymentRecord | null> {
    const doc = await this.redis.hget(keyFor(paymentId), 'doc');
    if (!doc) return null;
    try {
      return JSON.parse(doc) as PaymentRecord;
    } catch {
      return null;
    }
  }
}

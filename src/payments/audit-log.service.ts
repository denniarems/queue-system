import { Inject, Injectable } from '@nestjs/common';
import { Redis } from 'ioredis';
import { REDIS_CLIENT } from '../redis/redis.module.js';
import { AuditEvent } from '../domain/payment.js';

const keyFor = (paymentId: string) => `audit:payment:${paymentId}`;
const AUDIT_TTL_SECONDS = 7 * 86_400;

/**
 * Immutable, append-only audit log per payment (Redis list). Entries are only
 * ever appended — compensating actions, saga phases and terminal states are
 * recorded here for full financial traceability.
 */
@Injectable()
export class AuditLogService {
  constructor(@Inject(REDIS_CLIENT) private readonly redis: Redis) {}

  async record(event: Omit<AuditEvent, 'at'> & { at?: string }): Promise<void> {
    const entry: AuditEvent = { ...event, at: event.at ?? new Date().toISOString() };
    const key = keyFor(event.paymentId);
    await this.redis.rpush(key, JSON.stringify(entry));
    await this.redis.expire(key, AUDIT_TTL_SECONDS);
  }

  async list(paymentId: string): Promise<AuditEvent[]> {
    const raw = await this.redis.lrange(keyFor(paymentId), 0, -1);
    return raw.map((line) => JSON.parse(line) as AuditEvent);
  }
}

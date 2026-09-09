import { Inject, Injectable } from '@nestjs/common';
import { Redis } from 'ioredis';
import { REDIS_CLIENT } from '../redis/redis.module.js';
import { APP_CONFIG } from '../config/app-config.js';
import type { AppConfig } from '../config/app-config.js';
import { IdempotencyRecord } from '../domain/payment.js';

const keyFor = (paymentId: string) => `idempotency:payment:${paymentId}`;

export type ClaimResult =
  | { status: 'new' }
  | { status: 'conflict'; record: IdempotencyRecord }
  | { status: 'replayed'; record: IdempotencyRecord };

/**
 * Two-phase Redis idempotency (ADR 0002).
 *
 * Phase 1 — `claim`: atomically `SET key NX EX <lease>` claiming a PROCESSING
 * lease. Concurrent submissions of the same payment id lose the race and see a
 * conflict; the lease TTL makes a crashed worker's claim expire safely.
 *
 * Phase 2 — `finalize`: the record is rewritten with a final COMPLETED/FAILED
 * state plus transaction metadata and the 24h retention TTL, so resubmissions
 * can be answered from cache without re-executing the charge.
 */
@Injectable()
export class IdempotencyService {
  constructor(
    @Inject(REDIS_CLIENT) private readonly redis: Redis,
    @Inject(APP_CONFIG) private readonly config: AppConfig,
  ) {}

  async get(paymentId: string): Promise<IdempotencyRecord | null> {
    const raw = await this.redis.get(keyFor(paymentId));
    if (!raw) return null;
    try {
      return JSON.parse(raw) as IdempotencyRecord;
    } catch {
      return null;
    }
  }

  /** Phase 1: atomically claim a processing lease. */
  async claim(paymentId: string): Promise<ClaimResult> {
    const now = new Date().toISOString();
    const record: IdempotencyRecord = {
      paymentId,
      state: 'PROCESSING',
      claimedAt: now,
      updatedAt: now,
    };
    const leaseTtl = this.config.idempotency.leaseTtlSeconds;
    const acquired = await this.redis.set(keyFor(paymentId), JSON.stringify(record), 'EX', leaseTtl, 'NX');
    if (acquired === 'OK') return { status: 'new' };

    // Lost the race or a previous lifecycle exists.
    const existing = await this.get(paymentId);
    if (!existing) {
      // Lease present but not yet readable (rare) — treat as in-flight conflict.
      return { status: 'conflict', record };
    }
    if (existing.state === 'PROCESSING') return { status: 'conflict', record: existing };
    return { status: 'replayed', record: existing };
  }

  /** Release the lease without finalizing (e.g. enqueue failed after claim). */
  async release(paymentId: string): Promise<void> {
    await this.redis.del(keyFor(paymentId));
  }

  /** Phase 2: persist the terminal state with the retention TTL. */
  async finalize(
    paymentId: string,
    input: {
      state: 'COMPLETED' | 'FAILED';
      paymentStatus: IdempotencyRecord['paymentStatus'];
      transactionId?: string;
      failureReason?: string;
    },
  ): Promise<void> {
    const previous = await this.get(paymentId);
    const now = new Date().toISOString();
    const record: IdempotencyRecord = {
      paymentId,
      state: input.state,
      paymentStatus: input.paymentStatus,
      transactionId: input.transactionId,
      failureReason: input.failureReason,
      claimedAt: previous?.claimedAt ?? now,
      updatedAt: now,
    };
    await this.redis.set(keyFor(paymentId), JSON.stringify(record), 'EX', this.config.idempotency.retentionSeconds);
  }
}

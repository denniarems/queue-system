import { Inject, Injectable } from '@nestjs/common';
import { APP_CONFIG } from '../config/app-config.js';
import type { AppConfig } from '../config/app-config.js';
import { EventBus } from '../common/event-bus.js';
import { AdaptiveTokenBucket, TokenBucketParams } from './adaptive-token-bucket.js';

/**
 * One AdaptiveTokenBucket per gateway (ADR 0003). Gateways are isolated: a
 * rate-limit meltdown of one provider never affects another gateway's bucket.
 * Per-gateway limits come from the global config; `configure` allows operator
 * or test overrides for an individual gateway.
 */
@Injectable()
export class RateLimiterRegistry {
  private readonly buckets = new Map<string, AdaptiveTokenBucket>();

  constructor(
    @Inject(APP_CONFIG) private readonly config: AppConfig,
    private readonly events: EventBus,
  ) {}

  private createBucket(gatewayId: string, params: TokenBucketParams): AdaptiveTokenBucket {
    return new AdaptiveTokenBucket(params, {
      onThrottle: () => {
        void this.events.emit({
          type: 'rate_limit.throttled',
          gatewayId,
          at: new Date().toISOString(),
        });
      },
    });
  }

  get(gatewayId: string): AdaptiveTokenBucket {
    let bucket = this.buckets.get(gatewayId);
    if (!bucket) {
      bucket = this.createBucket(gatewayId, this.paramsFor(gatewayId));
      this.buckets.set(gatewayId, bucket);
    }
    return bucket;
  }

  configure(gatewayId: string, partial: Partial<TokenBucketParams>): void {
    const bucket = this.buckets.get(gatewayId);
    const params = { ...this.paramsFor(gatewayId), ...partial };
    if (bucket) {
      bucket.reconfigure(params);
    } else {
      this.buckets.set(gatewayId, this.createBucket(gatewayId, params));
    }
  }

  resetAll(): void {
    for (const gatewayId of Array.from(this.buckets.keys())) {
      const bucket = this.buckets.get(gatewayId);
      if (bucket) bucket.reconfigure(this.paramsFor(gatewayId));
    }
  }

  private paramsFor(_gatewayId: string): TokenBucketParams {
    const rl = this.config.rateLimiter;
    return {
      nominalRps: rl.nominalRps,
      burstFactor: rl.burstFactor,
      minRateFactor: rl.minRateFactor,
      aiStepRps: rl.aiStepRps,
      tokenWaitMs: rl.tokenWaitMs,
    };
  }
}

import { Inject, Injectable } from '@nestjs/common';
import { APP_CONFIG } from '../config/app-config.js';
import type { AppConfig } from '../config/app-config.js';
import { ERROR_CODES, PaymentProcessingError } from '../domain/errors.js';
import type { PaymentErrorCode } from '../domain/errors.js';
import type { Payment } from '../domain/payment.js';
import type { TokenBucketParams } from './adaptive-token-bucket.js';
import type { CircuitBreakerParams, CircuitBreakerState } from './circuit-breaker.js';
import { CircuitBreakerRegistry } from './circuit-breaker.registry.js';
import type { GatewayCallContext } from './gateway.types.js';
import { MockGatewayRegistry } from './mock-gateway.service.js';
import { RateLimiterRegistry } from './rate-limiter.registry.js';

/** Result of a gated gateway payment call, with the failure fully classified. */
export type CallOutcome =
  | { ok: true; transactionId: string; httpStatus: number; latencyMs: number }
  | { ok: false; error: PaymentProcessingError };

/** Result of a compensation refund. */
export type RefundOutcome =
  | { ok: true; refundId: string; httpStatus: number; latencyMs: number }
  | { ok: false; error: PaymentProcessingError };

/** Guard policy for one gateway. Provider behaviour is not Guard policy. */
export interface GuardPolicy {
  rateLimiter?: Partial<TokenBucketParams>;
  circuitBreaker?: Partial<CircuitBreakerParams>;
}

/** Flattened, Guard-owned view of a gateway's resilience state. */
export interface GatewayHealth {
  gatewayId: string;
  state: CircuitBreakerState;
  samples: number;
  failures: number;
  failureRate: number;
  tripCount: number;
  rate: number;
  burst: number;
  tokens: number;
  throttleCount: number;
}

/**
 * The single seam between the Payment Saga and a Payment Gateway (ADR 0005).
 *
 * `call` sequences the whole outbound attempt: wait for a Token Bucket token,
 * consult the Circuit Breaker, invoke the Payment Gateway, classify the outcome
 * from `code` and `httpStatus`, then feed throttling back to the bucket and
 * transient degradation back to the breaker. Gateway failures, circuit
 * refusals and rate-limit refusals all leave through one shape, carrying a
 * classified error, so the caller never reads provider status codes.
 *
 * `refund` crosses the same seam but is deliberately exempt from that gating:
 * a Circuit Breaker is usually OPEN precisely when a refund is needed, so
 * gating compensation would leave a Payment charged, unsettled and unrefunded.
 *
 * The Rate Limiter and Circuit Breaker registries are internal seams of this
 * module; nothing outside it reads or mutates them.
 */
@Injectable()
export class GatewayGuard {
  constructor(
    @Inject(APP_CONFIG) private readonly config: AppConfig,
    private readonly gateways: MockGatewayRegistry,
    private readonly rateLimiters: RateLimiterRegistry,
    private readonly breakers: CircuitBreakerRegistry,
  ) {}

  /** Gated charge: rate limited, breaker gated, and classified. */
  async call(payment: Payment, context?: GatewayCallContext): Promise<CallOutcome> {
    const gatewayId = payment.gatewayId;

    const bucket = this.rateLimiters.get(gatewayId);
    const admitted = await bucket.waitForToken(this.config.rateLimiter.tokenWaitMs);
    if (!admitted) {
      return {
        ok: false,
        error: new PaymentProcessingError(
          `gateway ${gatewayId} rate limiter: no token within ${this.config.rateLimiter.tokenWaitMs}ms`,
          ERROR_CODES.RATE_LIMITED,
          429,
          gatewayId,
        ),
      };
    }

    // Fast-fail while OPEN without invoking the gateway.
    const breaker = this.breakers.get(gatewayId);
    const gate = breaker.allowCall();
    if (!gate.allowed) {
      return {
        ok: false,
        error: new PaymentProcessingError(
          `gateway ${gatewayId} circuit is ${gate.state}; call fast-failed`,
          ERROR_CODES.CIRCUIT_OPEN,
          503,
          gatewayId,
        ),
      };
    }

    const charge = await this.gateways.get(gatewayId).charge(payment, context);
    if (!charge.ok) {
      const error = this.toProcessingError(charge.failure, gatewayId);
      // Only throttling sheds load; a business rejection leaves the bucket alone.
      if (charge.failure.httpStatus === 429 || charge.failure.httpStatus === 503) bucket.onThrottled();
      // Only provider degradation samples the breaker; permanent business
      // rejections are excluded from its statistics.
      if (error.retryable) breaker.recordOutcome(false);
      return { ok: false, error };
    }

    breaker.recordOutcome(true);
    bucket.onSuccess();
    return {
      ok: true,
      transactionId: charge.transactionId,
      httpStatus: charge.httpStatus,
      latencyMs: charge.latencyMs,
    };
  }

  /** Compensation: same seam, exempt from the Rate Limiter and Circuit Breaker. */
  async refund(gatewayId: string, transactionId: string): Promise<RefundOutcome> {
    const refund = await this.gateways.get(gatewayId).refund(transactionId);
    if (!refund.ok) {
      return {
        ok: false,
        error: this.toProcessingError(refund.failure, gatewayId),
      };
    }
    return {
      ok: true,
      refundId: refund.refundId,
      httpStatus: refund.httpStatus,
      latencyMs: refund.latencyMs,
    };
  }

  /**
   * Override Guard policy for one gateway. Provider behaviour is not Guard
   * policy and stays on the gateway adapter, so no mock-only type reaches the
   * interface.
   */
  configure(gatewayId: string, policy: GuardPolicy): void {
    if (policy.rateLimiter) this.rateLimiters.configure(gatewayId, policy.rateLimiter);
    if (policy.circuitBreaker) this.breakers.configure(gatewayId, policy.circuitBreaker);
  }

  /**
   * Read-only health for one gateway. Deliberately does not call `allowCall()`,
   * which would transition OPEN -> HALF_OPEN and consume the probe.
   */
  health(gatewayId: string): GatewayHealth {
    const breaker = this.breakers.get(gatewayId).getState();
    const bucket = this.rateLimiters.get(gatewayId).getState();
    return {
      gatewayId,
      state: breaker.state,
      samples: breaker.samples,
      failures: breaker.failures,
      failureRate: breaker.failureRate,
      tripCount: breaker.tripCount,
      rate: bucket.rate,
      burst: bucket.burst,
      tokens: bucket.tokens,
      throttleCount: bucket.throttleCount,
    };
  }

  private toProcessingError(
    failure: { message: string; code: PaymentErrorCode; httpStatus: number },
    gatewayId: string,
  ): PaymentProcessingError {
    return new PaymentProcessingError(failure.message, failure.code, failure.httpStatus, gatewayId);
  }
}

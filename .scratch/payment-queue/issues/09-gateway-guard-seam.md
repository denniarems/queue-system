# 09: Gateway Guard as the Single Seam to a Payment Gateway

**What to build:**
One deep module — the **Gateway Guard** (see CONTEXT.md and ADR 0005) — that owns the only path from the Payment Saga to a Payment Gateway. Today `PaymentSagaService.execute` calls `MockGatewayRegistry.get()`, `RateLimiterRegistry.get()` and `CircuitBreakerRegistry.get()`, then hand-sequences the Token Bucket and the Circuit Breaker and decides what a gateway failure means to each of them by reading `charge.failure.httpStatus`. The Guard absorbs all of it: `call(payment)` waits for a token, consults the breaker, invokes the Payment Gateway, classifies the outcome from `code` and `httpStatus`, and feeds the result back to both controls. The Payment Saga keeps only its own bookkeeping (history, compensation, saga events) and receives a discriminated union whose failure branch carries a fully classified `PaymentProcessingError`. Compensation — refunding a charge — crosses the same seam but is exempt from the gating, because a Circuit Breaker is usually OPEN precisely when a refund is needed. The registries become internal seams of the Guard and stop being exported.

**Blocked by:** 04: Adaptive Token Bucket Rate Limiting, 05: Circuit Breaker, Error Classification, and DLQ

**Status:** ready-for-agent

- [ ] A `GatewayGuard` module owns the only path from the Payment Saga to a Payment Gateway: `call(payment, context?)`, `refund(gatewayId, transactionId)`, `configure(gatewayId, policy)`, `health(gatewayId)`
- [ ] `call()` sequences token wait -> breaker gate -> gateway invocation, classifies the outcome from `code` and `httpStatus`, and feeds 429/503 back to the Token Bucket and transient outcomes back to the Circuit Breaker
- [ ] `call()` returns a discriminated union; gateway failure, circuit refusal and rate-limit refusal all produce one shape carrying a classified `PaymentProcessingError`
- [ ] `refund()` crosses the same seam but is exempt from the Rate Limiter and the Circuit Breaker (ADR 0005)
- [ ] `retryable` is derived from `code` through `RETRYABLE_CODES`; `GatewayFailure` and `MockStep` no longer carry it, and `PaymentProcessingError` no longer accepts it as a constructor argument
- [ ] `UNKNOWN` is retryable, preserving the pre-existing behaviour for unclassified failures
- [ ] `configure(gatewayId, policy)` takes Guard policy only (`rateLimiter`, `circuitBreaker`, both partial); provider behaviour scripting stays on the gateway adapter so `MockGatewayBehavior` never enters the Guard's interface
- [ ] `health(gatewayId)` returns a flattened, Guard-owned shape exposing breaker state/samples/failures/failureRate/tripCount and rate-limiter rate/burst/tokens/throttleCount
- [ ] `health()` has no side effects — it must not call `allowCall()`, which would transition OPEN -> HALF_OPEN and consume the probe
- [ ] `RateLimiterRegistry` and `CircuitBreakerRegistry` are no longer exported from `GatewayModule`; nothing outside the Guard reads or mutates them
- [ ] `PaymentSagaService` no longer injects the gateway, rate-limiter or breaker registries
- [ ] The charge-step saga history keeps a single `charge:failure` event with the error `code` in `detail`, replacing the ad-hoc `rate_limited` / `circuit_open` / `failure` split
- [ ] Per-gateway rate-limiter and breaker limits sourced from config stay out of scope; `configure(gatewayId, ...)` remains the per-gateway override route
- [ ] `PaymentGateway.getStatus` is untouched (still uncalled in `src/`)

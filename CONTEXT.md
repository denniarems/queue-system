# Payment Processing System

A distributed, fault-tolerant payment processing queue system handling high-volume transactions with rate limiting, circuit breaking, and real-time observability.

## Language

**Payment**:
A request to transfer funds from a customer through an external gateway.
_Avoid_: Transaction, charge, transfer, order

**Payment Gateway**:
An external payment provider (e.g., Stripe, PayPal, Adyen) capable of authorizing and settling payments.
_Avoid_: Processor, vendor, acquirer

**Gateway Guard**:
The module that decides whether a call to a Payment Gateway may proceed, applies the Rate Limiter and the Circuit Breaker, classifies the outcome, and feeds it back to both controls. Compensation — refunding a charge — crosses the same seam but is exempt from that gating.
_Avoid_: Gateway wrapper, Resilience service, Proxy, Dispatcher

**Queue Manager**:
The orchestrating component responsible for dynamically managing gateway queues, worker pools, and lifecycle events.
_Avoid_: Dispatcher, task runner

**Idempotency Record**:
A stateful record in Redis tracking payment execution phase and preventing duplicate processing.
_Avoid_: Deduplication key, cache entry

**Payment Saga**:
A multi-step coordinated workflow (Reserve -> Authorize & Charge -> Settle & Audit) with automated compensation upon failure.
_Avoid_: 2PC, distributed transaction

**Circuit Breaker**:
A per-gateway resilience pattern with Closed, Open, and Half-Open states to isolate failing or degraded external gateways.
_Avoid_: Rate limiter, throttle

**Dead Letter Queue**:
A terminal queue holding payments that suffered permanent failures or exceeded maximum retry attempts.
_Avoid_: Error queue, failed queue

**Rate Limiter**:
A token bucket mechanism enforcing per-gateway throughput limits and adapting dynamically during gateway pressure.
_Avoid_: Throttler, traffic shaper

**Token Bucket**:
A rate-limiting mechanism providing flow control per gateway with adaptive capacity scaling based on failure signals.
_Avoid_: Leaky bucket, static throttle

**Metrics Snapshot**:
An aggregated real-time operational state including rolling TPS, error rates, queue depths, and P95/P99 latencies.
_Avoid_: Telemetry dump, performance log

**Correlation ID**:
A unique tracking identifier propagated across queues, saga execution phases, and external gateway calls to trace an end-to-end payment lifecycle.
_Avoid_: Request ID, trace token

**Alert**:
A high-priority notification emitted when operational thresholds (e.g. error rate spikes, circuit breaker trips, queue backlog) are violated.
_Avoid_: Warning log, notification

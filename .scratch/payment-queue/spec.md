# Spec: Distributed Payment Processing Queue System

Status: ready-for-agent

## Problem Statement

High-volume payment platforms encounter volatile traffic (50K+ transactions/hour), third-party gateway rate limits, intermittent provider downtime, and network dropouts. Without robust queue isolation, transient failures cascade across gateways, duplicate charges can be executed under network retries, and operations teams lack visibility into live bottlenecks, error rates, and queue latencies.

## Solution

A production-grade, distributed payment processing queue system built on NestJS and BullMQ. The system provides:
1. Multi-tier queue partitioning by payment gateway with numeric priority ordering (high, normal, low) and scheduled payment capabilities.
2. Two-phase Redis idempotency enforcement preventing duplicate charges.
3. Resilience patterns including an adaptive token bucket rate limiter per gateway, 3-state circuit breakers, and automatic exponential backoff with jitter.
4. A 3-step Payment Saga coordinating reservation, gateway charge, and settlement with automated compensation rollback on failure.
5. Terminal Dead Letter Queue routing for unrecoverable errors.
6. Centralized Metrics Collector and Socket.IO WebSocket gateway delivering live P95/P99 latency, TPS, and operational alerts, with end-to-end OpenTelemetry distributed tracing.

## User Stories

1. As an API client, I want to submit a payment request with a specified priority (`high`, `normal`, `low`), so that urgent payments are processed ahead of standard transactions.
2. As an API client, I want to schedule a delayed payment request, so that future-dated charges execute automatically at the requested time.
3. As an API client, I want each payment submission to be idempotent, so that network timeouts or duplicate retry attempts never result in multiple charges to the customer.
4. As an operations engineer, I want payments to be segregated into per-gateway queues, so that an outage or slowdown in one payment provider does not stall traffic to healthy providers.
5. As an operations engineer, I want dynamic worker pool management, so that queue consumers scale with load and shut down gracefully during deployments without dropping inflight transactions.
6. As a system architect, I want an adaptive token bucket rate limiter per gateway, so that the system adheres to provider rate limits and automatically throttles down throughput when receiving HTTP 429 or 503 responses.
7. As a system architect, I want the rate limiter to recover capacity gradually (AIMD) as successful charges resume, so that gateway bandwidth is utilized efficiently without re-triggering rate limits.
8. As a reliability engineer, I want transient failures (e.g., connection resets, 5xx gateway errors) to be retried with exponential backoff and jitter, so that temporary hiccups resolve without manual intervention.
9. As a reliability engineer, I want permanent failures (e.g., invalid card numbers, fraudulent flags, insufficient funds) to fail immediately without retry, so that queue capacity is not wasted on unrecoverable requests.
10. As an operations engineer, I want payments that exceed maximum retries or fail permanently to be routed to a Dead Letter Queue (DLQ), so that failures can be inspected and remediated safely.
11. As a reliability engineer, I want each gateway protected by a circuit breaker (Closed, Open, Half-Open), so that upstream calls halt immediately when a gateway experiences sustained degradation.
12. As a customer, I want payment processing to execute as a 3-step Saga (Reserve -> Charge -> Settle/Audit), so that if any downstream step fails, prior actions are compensated (e.g., reservations released, charges voided or refunded).
13. As an auditor, I want an immutable audit trail recorded for every payment transition and saga compensation, so that full financial traceability is maintained.
14. As an operations engineer, I want real-time metrics tracking queue depths, throughput (TPS), success/failure ratios, and P95/P99 latencies, so that system health is observable at all times.
15. As a dashboard user, I want real-time WebSocket event streaming (`metrics:snapshot`, `payment:event`, `alert:raised`), so that operators can monitor traffic flow and receive instant alerts when error or latency thresholds are breached.
16. As a developer debugging issues, I want correlation IDs propagated through every job, span, log line, and gateway request using OpenTelemetry, so that distributed payment lifecycles can be traced end-to-end.

## Implementation Decisions

### Architectural Topology
- **Single-Context Repository**: Conforms to the single-context layout specified in `CONTEXT.md` and `docs/agents/domain.md`.
- **Per-Gateway Queues with Native Priority (ADR 0001)**: BullMQ queues partitioned per gateway (`payments:{gatewayId}`), with high (priority: 1), normal (priority: 2), and low (priority: 3) handled by BullMQ's native numeric priority mechanism. A separate `payments:dlq` holds terminal failures.
- **Two-Phase Idempotency Record (ADR 0002)**: Redis key `idempotency:payment:{paymentId}` atomically acquired via lease (`SET ... NX EX`) upon ingestion, transitioning to `COMPLETED` or `FAILED` with a 24-hour TTL upon conclusion.
- **Adaptive Token Bucket (ADR 0003)**: Token bucket per gateway using Additive Increase / Multiplicative Decrease (AIMD). Tokens are acquired before gateway dispatch; failure triggers backpressure reduction.
- **Rolling Window Metrics & WebSockets (ADR 0004)**: In-memory sliding time window (60s buffer) computing live TPS, error percentage, and P95/P99 latency without querying expensive Redis historical ranges. Streamed via NestJS WebSocket Gateway (Socket.IO).

### Core Modules
1. **`QueueModule`**:
   - `QueueManager`: Manages lifecycle, registration of dynamic gateway queues, worker pool dispatch, pause/resume, and graceful shutdown.
   - Enqueues jobs with priority, delay (scheduled payments), and correlation metadata.
2. **`PaymentModule`**:
   - `PaymentProcessor`: BullMQ worker processor coordinating idempotency verification, rate limiter checks, circuit breaker evaluation, and saga execution.
   - `PaymentSagaService`: Coordinates the 3-step saga:
     - Step 1: `reserveFunds(payment)`
     - Step 2: `chargeGateway(payment)`
     - Step 3: `settleAndAudit(payment, result)`
     - Compensations: `releaseFunds(payment)` and `refundGateway(payment, transactionId)`.
   - `PaymentService`: Application boundary offering payment submission, delayed scheduling, status lookups, and DLQ queries.
3. **`GatewayModule`**:
   - `PaymentGateway` interface: `process(payment)` and `getStatus(transactionId)`.
   - `CircuitBreaker`: State machine (`CLOSED`, `OPEN`, `HALF_OPEN`) tracking sliding failure rates.
   - `AdaptiveTokenBucket`: Per-gateway token bucket with dynamic refill rate adjustment.
   - `MockGatewayService`: Configurable mock implementations (e.g. Stripe, PayPal) capable of simulating latency, 429 throttling, transient 5xx errors, and permanent 4xx failures for deterministic testing.
4. **`MetricsModule`**:
   - `MetricsCollector`: Ingests completion times, errors, and queue counts to produce live `MetricsSnapshot` objects.
   - `MetricsGateway`: Socket.IO gateway broadcasting `metrics:snapshot` (1s intervals), `payment:event`, and `alert:raised`.
5. **`TracingModule`**:
   - OpenTelemetry wrapper extracting/injecting `correlationId` into jobs, creating spans for each saga phase.

### API Contracts
- `POST /payments`: Submit immediate payment (`{ id, amount, currency, customerId, gatewayId, priority, metadata }`).
- `POST /payments/scheduled`: Submit delayed payment (`{ ..., delayMs }` or `{ ..., scheduledAt }`).
- `GET /payments/:id`: Fetch payment status, retry count, saga state, and failure details.
- `GET /queues/metrics`: Query current queue depths, TPS, error rates, and P95/P99 latencies.
- `GET /queues/dlq`: Retrieve unrecoverable dead letter payments.

## Testing Decisions

### Seam Strategy
- **Primary Driving Seam**: Tests drive the system at the highest architectural boundary — via the REST API controller or `PaymentService` / `QueueManager`.
- **Primary Observation Seam**: Assertions inspect final state through `getPaymentStatus(id)`, DLQ retrieval, and `MetricsCollector.getSnapshot()`.
- **Controlled Seam Boundary**: Only external payment gateways (`PaymentGateway`) are mocked via `MockPaymentGateway`. All internal services (BullMQ, Redis idempotency, worker concurrency, token bucket rate limiter, circuit breaker, saga orchestrator, and metrics aggregator) execute together as real code.

### Good Test Principles
- Avoid testing internal private methods or variables.
- Validate behavior under realistic edge cases:
  - Concurrent submissions of identical payment IDs must execute exactly once.
  - Gateway rate limiting must induce queue delay without losing jobs.
  - Gateway circuit breaker must trip to `OPEN` on consecutive 5xx errors and recover on `HALF_OPEN` probe success.
  - Settle failure in the saga must execute compensation to refund the gateway charge.
  - Non-retryable permanent errors (e.g. invalid card) must immediately land in the DLQ.

## Out of Scope

- Integrating with live external banking credentials / real credit card processors.
- Front-end graphical single-page application (WebSocket stream is verified programmatically and via test client).
- Distributed relational database sharding (ledger state and audit logs are managed via in-memory/Redis repository for this implementation).

## Further Notes

- Includes a `docker-compose.yml` defining Redis for local development.
- Automated tests in Vitest run cleanly against either local Redis or embedded mock Redis (`ioredis-mock`).

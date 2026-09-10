# Architectural & System Design Q&A

This document provides in-depth technical analyses and trade-off rationales for key architectural questions regarding the **Distributed Payment Processing Queue System**.

---

## Table of Contents

1. [Why BullMQ vs other queue systems?](#1-why-bullmq-vs-other-queue-systems)
2. [How to prevent duplicate processing?](#2-how-to-prevent-duplicate-processing)
3. [Database vs Redis for state storage](#3-database-vs-redis-for-state-storage)
4. [Microservices vs monolithic approach](#4-microservices-vs-monolithic-approach)
5. [Event sourcing considerations](#5-event-sourcing-considerations)

---

## 1. Why BullMQ vs other queue systems?

### Context & Requirements
The system processes 50,000 to 500,000+ payments per hour across heterogeneous payment gateways (e.g., Stripe, PayPal), requiring:
* Strict failure domain isolation per gateway.
* Priority scheduling (`high`, `normal`, `low`) within gateway queues.
* Delayed and scheduled payment execution (`delayMs` / `scheduledAt`).
* Exponential backoff with jitter and automatic stalled-job recovery.
* Low operational complexity and minimal infrastructure overhead.

### Technical Evaluation

| Feature / Dimension | **BullMQ (Redis 7+)** | **Apache Kafka** | **RabbitMQ** | **AWS SQS (FIFO / Standard)** |
| :--- | :--- | :--- | :--- | :--- |
| **Priority Scheduling** | **Native numeric priority** within a single queue via Redis sorted sets (`prioritized`). | Partition-ordered only; no individual message priority without topic multiplexing. | Priority queues supported, but requires pre-allocated `x-max-priority` memory trees. | No native message priority in a single queue; requires separate queues per priority tier. |
| **Delayed & Scheduled Jobs** | **Arbitrary millisecond delays** via sorted set (`delayed`); exact timer wakeups. | Not supported natively; requires custom external schedulers or stream buffering. | Requires non-core community plugin (`rabbitmq-delayed-message-exchange`). | Delayed queues max 15 minutes; no arbitrary date/time scheduling. |
| **Job Deduplication** | **Atomic job ID deduplication** (`jobId: paymentId`) across waiting/delayed/active states. | Deduplication across idempotent producer keys, but lacks arbitrary job lifecycle deduplication. | Deduplication is manual or requires deduplication plugins. | SQS FIFO provides 5-minute deduplication window; standard SQS has no deduplication. |
| **Operational Footprint** | **Zero extra brokers**; utilizes existing Redis cluster/instance already used for idempotency and state. | Heavy footprint: requires ZooKeeper or KRaft metadata controllers, JVM tuning, and disk volumes. | Requires Erlang runtime, dedicated cluster state management (Mnesia), and cluster monitoring. | Managed AWS service (vendor lock-in, cannot run in local/on-premise hybrid setups). |
| **Throughput Fit** | **50K–500K/hr ($\approx 14\text{–}140\text{ jobs/s}$)** utilizes $<2\%$ of a single Redis instance capacity ($>10\text{K jobs/s}$). | Designed for $>100\text{K msgs/s}$ streaming; massive architectural overkill for this transactional scale. | Sustains $>20\text{K msgs/s}$, but adds routing topology complexity. | Standard SQS is high throughput; SQS FIFO capped at 3,000 msg/s with batching. |
| **Individual Job Acknowledgement** | Fine-grained individual job ACK/NACK, stalled-job detection, and retry budgets. | Offset-based commits; acknowledging message $N$ marks all messages $< N$ as committed (head-of-line blocking). | Individual ACK/NACK supported. | Individual message deletion via receipt handles. |

### Codebase Decision ([ADR 0001](adr/0001-per-gateway-queues.md))
* **Isolation without Queue Explosion:** We provision dedicated BullMQ queues per gateway (`bull:payments:{gatewayId}`) and use BullMQ's native numeric priorities (`1: high`, `2: normal`, `3: low`) inside each queue. This prevents provisioning $N \times 3$ individual physical queues.
* **Unified State:** BullMQ directly shares the Redis 7+ instance used for two-phase idempotency locks, token bucket rate limits, and audit trails.

---

## 2. How to prevent duplicate processing?

### Multi-Tier Defense Strategy
Duplicate payment processing is prevented across two distinct operational boundaries: **Ingress (API/Enqueue)** and **Worker Execution (Processing)**, implementing **Two-Phase Idempotency ([ADR 0002](adr/0002-two-phase-idempotency.md))**:

```
[REST Client] ──► PaymentService.submit()
                      │
                      ├─► Phase 1: SET idempotency:payment:{id} ... EX 600 NX
                      │     ├─► 'new'       ──► Store record & Enqueue BullMQ job (jobId = paymentId)
                      │     ├─► 'conflict'  ──► 409 Conflict (already in-flight)
                      │     └─► 'replayed'  ──► 200 OK with cached result
                      ▼
[BullMQ Worker] ──► PaymentProcessor.processJob()
                      │
                      ├─► Check idempotency:payment:{id} ──► if COMPLETED/FAILED, skip
                      ├─► Acquire lock:payment:process:{id} (SET NX EX 600)
                      ├─► Execute 3-step Saga (Reserve -> Charge -> Settle)
                      ├─► Phase 2: Finalize idempotency:payment:{id} (24h TTL)
                      └─► Release lock:payment:process:{id}
```

### 1. Ingress Phase 1 — Atomic Lease Claim
When a payment request arrives at [`PaymentService.submit`](../src/payments/payment.service.ts):
* An atomic Redis command is executed via [`IdempotencyService.claim`](../src/payments/idempotency.service.ts):
  ```redis
  SET idempotency:payment:{paymentId} '{"paymentId":"...","state":"PROCESSING",...}' EX 600 NX
  ```
* **Status Outcomes:**
  1. **`new` (Winner):** The caller successfully acquired the execution lease. The record is persisted to [`PaymentStore`](../src/payments/payment-store.service.ts) and enqueued to BullMQ. If enqueuing fails unexpectedly, the lease is explicitly released via [`IdempotencyService.release`](../src/payments/idempotency.service.ts) so the client can retry.
  2. **`conflict` (In-flight):** Another request or worker is currently processing this payment. The API immediately rejects the request with `409 Conflict` (`payment is already being processed`). Stale crashes prior to enqueueing are detected and released.
  3. **`replayed` (Finalized):** The payment was already completed or failed. The API returns `200 OK` (`replayed: true`) with the cached result, never re-enqueueing the transaction.

### 2. Queue-Level Deduplication
In [`QueueManager.enqueue`](../src/queue/queue-manager.service.ts), jobs are enqueued with `jobId: input.paymentId`. BullMQ natively deduplicates active, waiting, and delayed jobs sharing that job ID, preventing duplicate queue insertions during network blips.

### 3. Worker-Level Execution Lock & Finalization Re-check
When a worker picks up the job in [`PaymentProcessor.processJob`](../src/payments/payment.processor.ts):
* It verifies whether the idempotency record is already `COMPLETED` or `FAILED`. If so, the job is cleanly skipped.
* It acquires an exclusive distributed execution lock:
  ```redis
  SET lock:payment:process:{paymentId} "1" EX 600 NX
  ```
  If another worker instance is concurrently holding the lock, the attempt is skipped immediately, preventing dual execution during BullMQ stalled-job handoffs.

### 4. Ingress Phase 2 — Finalization with 24-Hour Retention
Once the 3-step payment saga settles (or fails permanently), [`IdempotencyService.finalize`](../src/payments/idempotency.service.ts) updates the idempotency record with the final state (`COMPLETED` or `FAILED`), transaction ID, and a 24-hour retention TTL (`IDEMPOTENCY_RETENTION_SECONDS = 86400`). Subsequent submissions within 24 hours receive the cached response without re-executing gateway charges.

### 5. Gateway-Side Idempotency
Outbound calls through [`GatewayGuard`](../src/gateway/gateway-guard.ts) attach the `paymentId` and `correlationId` to the downstream gateway request, ensuring external providers (e.g., Stripe's `Idempotency-Key` header) safely reject duplicate charges if network drops occur between our worker and their API.

---

## 3. Database vs Redis for state storage

### Current Architecture: Redis as the Operational Store
In the current implementation, Redis acts as the unified operational data store:
* **Payment Records:** Redis hashes (`payment:{id}`, field `doc`) managed by [`PaymentStore`](../src/payments/payment-store.service.ts).
* **Idempotency Leases & Locks:** Key-value leases with TTL (`idempotency:payment:{id}`) and execution locks (`lock:payment:process:{id}`).
* **Audit Trail:** Append-only Redis lists (`audit:payment:{id}`) via [`AuditLogService`](../src/payments/audit-log.service.ts).
* **Settlement Ledger:** Append-only Redis list (`ledger:settlements`) via [`SettlementLedger`](../src/payments/settlement-ledger.service.ts).

### Trade-Off Comparison

| Dimension | **Redis (Current)** | **Relational Database (e.g., PostgreSQL)** |
| :--- | :--- | :--- |
| **Write/Read Latency** | **Sub-millisecond ($<1\text{ms}$)**; zero disk seek overhead on hot path. | $5\text{--}20\text{ms}$ depending on connection pool, transaction locks, and disk flushing. |
| **Idempotency & TTLs** | **Native key TTLs** automatically expire 10m leases and 24h cached entries without background cleanup jobs. | Requires periodic cron/vacuum sweep jobs to prune expired locks and deduplication tables. |
| **Coordination with Queue** | Co-located on the same Redis engine; eliminates distributed transaction coordination during enqueue. | Dual-write hazard: enqueuing to Redis and writing to SQL requires an Outbox Pattern. |
| **Querying & Indexing** | Key-based lookup only (`payment:{id}`); no secondary indices, complex joins, or aggregation without manual secondary sets. | Rich relational queries, secondary indices, range filters, aggregations, and joins. |
| **Financial ACID & Constraints** | Fast single-key atomicity, but lacks multi-table relational foreign keys and double-entry invariants. | Strict ACID transactions, foreign keys, table check constraints, and double-entry audit consistency. |
| **Storage Economics** | High RAM cost: storing months/years of historical financial transactions in memory is cost-prohibitive. | Cost-effective disk storage (NVMe/SSD) with table partitioning and cold object store offloading (S3/WORM). |

### Target Production Hybrid Architecture ([docs/architecture-discussion.md §3](architecture-discussion.md#3-database-sharding-when-a-relational-ledger-arrives))
* **Redis for Hot Operational Data:** Ephemeral queue transport, distributed locks, token buckets, and 24-hour fast idempotency caching.
* **Relational SQL Database (e.g., PostgreSQL) for System of Record:**
  * **Ledger & Settlement:** Double-entry journal entries stored in relational tables with foreign keys and strict constraints.
  * **Sharding Key:** Sharded by `customerId` (or `paymentId` hash) so all transactions for a customer stay on one database shard, eliminating distributed cross-shard 2PC.
  * **Audit Partitioning:** Monthly audit tables (`audit_YYYY_MM`) archived to immutable WORM storage.
  * **Transaction-Free Sagas:** Because [`PaymentSagaService`](../src/payments/payment-saga.service.ts) is fully idempotent and compensated, crash recovery replays from idempotency records without requiring cross-system two-phase commits.

---

## 4. Microservices vs monolithic approach

### Current Architecture: Modular Monolith
The application is structured as a **Modular Monolith** using NestJS:
```
src/
├── payments/    # PaymentService, Idempotency, Saga, SettlementLedger, AuditLog
├── queue/       # QueueManager (BullMQ lifecycle, dynamic pools)
├── gateway/     # GatewayGuard, CircuitBreaker, AdaptiveTokenBucket, MockGateways
├── metrics/     # MetricsCollector (rolling window), MetricsGateway (WebSockets)
├── tracing/     # OpenTelemetry tracing & CorrelationService
└── redis/       # Shared connection pooling
```

### Why Modular Monolith is Optimal Here
1. **Zero RPC Latency on the Financial Hot Path:**
   Saga execution transitions through multiple phases: `Reserve -> Guard Check -> Gateway Charge -> Settle -> Audit`. In a microservices model, each phase represents a remote network hop (HTTP/gRPC) subject to network serialization, latency spikes, and partial network partitions. In our monolith, transitions execute in-process in microseconds.
2. **Clean Architectural Seams ([ADR 0005](adr/0005-gateway-guard-seam.md)):**
   * [`QueueManager`](../src/queue/queue-manager.service.ts) has zero payment domain dependencies (registers callbacks via `setProcessor` and `setFailureHandler`).
   * [`GatewayGuard`](../src/gateway/gateway-guard.ts) completely isolates rate limiting, circuit breaking, and gateway adapters from the saga logic.
3. **Operational Simplicity & End-to-End Tracing:**
   A single deployment pipeline, unified test suite (Vitest runs unit, integration, and E2E in seconds), and unbroken OpenTelemetry context propagation via `AsyncLocalStorage` without cross-service trace dropouts.
4. **Stateless Horizontal Scaling:**
   The application is completely stateless. Scaling from 50K to 500K payments/hour is accomplished by increasing replica counts via the Kubernetes Horizontal Pod Autoscaler ([`k8s/hpa.yaml`](../k8s/hpa.yaml), 3 to 20 replicas), sharing Redis.

### Practical Evolution to Distributed Services
When team topology or organizational scaling demands service decomposition:
1. **Role-Split Deployments (Same Codebase, Separate Roles):**
   Run the monolith in two distinct container roles:
   * **API Role (`--role=api`):** Exposes HTTP REST endpoints, validates input, claims idempotency leases, and enqueues jobs.
   * **Worker Role (`--role=worker`):** Consumes BullMQ queues, executes sagas, and interacts with gateways.
2. **PCI-DSS Tokenization Boundary (Cardholder Data Environment):**
   Extract the gateway communication adapter into an isolated, hardened PCI-scoped microservice to drastically minimize the compliance and audit scope of the primary application.
3. **Analytics & Historical Reporting Service:**
   Consume domain events asynchronously via event streams to update read models and financial reporting dashboards without burdening the transactional processing path.

---

## 5. Event sourcing considerations

### Current Design: State-Based with Append-Only Audit Trail
The current codebase stores the latest state snapshot in [`PaymentRecord`](../src/domain/payment.ts) (`payment:{id}`), while maintaining rich event-driven and append-only primitives:
* **Append-Only Audit Log ([`AuditLogService`](../src/payments/audit-log.service.ts)):** Every payment event (`payment.submitted`, `payment.processing`, `saga.reserve`, `saga.charge`, `saga.settle`, `saga.compensate.*`, `payment.completed`, `payment.dead_lettered`) is appended immutably to `audit:payment:{id}`.
* **Append-Only Settlement Ledger ([`SettlementLedger`](../src/payments/settlement-ledger.service.ts)):** Transactions append immutable double-entry journal records to `ledger:settlements`.
* **Chronological Saga History (`PaymentRecord.history`):** Records every phase outcome and compensation reason.
* **Domain Event Bus ([`EventBus`](../src/common/event-bus.ts)):** Dispatches domain events asynchronously to [`MetricsCollector`](../src/metrics/metrics-collector.service.ts) and streams them over WebSockets.

### Pure Event Sourcing: How It Differs
In a pure Event-Sourced architecture:
1. `PaymentRecord` state is **never stored as a mutable document**.
2. The primary system of record is an **Event Store** (an append-only stream of fine-grained immutable domain events: `PaymentSubmitted`, `FundsReserved`, `PaymentCharged`, `PaymentSettled`, `ReservationReleased`, `ChargeRefunded`).
3. Current state is reconstructed on-demand by replaying events or maintained via materialized read projections (CQRS).

### Key Architectural Considerations & Trade-Offs

```
State-Based + Audit Log (Current)              Pure Event Sourcing
┌────────────────────────────────┐            ┌────────────────────────────────┐
│  State: payment:{id} (Hash)    │            │  Event Stream: events:{id}     │
│  Audit: audit:payment:{id}     │            │  - PaymentSubmitted            │
│  Locks: SET NX EX (O(1))       │            │  - FundsReserved               │
│  Fast single-key reads         │            │  - PaymentCharged              │
└────────────────────────────────┘            │  - PaymentSettled              │
                                              └────────────────────────────────┘
                                                              │
                                                              ▼
                                                   Projected Read Model (CQRS)
```

#### Advantages of Event Sourcing in Payments
* **Complete Financial Non-Repudiation:** Mathematical audit trail by default; impossible for state to change without a corresponding event.
* **Time-Travel & Forensic Auditing:** Operators can inspect the exact state of any transaction at any given millisecond during dispute investigations.
* **Native Outbox Pattern:** Publishing events to external systems (fraud detection, accounting) is guaranteed without dual-write synchronization bugs.

#### Challenges & Complexities in this Payment Context
1. **Concurrency & Idempotency Collisions:**
   Event sourcing relies on optimistic concurrency control (`expectedVersion`). In a high-throughput queue with rapid retries and worker concurrency, version conflicts cause writes to abort and retry. In contrast, our current Redis `SET NX EX` lease lock resolves concurrency in $O(1)$ time.
2. **Read Latency on Status Queries:**
   Replaying event streams on every status check (`GET /payments/:id`) introduces latency. While materialized read projections (CQRS) solve read speed, asynchronous projections introduce eventual consistency lags where an API client immediately querying after submit might read stale data.
3. **Event Schema Evolution & Upcasting:**
   Financial transaction events must be preserved indefinitely. As payment gateway APIs and regulatory requirements evolve, supporting schema migration and upcasting across event versions incurs significant maintenance overhead.
4. **Saga Orchestration vs. Event Choreography:**
   With event sourcing, multi-step workflows often shift to choreographed event reactions or distributed process managers. This makes tracking end-to-end payment lifecycles and diagnosing partial failures significantly more complex than our explicit, centralized orchestrator in [`PaymentSagaService`](../src/payments/payment-saga.service.ts).

---

## Related Documentation & ADRs
* **[Architecture Discussion & Scaling Guide](architecture-discussion.md)**
* **[ADR 0001: Per-Gateway Queues with Native Priority](adr/0001-per-gateway-queues.md)**
* **[ADR 0002: Two-Phase Redis Idempotency Record](adr/0002-two-phase-idempotency.md)**
* **[ADR 0003: Adaptive AIMD Rate Limiting](adr/0003-adaptive-token-bucket-rate-limiting.md)**
* **[ADR 0004: Rolling Window Metrics & WebSockets](adr/0004-rolling-window-metrics-and-websockets.md)**
* **[ADR 0005: Gateway Guard as Single Gateway Seam](adr/0005-gateway-guard-seam.md)**

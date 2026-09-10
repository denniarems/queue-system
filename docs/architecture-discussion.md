# Architecture Discussion — Distributed Payment Processing Queue System

Status: companion to the implementation (`spec.md`, tickets 01–08)
Companion Q&A: for direct answers to core design questions, see **[Architectural & System Design Q&A (docs/architecture-qa.md)](architecture-qa.md)**.

## 1. System overview (as built)

```
REST clients ──► NestJS (payments controller)
                    │ X-Correlation-Id middleware (OpenTelemetry)
                    ▼
        PaymentService (two-phase idempotency claim: SET NX EX)
                    │ queue.add (jobId = paymentId, priority, delay, attempts)
                    ▼
   QueueManager ──► BullMQ queues: bull:payments:{gatewayId}  (+ bull:payments:dlq)
                    │ worker pool (dynamic per gateway)
                    ▼
        PaymentProcessor: idempotency re-check → Saga
        Saga: Reserve → [rate limiter token] → [circuit breaker] → Charge → Settle
              compensation: release reservation / refund charge
                    │ outcomes
                    ▼
        EventBus ──► MetricsCollector (60s rolling window) ──► Socket.IO (snapshots/payment:event/alert:raised)
        OpenTelemetry spans on every lifecycle step (in-memory or OTLP exporter)
```

Data stores (Redis): payment record `payment:{id}` (hash), idempotency record
`idempotency:payment:{id}` (24h TTL), append-only audit list
`audit:payment:{id}`, settlement ledger list. Queue state lives in BullMQ keys.

Guarantees implemented and verified by the test suite:
- exactly-once execution per payment id (atomic lease + BullMQ jobId dedupe);
- retryable vs permanent failure classification; permanent → DLQ immediately;
- exponential backoff with jitter, per-gateway adaptive token bucket (AIMD),
  3-state circuit breaker with HALF_OPEN probing;
- saga compensation (refund + release) leaves no half-executed financial flow;
- live TPS / error rate / P95/P99 and threshold alerts over WebSockets.

## 2. Scaling from 50K to 500K payments/hour

50K/hr ≈ 14 jobs/s, 500K/hr ≈ 140 jobs/s. The bottleneck is never the queue
(BullMQ sustains >10K jobs/s on one Redis), it is Redis command volume,
worker-process count and the gateway RPS ceilings.

1. **Redis**: 500K/hr ≈ 140 adds/s + 140 completions/s + idempotency/audit
   writes — one Redis 7 instance handles this comfortably (BullMQ recommends
   dedicated instances per queue family; split writes onto a replica for
   read-only metrics later). Move payment-state reads (GET /payments/:id,
   DLQ inspection) to replicas when read:write skew grows.
2. **Workers**: run the app stateless across N nodes; BullMQ's Redis locks
   guarantee each job runs once even with many workers per queue. Scale the
   worker pool by gateway (QueueManager.setWorkerPoolSize) with load triggers
   (waiting depth from `GET /queues/metrics` — already emitted as alerts).
   Horizontal worker scaling requires no architectural change: jobs are
   self-describing by `paymentId` and state is in Redis.
3. **Queue partitioning**: per-gateway queues already isolate failure domains.
   If a single gateway exceeds ~5-10K jobs/s, shard by customer hash
   (`payments:{gateway}:{shard}`) with a deterministic hash → no ordering
   requirements per customer; each shard keeps its own worker pool. The
   QueueManager abstraction (queue name + pool) makes this a config change.
4. **Idempotency/audit write path**: single Redis KEYS→ lookups are fine at
   140/s; batch the audit appends (pipeline) and give audit/ledger their own
   Redis DB index so page-cache pressure from queue keys never evicts them.
5. **Rate limiting today** is an in-memory token bucket per app instance —
   correct *per instance*; for many instances, enforce the provider ceiling
   with a Redis-backed distributed bucket or BullMQ's global rate limiter,
   and treat local buckets as a first, coarse gate.

## 3. Database sharding (when a relational ledger arrives)

Today's ledger is Redis + an append-only audit list — appropriate for the
scope (no relational sharding, per spec). If payment state moves to SQL:

- Shard on `customerId` (or `paymentId` hash) — all writes for one payment
  stay on one shard (no cross-shard transactions in the hot path).
- The saga is *transaction-free by design*: each step is idempotent, so
  per-shard transactions are unnecessary; crash recovery replays from the
  idempotency record.
- Archive: partition audit by month (`audit_YYYY_MM`); keep hot lookups on a
  per-payment projection, history in append-only storage (object store /
  event log) — audit immutability maps naturally to an event log.
- Idempotency keys need a global uniqueness scope only per payment id; keep
  them in Redis (fast, TTL-native) and let the SQL ledger be a projection.

## 4. Multi-region deployment and disaster recovery

A payment queue system must pick one *authoritative region* per payment to
avoid double-charges; eventual replication of Redis/BullMQ state is not safe
for the claim step.

- **Primary/active-passive**: one region owns Redis (or a managed
  Redis/Valkey with synchronous cross-AZ replication); the other region runs
  warm workers pointed at the primary via private network (read-only replicas
  locally for status reads). DNS/failover cuts over as a unit.
- **RPO/RTO**: Redis AOF (everysec→always for the claim keys) gives RPO ≤ 1s;
  BullMQ jobs are durable in Redis, so on failover the queue resumes where it
  stopped. RTO ≈ failover time + drain of in-flight jobs (workers re-attach
  and stalled jobs are re-run by BullMQ's stalled-job handling — safe because
  every step is idempotent and the saga compensates).
- **Gateway traffic is inherently cross-region** (Stripe etc. are global):
  outbound calls carry `X-Correlation-Id` (ticket 08) so provider-side logs
  join our traces; retries respect idempotency keys on the gateway side
  (real providers require the same two-phase pattern we model).
- **Network partitions**: a partitioned region must stop *charging* (circuit
  breaker + short lease on idempotency claims) rather than risk duplicate
  charges; queue them, don't drop them.

## 5. Security & PCI

This repo models *plumbing*, not card data — the mock gateway never sees PANs.
For a production deployment:

- **PCI scope**: card data should never touch the application runtime —
  use a tokenization vault or gateway-hosted fields. In our model,
  `Payment.metadata` must never contain PANs/CVV (enforce a schema allowlist).
- **Encryption**: TLS everywhere (API, Redis in transit, OTLP exporter);
  at rest, Redis AOF + snapshots encrypted, key management via a KMS.
- **Audit**: the append-only audit log (immutable by construction — entries
  are only appended) records every financial transition and compensation;
  ship it to a WORM store for the retention window.
- **Access control**: DLQ inspection and metrics are operator APIs — put them
  behind mTLS/service identity today; API keys for clients.
- **Sensitive logging**: correlation ids, payment ids and amounts only; never
  full card data; structured logs with the correlation id bound (ticket 08).

## 6. Trade-offs and design choices (with alternatives)

| Choice | Why | Alternative considered |
|---|---|---|
| BullMQ per gateway (ADR 0001) | failure isolation + native priority/delay/retries | one shared queue + routing (head-of-line blocking across gateways) |
| Redis two-phase idempotency (ADR 0002) | crash-safe leases, 24h cache of results | BullMQ jobId dedupe alone (lost on job cleanup) |
| In-memory adaptive bucket (ADR 0003) | zero extra Redis ops on hot path; AIMD matches gateway behavior | Redis distributed bucket (needed only at many instances) |
| 3-state circuit breaker with HALF_OPEN probe | fails fast, self-heals without human action | exponential-only backoff (hammers a dying provider) |
| Saga with compensation, no 2PC | consistent eventual state w/o distributed transactions | 2PC (blocks, needs coordinator) |
| Rolling 60s in-memory metrics (ADR 0004) | live P95/P99 without Redis history queries | Redis time-series (heavier; better for long-term) |
| Job = worker-owned in-slot token wait | keeps priority order & bounded retries | pause/resume whole queue (coarse) |
| BullMQ native attempts+backoff for transients | battle-tested retry state machine | re-queue from processor (reinvents it) |

Notable implementation facts learned while building (documented for future
readers):
- BullMQ 6 forbids `:` in queue names — we put the partition in the Redis
  prefix (`bull:payments:{gateway}`) to keep ADR-0001 naming.
- BullMQ 6 emits the worker `failed` event on **every** failed attempt, not
  only after retries are exhausted; the DLQ path checks
  `attemptsMade >= opts.attempts`.
- BullMQ 6 stores prioritized jobs in a separate `prioritized` set — queue
  depth metrics must sum `waiting + prioritized`.
- BullMQ has no supported Redis-less mode; tests run against a throwaway
  Redis container provisioned by the vitest global setup (the spec's
  "embedded mock Redis" branch is not implementable with BullMQ 6).

## 7. Observability runbook sketch

- `GET /queues/metrics` — instant snapshot (TPS, error %, P95/P99, depths).
- `ws://…` — `metrics:snapshot` (1s), `payment:event`, `alert:raised`.
- Alerts: error rate > 10%, P95 > 1s, waiting depth > 200, breaker OPEN
  (thresholds env-configurable).
- Tracing: set `OTEL_EXPORTER_OTLP_ENDPOINT` to export spans; every span of a
  payment carries `correlationId`/`paymentId` so a failing payment can be
  followed from the HTTP boundary to the gateway call and back.

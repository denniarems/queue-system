# 07: Rolling Window Metrics and WebSocket Dashboard

**What to build:**
A real-time observability engine and WebSocket streaming gateway. `MetricsCollector` aggregates throughput (TPS), success/failure counts, and execution durations inside a rolling 60-second time window to compute accurate P95 and P99 latencies without heavy database queries. A Socket.IO WebSocket gateway broadcasts periodic `metrics:snapshot` payloads, real-time `payment:event` status changes, and immediate `alert:raised` events when error rates (>10%), queue depth, or latency thresholds are breached. `GET /queues/metrics` exposes current snapshot data over HTTP.

**Blocked by:** 02: Per-Gateway Queues, Priorities, and Scheduling, 05: Circuit Breaker, Error Classification, and DLQ

**Status:** ready-for-agent

- [ ] `MetricsCollector` records job completions, durations, and failures in a rolling 60-second window
- [ ] Accurately calculates live TPS, error rate percentage, and P95 and P99 latency percentiles
- [ ] Aggregates active, waiting, delayed, and failed queue depths across all BullMQ queues
- [ ] Socket.IO gateway emits `metrics:snapshot` event every 1-2 seconds to connected clients
- [ ] Socket.IO gateway emits `payment:event` on lifecycle transitions
- [ ] Threshold breach triggers `alert:raised` event over WebSocket (e.g. error rate > 10%, circuit open)
- [ ] `GET /queues/metrics` returns the latest operational metrics snapshot

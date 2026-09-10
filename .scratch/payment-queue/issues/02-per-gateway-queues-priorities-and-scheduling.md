# 02: Per-Gateway Queues, Priorities, and Scheduling

**What to build:**
Dynamic multi-tier queue management that partitions incoming traffic into dedicated queues per payment gateway, enforces priority scheduling (high, normal, low), and executes scheduled/delayed payments. High-priority payments overtake pending normal/low jobs within each gateway queue, and delayed payments remain deferred until their designated execution time. The worker pool can be scaled dynamically per gateway and responds to shutdown signals gracefully without dropping inflight jobs.

**Blocked by:** 01: Infrastructure Foundation and Ingestion Spike

**Status:** ready-for-agent

- [ ] `QueueManager` dynamically provisions isolated queues and worker pools per gateway (`payments:{gatewayId}`)
- [ ] Enqueued payments with `priority: 'high'` are processed before `priority: 'normal'` and `'low'` payments
- [ ] Submitting a payment with a delay or scheduled time defers execution until the target timestamp
- [ ] Multiple gateways process concurrently without contention or cross-gateway queue blocking
- [ ] Application lifecycle hooks handle graceful shutdown (`close()` on all queues and active workers) on SIGTERM without dropping jobs

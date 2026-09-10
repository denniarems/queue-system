# 03: Two-Phase Redis Idempotency

**What to build:**
A financial-grade idempotency layer preventing duplicate payment processing across network retries and concurrent client submissions. When a payment ID is first received, the system atomically claims an in-progress processing lease in Redis. Subsequent submissions with the same payment ID while in-progress are rejected with a conflict status. Upon completion or failure, the record persists with final status, gateway reference, and a 24-hour TTL, serving cached results for repeated requests.

**Blocked by:** 01: Infrastructure Foundation and Ingestion Spike

**Status:** ready-for-agent

- [ ] Atomically acquire an idempotency lease in Redis (`SET ... NX EX`) before processing a payment
- [ ] Concurrent requests with the same payment ID are detected and blocked from duplicate processing
- [ ] Once completed, the idempotency record transitions to `COMPLETED` with transaction metadata and a 24-hour TTL
- [ ] Resubmitting an already completed payment returns HTTP 200 with the original processed payment record without re-executing the charge
- [ ] In-flight worker crash allows the idempotency lease to safely expire or be cleaned up

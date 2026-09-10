# 05: Circuit Breaker, Error Classification, and DLQ

**What to build:**
A failure classification and resilience system that protects external gateways and prevents poisoned transactions from cycling endlessly. Failures are categorized into transient (network blips, 5xx server errors, rate limits) and permanent (invalid card, insufficient funds, fraud flags). Transient errors retry with exponential backoff and randomized jitter up to `maxRetries`. Permanent failures and exhausted retry attempts route directly to the Dead Letter Queue (`payments:dlq`). A 3-state Circuit Breaker (`CLOSED`, `OPEN`, `HALF_OPEN`) trips open when gateway failure rate breaches 50%, immediately failing fast new requests without bombarding the failing provider.

**Blocked by:** 02: Per-Gateway Queues, Priorities, and Scheduling

**Status:** ready-for-agent

- [ ] Clear distinction between transient errors (retried) and permanent errors (no retry)
- [ ] Transient failures retry with exponential backoff and randomized jitter
- [ ] Permanent failures immediately transition payment to `DEAD_LETTER` status and route to DLQ
- [ ] Jobs exceeding `maxRetries` (default 3) route to `payments:dlq` with failure reason preserved
- [ ] Circuit Breaker trips from `CLOSED` to `OPEN` when error threshold is exceeded over the sample window
- [ ] Circuit Breaker in `OPEN` fast-fails calls without invoking the gateway until cooldown expires
- [ ] Circuit Breaker tests recovery in `HALF_OPEN` state, closing on success or reopening on failure
- [ ] Endpoint `GET /queues/dlq` returns all dead letter payments for inspection

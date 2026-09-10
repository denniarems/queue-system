# 04: Adaptive Token Bucket Rate Limiting

**What to build:**
An adaptive token bucket rate limiter governing dispatch to each payment gateway. Each gateway maintains a token bucket enforcing throughput limits (requests per second). When a gateway begins emitting rate limit responses (HTTP 429) or transient degradation, the rate limiter throttles down its token capacity and refill rate using multiplicative decrease to shed load. As successful transactions resume without errors, capacity recovers gradually via additive increase (AIMD). Jobs unable to obtain tokens are delayed and requeued without losing priority.

**Blocked by:** 02: Per-Gateway Queues, Priorities, and Scheduling

**Status:** ready-for-agent

- [ ] Per-gateway Token Bucket rate limiter regulates job dispatch up to nominal capacity
- [ ] Jobs arriving when no tokens are available are paused/delayed back to the queue
- [ ] Receiving rate-limited (HTTP 429) or degraded responses from a gateway dynamically reduces token refill capacity (multiplicative decrease)
- [ ] Sustained consecutive successful responses gradually restore rate limiter capacity to nominal limits (additive increase)
- [ ] One gateway's rate-limiting or throttling has zero impact on other gateways' processing rates

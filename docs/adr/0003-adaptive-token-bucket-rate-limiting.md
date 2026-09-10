# Adaptive Token Bucket Rate Limiting per Gateway

We need to respect payment gateway throughput limits while adjusting dynamically when gateways suffer degradation or emit 429 rate limit errors. We decided to implement an adaptive Token Bucket rate limiter per gateway that uses Additive Increase / Multiplicative Decrease (AIMD) based on gateway responses. When a gateway emits transient throttling or error spikes, token capacity is reduced to shed load, and recovers gradually on consecutive successes.

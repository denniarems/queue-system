# Rolling Window Metrics and Real-Time WebSocket Streaming

We need real-time observability over queue depths, TPS, error rates, and P95/P99 latencies without imposing heavy query load on Redis or losing recent variance. We decided to maintain a rolling time-window buffer of recent execution latencies in `MetricsCollector` to derive live quantiles, paired with a Socket.IO WebSocket gateway that broadcasts periodic metric snapshots and instant threshold alert events.

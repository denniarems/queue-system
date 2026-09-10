# Deployment & Operations Guide

Production deployment guide for the Distributed Payment Processing Queue System.

---

## 1. Overview

The Payment Processing Queue System is composed of two primary runtime tiers:
1. **Stateless Application Nodes (NestJS / BullMQ Workers)**: Exposes the HTTP REST API, streams WebSocket operational metrics, and dynamically consumes per-gateway BullMQ queues.
2. **Stateful Storage Tier (Redis 7.4+ with AOF Persistence)**: Stores BullMQ priority queues, two-phase idempotency locks, and append-only financial audit trails.

---

## 2. Local & Single-Node Deployment (Docker Compose)

### Quick Start
To build the application container and start both Redis and the API service:

```bash
docker compose up -d --build
```

### Checking Services
```bash
# View running containers and health status
docker compose ps

# View API logs
docker compose logs -f api

# View Redis logs
docker compose logs -f redis
```

### Verification
Test that the health endpoint returns `200 OK`:
```bash
curl http://localhost:3000/
# Expected: {"name":"queue-system","status":"ok"}
```

Submit a test payment:
```bash
curl -X POST http://localhost:3000/payments \
  -H "Content-Type: application/json" \
  -d '{"id":"pay_test_001","amount":5000,"currency":"USD","customerId":"cust_01","gatewayId":"stripe"}'
```

---

## 3. Production Kubernetes Deployment

### Manifest Structure
All Kubernetes manifests are located in the `k8s/` directory:
- `k8s/configmap.yaml`: System configuration parameters.
- `k8s/secret.yaml`: Sensitive credentials (e.g., Redis password).
- `k8s/deployment.yaml`: Replicated Pod deployment with zero-downtime rolling update.
- `k8s/service.yaml`: Internal ClusterIP service exposing port 3000.
- `k8s/hpa.yaml`: Horizontal Pod Autoscaler (3 to 20 replicas).

### Deployment Steps
```bash
# 1. Apply configuration and secrets
kubectl apply -f k8s/configmap.yaml
kubectl apply -f k8s/secret.yaml

# 2. Deploy application and internal service
kubectl apply -f k8s/deployment.yaml
kubectl apply -f k8s/service.yaml

# 3. Enable horizontal autoscaling
kubectl apply -f k8s/hpa.yaml

# 4. Monitor rollout status
kubectl rollout status deployment/payment-queue-system
```

### Zero-Downtime & Graceful Draining
The deployment defines:
```yaml
strategy:
  type: RollingUpdate
  rollingUpdate:
    maxSurge: 1
    maxUnavailable: 0
terminationGracePeriodSeconds: 60
```
When a pod receives `SIGTERM`:
1. The Kubernetes ingress immediately removes the terminating pod from the endpoints pool.
2. NestJS shutdown hooks execute (`QueueManager.onApplicationShutdown()`).
3. Workers stop accepting new jobs and wait for all in-flight gateway charges and saga steps to conclude cleanly.
4. If in-flight requests finish within the grace period (up to 60s), the process exits gracefully with exit code 0.

---

## 4. Environment Variables Reference

| Variable | Default | Description |
| :--- | :--- | :--- |
| `PORT` | `3000` | HTTP & WebSocket port. |
| `NODE_ENV` | `production` | Node execution environment. |
| `REDIS_URL` | `""` | Complete Redis URI (overrides `REDIS_HOST`/`REDIS_PORT`). |
| `REDIS_HOST` | `127.0.0.1` | Redis host name. |
| `REDIS_PORT` | `6379` | Redis port. |
| `REDIS_PASSWORD` | `""` | Redis authentication password. |
| `QUEUE_PREFIX` | `bull:payments` | Redis key namespace for BullMQ queues. |
| `QUEUE_WORKER_POOL_SIZE` | `2` | Default worker concurrency per gateway. |
| `QUEUE_BACKOFF_BASE_MS` | `500` | Exponential backoff base delay. |
| `QUEUE_BACKOFF_JITTER` | `30` | Exponential backoff jitter percentage. |
| `IDEMPOTENCY_LEASE_TTL_SECONDS` | `600` | Crash-safe lock lease for in-progress payments (10m). |
| `IDEMPOTENCY_RETENTION_SECONDS` | `86400` | Finalized payment record retention TTL (24h). |
| `RATE_LIMIT_NOMINAL_RPS` | `20` | Nominal tokens per second refill rate. |
| `RATE_LIMIT_BURST_FACTOR` | `2` | Burst multiplier for token bucket. |
| `CIRCUIT_BREAKER_WINDOW_MS` | `30000` | Sliding window duration for circuit breaker (30s). |
| `CIRCUIT_BREAKER_FAILURE_THRESHOLD` | `0.5` | Failure ratio (50%) to trip breaker to `OPEN`. |
| `CIRCUIT_BREAKER_COOLDOWN_MS` | `30000` | Cooldown duration before `HALF_OPEN` probe. |
| `METRICS_BROADCAST_INTERVAL_MS` | `1000` | Broadcast frequency for WebSocket metrics snapshots. |
| `OTEL_EXPORTER_OTLP_ENDPOINT` | `""` | OpenTelemetry OTLP HTTP collector endpoint. |

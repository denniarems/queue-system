# 08: OpenTelemetry Tracing and E2E Verification

**What to build:**
End-to-end distributed tracing using OpenTelemetry and a comprehensive integration and chaos verification test suite. Every payment request propagates a `correlationId` across the API boundary, queue job metadata, OpenTelemetry spans around each saga step (`reserve`, `charge`, `settle`, `compensation`, `dlq`), and external gateway call headers. An automated end-to-end benchmark test simulates concurrent high-volume traffic (50K+/hr scale) across multiple gateways with varying priorities, inducing rate limits and transient faults to verify autonomous recovery, circuit stability, and metrics accuracy.

**Blocked by:** 06: Payment Saga and Compensation Rollback, 07: Rolling Window Metrics and WebSocket Dashboard

**Status:** ready-for-agent

- [ ] Propagate `correlationId` through HTTP request headers, BullMQ job metadata, and external gateway requests
- [ ] Create OpenTelemetry spans for each payment lifecycle step (enqueue, reserve, charge, settle, compensation, DLQ)
- [ ] Correlation IDs and spans are bound to all log outputs for distributed tracing
- [ ] End-to-end integration test verifies full payment flow across multiple concurrent gateways and priorities
- [ ] Chaos verification tests simulate gateway rate limiting and outages, verifying circuit recovery, saga compensation, and DLQ routing under load
- [ ] Architecture documentation in `docs/architecture-discussion.md` addressing Part 2 questions (scaling to 500K/hr, database sharding, multi-region disaster recovery, PCI compliance)

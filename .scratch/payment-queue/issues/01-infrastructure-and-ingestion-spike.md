# 01: Infrastructure Foundation and Ingestion Spike

**What to build:**
A running payment ingestion pipeline where API clients can submit a payment request to the system and verify that it moves from queued to completed. Operators have a containerized Redis service available, while automated test suites execute smoothly using an in-memory Redis mock without external dependencies. A baseline worker processes the payment through a mock gateway and records the completed status.

**Blocked by:** None (can start immediately)

**Status:** ready-for-agent

- [ ] Redis service definition exists for local development (`docker-compose.yml`)
- [ ] Application connects to Redis with configurable host and port via environment configuration
- [ ] Automated tests run successfully with an in-memory Redis mock without requiring external services
- [ ] Submitting a valid payment payload via `POST /payments` returns HTTP 201 with payment ID and status `queued`
- [ ] A background worker consumes the payment job, simulates gateway authorization, and updates status to `completed`
- [ ] `GET /payments/:id` returns the current status and payment details

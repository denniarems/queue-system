# Two-Phase Redis Idempotency Record

To prevent duplicate financial payments under network retries and worker restarts, we decided to implement two-phase idempotency using Redis (`idempotency:payment:{paymentId}`). A lease is atomically acquired upon job processing with a status of PROCESSING, which transitions to COMPLETED or FAILED upon completion with a 24-hour TTL. This avoids relying solely on BullMQ's transient job deduplication.

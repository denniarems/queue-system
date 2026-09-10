# 06: Payment Saga and Compensation Rollback

**What to build:**
A distributed 3-step Saga pattern orchestrating payment lifecycle events with rollback mechanisms. The flow proceeds through `RESERVE` (internal allocation/balance lock), `CHARGE` (external payment gateway execution), and `SETTLE_AND_AUDIT` (ledger update and immutable audit log). If the gateway charge fails permanently, the saga automatically executes compensation to release the reservation. If settlement fails after a successful charge, the saga triggers gateway compensation (refund/void) and releases the reservation, keeping financial records consistent.

**Blocked by:** 03: Two-Phase Redis Idempotency, 05: Circuit Breaker, Error Classification, and DLQ

**Status:** ready-for-agent

- [ ] Payment executes as a discrete 3-step saga: Reserve -> Charge -> Settle
- [ ] If Step 2 (Charge) fails with permanent error, Step 1 compensation runs (releasing fund reservation)
- [ ] If Step 3 (Settle) fails, compensating transactions execute (refund/void gateway charge and release reservation)
- [ ] Saga step execution history and status transitions are persisted with the payment
- [ ] An immutable audit log records all lifecycle events, errors, and compensation actions

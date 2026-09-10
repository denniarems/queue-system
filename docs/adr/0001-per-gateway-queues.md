# Per-Gateway Queues with Native Priority Scheduling

We need to isolate independent payment gateways while supporting high, normal, and low priority payments. We decided to provision a dedicated BullMQ queue per gateway (`payments:{gatewayId}`) and utilize BullMQ's native numeric job priority rather than creating a separate physical queue for every gateway-priority combination. This guarantees failure isolation per gateway while avoiding queue explosion.

import { Module } from '@nestjs/common';
import { CommonModule } from '../common/event-bus.js';
import { CircuitBreakerRegistry } from './circuit-breaker.registry.js';
import { GatewayGuard } from './gateway-guard.js';
import { MockGatewayRegistry } from './mock-gateway.service.js';
import { RateLimiterRegistry } from './rate-limiter.registry.js';

/**
 * The Rate Limiter and Circuit Breaker registries are internal seams of the
 * Gateway Guard and are deliberately not exported (ADR 0005): everything
 * outside this module crosses the Guard instead.
 */
@Module({
  imports: [CommonModule],
  providers: [MockGatewayRegistry, RateLimiterRegistry, CircuitBreakerRegistry, GatewayGuard],
  exports: [MockGatewayRegistry, GatewayGuard],
})
export class GatewayModule {}

import { Module } from '@nestjs/common';
import { CommonModule } from '../common/event-bus.js';
import { CircuitBreakerRegistry } from './circuit-breaker.registry.js';
import { MockGatewayRegistry } from './mock-gateway.service.js';
import { RateLimiterRegistry } from './rate-limiter.registry.js';

@Module({
  imports: [CommonModule],
  providers: [MockGatewayRegistry, RateLimiterRegistry, CircuitBreakerRegistry],
  exports: [MockGatewayRegistry, RateLimiterRegistry, CircuitBreakerRegistry],
})
export class GatewayModule {}

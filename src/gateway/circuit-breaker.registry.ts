import { Inject, Injectable } from '@nestjs/common';
import { APP_CONFIG } from '../config/app-config.js';
import type { AppConfig } from '../config/app-config.js';
import { EventBus } from '../common/event-bus.js';
import { CircuitBreaker, CircuitBreakerParams } from './circuit-breaker.js';

/**
 * One CircuitBreaker per gateway, emitting domain events on state changes so
 * metrics/websocket layers can raise alerts (ticket 07).
 */
@Injectable()
export class CircuitBreakerRegistry {
  private readonly breakers = new Map<string, CircuitBreaker>();

  constructor(
    @Inject(APP_CONFIG) private readonly config: AppConfig,
    private readonly events: EventBus,
  ) {}

  get(gatewayId: string): CircuitBreaker {
    let breaker = this.breakers.get(gatewayId);
    if (!breaker) {
      breaker = this.create(gatewayId);
      this.breakers.set(gatewayId, breaker);
    }
    return breaker;
  }

  configure(gatewayId: string, partial: Partial<CircuitBreakerParams>): void {
    const params = { ...this.paramsFor(), ...partial };
    const breaker = this.breakers.get(gatewayId);
    if (breaker) {
      breaker.reconfigure(params);
    } else {
      const created = this.create(gatewayId, params);
      this.breakers.set(gatewayId, created);
    }
  }

  reset(gatewayId: string): void {
    this.breakers.get(gatewayId)?.reset();
  }

  resetAll(): void {
    for (const breaker of this.breakers.values()) breaker.reset();
  }

  private create(gatewayId: string, params: CircuitBreakerParams = this.paramsFor()): CircuitBreaker {
    return new CircuitBreaker(params, {
      onOpened: () => {
        void this.events.emit({ type: 'circuit.opened', gatewayId, at: new Date().toISOString() });
      },
      onClosed: () => {
        void this.events.emit({ type: 'circuit.closed', gatewayId, at: new Date().toISOString() });
      },
    });
  }

  private paramsFor(): CircuitBreakerParams {
    const cb = this.config.circuitBreaker;
    return {
      windowMs: cb.windowMs,
      failureThreshold: cb.failureThreshold,
      minSamples: cb.minSamples,
      cooldownMs: cb.cooldownMs,
    };
  }
}

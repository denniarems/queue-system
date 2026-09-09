/**
 * 3-state Circuit Breaker per gateway: CLOSED -> OPEN -> HALF_OPEN.
 *
 *  - CLOSED: every call reaches the gateway; outcomes are sampled in a
 *    sliding time window. When the sample count reaches `minSamples` and the
 *    failure rate breaches `failureThreshold`, the breaker trips to OPEN.
 *  - OPEN: `allowCall()` fast-fails callers (no gateway invocation) until
 *    `cooldownMs` elapses, then a single probe is admitted (HALF_OPEN).
 *  - HALF_OPEN: exactly one probe call runs; success closes the breaker,
 *    failure reopens it.
 *
 * Only *transient* gateway outcomes (5xx/429/network) count as failures:
 * permanent business rejections (declined card etc.) are not provider
 * degradation and are excluded from the sample window.
 */

export type CircuitBreakerState = 'CLOSED' | 'OPEN' | 'HALF_OPEN';

export interface CircuitBreakerParams {
  /** Sliding sample window length in ms. */
  windowMs: number;
  /** Failure-rate threshold above which the breaker trips. */
  failureThreshold: number;
  /** Minimum samples in the window before tripping is evaluated. */
  minSamples: number;
  /** Time the breaker stays OPEN before allowing a HALF_OPEN probe. */
  cooldownMs: number;
}

export interface CircuitBreakerSnapshot extends CircuitBreakerParams {
  state: CircuitBreakerState;
  samples: number;
  failures: number;
  failureRate: number;
  openedAt?: number;
  /** How many times the breaker tripped OPEN since creation/reset. */
  tripCount: number;
}

interface OutcomeSample {
  t: number;
  ok: boolean;
}

export interface CircuitBreakerCallbacks {
  onOpened?: () => void;
  onClosed?: () => void;
}

export class CircuitBreaker {
  private state: CircuitBreakerState = 'CLOSED';
  private window: OutcomeSample[] = [];
  private openedAt = 0;
  private probeInFlight = false;
  private tripCount = 0;
  private now: () => number;

  constructor(
    private params: CircuitBreakerParams,
    private readonly callbacks: CircuitBreakerCallbacks = {},
    now: () => number = Date.now,
  ) {
    this.now = now;
  }

  reconfigure(params: CircuitBreakerParams): void {
    this.params = params;
    this.reset();
  }

  reset(): void {
    this.state = 'CLOSED';
    this.window = [];
    this.openedAt = 0;
    this.probeInFlight = false;
    this.tripCount = 0;
  }

  /** May the caller invoke the gateway now? */
  allowCall(): { allowed: boolean; state: CircuitBreakerState } {
    const now = this.now();
    switch (this.state) {
      case 'CLOSED':
        return { allowed: true, state: this.state };
      case 'OPEN':
        if (now - this.openedAt >= this.params.cooldownMs) {
          // Enter HALF_OPEN and admit exactly one probe.
          this.state = 'HALF_OPEN';
          this.probeInFlight = true;
          return { allowed: true, state: 'HALF_OPEN' };
        }
        return { allowed: false, state: 'OPEN' };
      case 'HALF_OPEN':
        if (this.probeInFlight) return { allowed: false, state: 'HALF_OPEN' };
        this.probeInFlight = true;
        return { allowed: true, state: 'HALF_OPEN' };
    }
  }

  /** Report the outcome of a call that actually reached the gateway. */
  recordOutcome(ok: boolean): void {
    const now = this.now();
    if (this.state === 'HALF_OPEN') {
      this.probeInFlight = false;
      if (ok) {
        this.state = 'CLOSED';
        this.window = [];
        this.callbacks.onClosed?.();
      } else {
        this.state = 'OPEN';
        this.openedAt = now;
        this.tripCount += 1;
        this.callbacks.onOpened?.();
      }
      return;
    }
    if (this.state === 'OPEN') {
      // Outcome of a call started before the trip — ignore.
      return;
    }
    // CLOSED: sample and evaluate.
    this.window.push({ t: now, ok });
    this.prune(now);
    const stats = this.windowStats();
    if (stats.samples >= this.params.minSamples && stats.failureRate >= this.params.failureThreshold) {
      this.state = 'OPEN';
      this.openedAt = now;
      this.tripCount += 1;
      this.callbacks.onOpened?.();
    }
  }

  getState(): CircuitBreakerSnapshot {
    this.prune(this.now());
    const stats = this.windowStats();
    return {
      ...this.params,
      state: this.state,
      samples: stats.samples,
      failures: stats.failures,
      failureRate: stats.failureRate,
      openedAt: this.openedAt || undefined,
      tripCount: this.tripCount,
    };
  }

  private prune(now: number): void {
    const cutoff = now - this.params.windowMs;
    while (this.window.length > 0 && this.window[0].t < cutoff) this.window.shift();
  }

  private windowStats(): { samples: number; failures: number; failureRate: number } {
    const failures = this.window.filter((s) => !s.ok).length;
    const samples = this.window.length;
    return { samples, failures, failureRate: samples === 0 ? 0 : failures / samples };
  }
}

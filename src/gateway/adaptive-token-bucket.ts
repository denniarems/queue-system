/**
 * Adaptive Token Bucket (ADR 0003) — per-gateway throughput control with
 * Additive Increase / Multiplicative Decrease (AIMD):
 *
 *  - tokens are acquired before each gateway dispatch; when the bucket is
 *    empty the worker waits (job stays queued in-slot without losing its
 *    place or priority) up to `tokenWaitMs` before giving up to a retry;
 *  - HTTP 429 / 503 responses from the gateway trigger a multiplicative
 *    decrease of the refill rate (and burst ceiling) to shed load;
 *  - consecutive successes recover capacity gradually (additive increase)
 *    back up to the nominal per-gateway rate.
 */

export interface TokenBucketParams {
  /** Nominal refill rate in tokens/second (the provider's agreed limit). */
  nominalRps: number;
  /** Burst capacity as a multiple of the refill rate. */
  burstFactor: number;
  /** The rate never drops below nominalRps * minRateFactor. */
  minRateFactor: number;
  /** Additive increase per successful response (tokens/second). */
  aiStepRps: number;
  /** How long a job waits for a token before bouncing to a retry. */
  tokenWaitMs: number;
}

export interface TokenBucketState extends TokenBucketParams {
  /** Current dynamic refill rate (AIMD). */
  rate: number;
  /** Current dynamic burst ceiling. */
  burst: number;
  tokens: number;
  /** Times a throttling signal caused a multiplicative decrease. */
  throttleCount: number;
}

export interface TokenBucketCallbacks {
  onThrottle?: () => void;
  onTokenGrant?: () => void;
}

export class AdaptiveTokenBucket {
  private rate: number;
  private burst: number;
  private tokens: number;
  private lastRefill = Date.now();
  private throttleCount = 0;

  constructor(
    private params: TokenBucketParams,
    private readonly callbacks: TokenBucketCallbacks = {},
  ) {
    this.rate = params.nominalRps;
    this.burst = params.nominalRps * params.burstFactor;
    this.tokens = this.burst;
  }

  /** Reconfigure (e.g. an operator changes a provider limit). Resets tokens. */
  reconfigure(params: TokenBucketParams): void {
    this.params = params;
    this.rate = params.nominalRps;
    this.burst = params.nominalRps * params.burstFactor;
    this.tokens = this.burst;
  }

  private refill(now: number): void {
    if (now <= this.lastRefill) return;
    const elapsedSeconds = (now - this.lastRefill) / 1000;
    this.tokens = Math.min(this.burst, this.tokens + elapsedSeconds * this.rate);
    this.lastRefill = now;
  }

  /** Consume one token if available (non-blocking). */
  tryAcquire(): boolean {
    const now = Date.now();
    this.refill(now);
    if (this.tokens >= 1) {
      this.tokens -= 1;
      return true;
    }
    return false;
  }

  /** Wait up to timeoutMs for a token, polling the refill. */
  async waitForToken(timeoutMs: number): Promise<boolean> {
    const deadline = Date.now() + timeoutMs;
    for (;;) {
      if (this.tryAcquire()) {
        this.callbacks.onTokenGrant?.();
        return true;
      }
      const now = Date.now();
      if (now >= deadline) return false;
      await sleep(Math.min(15, Math.max(5, deadline - now)));
    }
  }

  /** AIMD: multiplicative decrease on 429/503 gateway responses. */
  onThrottled(): void {
    const { nominalRps, minRateFactor, burstFactor } = this.params;
    this.rate = Math.max(nominalRps * minRateFactor, this.rate * 0.5);
    this.burst = Math.max(nominalRps * minRateFactor * burstFactor, this.burst * 0.5);
    this.tokens = Math.min(this.tokens, this.burst);
    this.throttleCount += 1;
    this.callbacks.onThrottle?.();
  }

  /** AIMD: additive increase on every successful gateway response. */
  onSuccess(): void {
    const { nominalRps, burstFactor, aiStepRps } = this.params;
    const maxBurst = nominalRps * burstFactor;
    if (this.rate < nominalRps) {
      this.rate = Math.min(nominalRps, this.rate + aiStepRps);
    }
    if (this.burst < maxBurst) {
      this.burst = Math.min(maxBurst, this.burst + aiStepRps * burstFactor);
    }
    this.tokens = Math.min(this.tokens, this.burst);
  }

  getState(): TokenBucketState {
    this.refill(Date.now());
    return {
      ...this.params,
      rate: this.rate,
      burst: this.burst,
      tokens: this.tokens,
      throttleCount: this.throttleCount,
    };
  }
}

function sleep(ms: number): Promise<void> {
  return new Promise((resolve) => setTimeout(resolve, ms));
}

/**
 * Central application configuration. Values come from environment variables
 * with sane production defaults; tests override the whole object through the
 * APP_CONFIG injection token (see buildConfigForTest).
 */
import { readFileSync } from 'node:fs';

export interface CircuitBreakerConfig {
  /** Sliding sample window used to compute the failure rate. */
  windowMs: number;
  /** Failure rate above which the breaker trips to OPEN. */
  failureThreshold: number;
  /** Minimum number of samples in the window before tripping is allowed. */
  minSamples: number;
  /** Time the breaker stays OPEN before allowing a HALF_OPEN probe. */
  cooldownMs: number;
}

export interface RateLimiterConfig {
  /** Nominal token refill rate (tokens per second) per gateway. */
  nominalRps: number;
  /** Burst capacity expressed as a multiple of the refill rate. */
  burstFactor: number;
  /** Rate limiter never drops below nominalRps * minRateFactor. */
  minRateFactor: number;
  /** Capacity regained per successful request while recovering (AIMD). */
  aiStepRps: number;
  /** How long a job waits in-slot for a token before bouncing to a retry. */
  tokenWaitMs: number;
}

export interface QueueConfig {
  /** Redis key prefix for all BullMQ keys (see ADR 0001: payments:{gateway}). */
  prefix: string;
  /** Default number of worker instances started per gateway. */
  workerPoolSize: number;
  /** Base delay for exponential backoff between retries. */
  backoffBaseMs: number;
  /** Jitter ratio applied on top of the exponential delay. */
  backoffJitter: number;
}

export interface IdempotencyConfig {
  /** Lease TTL while a payment is PROCESSING (crash-safe expiry). */
  leaseTtlSeconds: number;
  /** TTL of final COMPLETED/FAILED idempotency records (24h default). */
  retentionSeconds: number;
}

export interface MetricsConfig {
  windowSeconds: number;
  broadcastIntervalMs: number;
  /** Error-rate threshold that raises an alert (> value). */
  errorRateAlert: number;
  /** P95 latency threshold in ms that raises an alert. */
  p95LatencyAlertMs: number;
  /** Waiting queue depth threshold that raises an alert. */
  queueDepthAlert: number;
  /** Cooldown before the same alert condition can be raised again. */
  alertCooldownMs: number;
}

export interface TracingConfig {
  serviceName: string;
  /** OTLP HTTP endpoint; when unset an in-memory span exporter is used. */
  otlpEndpoint?: string;
}

export interface AppConfig {
  redis: { url: string };
  http: { port: number };
  queue: QueueConfig;
  idempotency: IdempotencyConfig;
  rateLimiter: RateLimiterConfig;
  circuitBreaker: CircuitBreakerConfig;
  metrics: MetricsConfig;
  tracing: TracingConfig;
  /** Names of gateway scenarios exposed by the mock gateway. */
  gateways: string[];
}

const DEFAULT_GATEWAYS = ['stripe', 'paypal', 'adyen'];

export const APP_CONFIG = Symbol('APP_CONFIG');

function num(env: NodeJS.ProcessEnv, key: string, fallback: number): number {
  const raw = env[key];
  if (raw === undefined || raw === '') return fallback;
  const value = Number(raw);
  return Number.isFinite(value) ? value : fallback;
}

function redisUrl(env: NodeJS.ProcessEnv): string {
  const explicit = env.REDIS_URL;
  if (explicit) return explicit;
  const urlFile = env.TEST_REDIS_URL_FILE;
  if (urlFile) {
    try {
      const fromFile = readFileSync(urlFile, 'utf8').trim();
      if (fromFile) return fromFile;
    } catch {
      // fall through to defaults
    }
  }
  const host = env.REDIS_HOST ?? '127.0.0.1';
  const port = env.REDIS_PORT ?? '6379';
  const db = env.REDIS_DB !== undefined ? `/${env.REDIS_DB}` : '';
  const auth = env.REDIS_PASSWORD ? `:${encodeURIComponent(env.REDIS_PASSWORD)}@` : '';
  return `redis://${auth}${host}:${port}${db}`;
}

export function buildConfig(env: NodeJS.ProcessEnv = process.env): AppConfig {
  const gateways = (env.GATEWAYS ?? DEFAULT_GATEWAYS.join(','))
    .split(',')
    .map((g) => g.trim())
    .filter(Boolean);
  if (gateways.length === 0) gateways.push(...DEFAULT_GATEWAYS);

  return {
    redis: { url: redisUrl(env) },
    http: { port: num(env, 'PORT', 3000) },
    queue: {
      prefix: env.QUEUE_PREFIX ?? 'bull:payments',
      workerPoolSize: num(env, 'QUEUE_WORKER_POOL_SIZE', 2),
      backoffBaseMs: num(env, 'QUEUE_BACKOFF_BASE_MS', 500),
      backoffJitter: num(env, 'QUEUE_BACKOFF_JITTER', 30) / 100,
    },
    idempotency: {
      leaseTtlSeconds: num(env, 'IDEMPOTENCY_LEASE_TTL_SECONDS', 600),
      retentionSeconds: num(env, 'IDEMPOTENCY_RETENTION_SECONDS', 86_400),
    },
    rateLimiter: {
      nominalRps: num(env, 'RATE_LIMIT_NOMINAL_RPS', 20),
      burstFactor: num(env, 'RATE_LIMIT_BURST_FACTOR', 2),
      minRateFactor: num(env, 'RATE_LIMIT_MIN_RATE_FACTOR', 0.25),
      aiStepRps: num(env, 'RATE_LIMIT_AI_STEP_RPS', 1),
      tokenWaitMs: num(env, 'RATE_LIMIT_TOKEN_WAIT_MS', 15_000),
    },
    circuitBreaker: {
      windowMs: num(env, 'CIRCUIT_BREAKER_WINDOW_MS', 30_000),
      failureThreshold: num(env, 'CIRCUIT_BREAKER_FAILURE_THRESHOLD', 0.5),
      minSamples: num(env, 'CIRCUIT_BREAKER_MIN_SAMPLES', 10),
      cooldownMs: num(env, 'CIRCUIT_BREAKER_COOLDOWN_MS', 30_000),
    },
    metrics: {
      windowSeconds: num(env, 'METRICS_WINDOW_SECONDS', 60),
      broadcastIntervalMs: num(env, 'METRICS_BROADCAST_INTERVAL_MS', 1000),
      errorRateAlert: num(env, 'METRICS_ERROR_RATE_ALERT', 0.1),
      p95LatencyAlertMs: num(env, 'METRICS_P95_LATENCY_ALERT_MS', 1000),
      queueDepthAlert: num(env, 'METRICS_QUEUE_DEPTH_ALERT', 200),
      alertCooldownMs: num(env, 'METRICS_ALERT_COOLDOWN_MS', 10_000),
    },
    tracing: {
      serviceName: env.OTEL_SERVICE_NAME ?? 'queue-system',
      otlpEndpoint: env.OTEL_EXPORTER_OTLP_ENDPOINT || undefined,
    },
    gateways,
  };
}

/** Build a config for tests: isolated Redis db URL override convenience + fast timings. */
export function buildConfigForTest(overrides: Partial<AppConfig> = {}): AppConfig {
  return {
    ...buildConfig({}),
    ...overrides,
    redis: { url: process.env.REDIS_URL ?? 'redis://127.0.0.1:6379' },
  };
}

import { INestApplication } from '@nestjs/common';
import { Test, TestingModule } from '@nestjs/testing';
import { IoAdapter } from '@nestjs/platform-socket.io';
import { Redis } from 'ioredis';
import { APP_CONFIG, AppConfig, buildConfigForTest } from '../../src/config/app-config.js';
import { AppModule } from '../../src/app.module.js';
import { PaymentStore } from '../../src/payments/payment-store.service.js';
import { PaymentRecord } from '../../src/domain/payment.js';

export type ConfigPatch = (config: AppConfig) => AppConfig;

export interface ProviderOverride {
  provide: unknown;
  useValue: unknown;
}

async function compileApp(
  patch?: ConfigPatch,
  overrides: ProviderOverride[] = [],
): Promise<TestingModule> {
  const config = buildConfigForTest();
  config.queue.prefix = `bull:test:${Math.random().toString(36).slice(2, 10)}`;
  const finalConfig = patch ? patch(config) : config;

  let builder = Test.createTestingModule({
    imports: [AppModule],
  })
    .overrideProvider(APP_CONFIG)
    .useValue(finalConfig);
  for (const override of overrides) {
    builder = builder.overrideProvider(override.provide as never).useValue(override.useValue);
  }
  return builder.compile();
}

/**
 * Compile a full application instance for integration tests. Redis must be
 * reachable (the vitest global setup provisions it). Every app gets an
 * isolated BullMQ key prefix so parallel test files never steal each other's
 * jobs; payment/audit keys are namespaced by unique payment ids in tests.
 * `overrides` swap providers (e.g. a failing SettlementLedger seam).
 */
export async function createTestApp(
  patch?: ConfigPatch,
  overrides: ProviderOverride[] = [],
): Promise<INestApplication> {
  const moduleFixture = await compileApp(patch, overrides);
  const app = moduleFixture.createNestApplication();
  await app.init();
  return app;
}

/** Like createTestApp but with the Socket.IO adapter installed (for WS specs). */
export async function createTestAppWithWebSocket(
  patch?: ConfigPatch,
  overrides: ProviderOverride[] = [],
): Promise<INestApplication> {
  const moduleFixture = await compileApp(patch, overrides);
  const app = moduleFixture.createNestApplication();
  app.useWebSocketAdapter(new IoAdapter(app));
  await app.init();
  return app;
}

export async function waitFor(
  assertion: () => Promise<boolean> | boolean,
  opts: { timeoutMs?: number; intervalMs?: number; label?: string } = {},
): Promise<void> {
  const timeoutMs = opts.timeoutMs ?? 15_000;
  const intervalMs = opts.intervalMs ?? 50;
  const deadline = Date.now() + timeoutMs;
  let lastError: unknown;
  while (Date.now() < deadline) {
    try {
      const ok = await assertion();
      if (ok) return;
    } catch (err) {
      lastError = err;
    }
    await new Promise((resolve) => setTimeout(resolve, intervalMs));
  }
  throw new Error(
    `waitFor timed out after ${timeoutMs}ms${opts.label ? ` (${opts.label})` : ''}${
      lastError ? ` — last error: ${lastError instanceof Error ? lastError.message : String(lastError)}` : ''
    }`,
  );
}

export const now = () => Date.now();

/** Read payment record directly using PaymentStore service without bypassing seam. */
export async function readPaymentDirect(redisUrl: string, id: string): Promise<PaymentRecord | null> {
  const redis = new Redis(redisUrl, { maxRetriesPerRequest: null });
  try {
    const store = new PaymentStore(redis);
    return await store.get(id);
  } finally {
    redis.disconnect();
  }
}

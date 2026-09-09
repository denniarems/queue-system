/**
 * Vitest global setup: guarantees a reachable Redis for the test suite.
 *
 * Resolution order:
 *  1. `REDIS_URL` env var — if set and reachable it is used as-is.
 *  2. A container started earlier by this setup (named `queue-system-test-redis`,
 *     published on 127.0.0.1:6397). If it is already running it is reused.
 *  3. Otherwise a throwaway Redis container is started with `docker run` and
 *     stopped again by the teardown returned from this module.
 *
 * The chosen URL is exposed to test workers through the `REDIS_URL` env var and
 * mirrored to a temp file (see `TEST_REDIS_URL_FILE`) so every worker process
 * resolves the same endpoint even when environment mutation does not propagate.
 */
import { spawn, spawnSync } from 'node:child_process';
import { existsSync, readFileSync, writeFileSync } from 'node:fs';
import { tmpdir } from 'node:os';
import { join } from 'node:path';
import { Redis } from 'ioredis';

const CONTAINER_NAME = 'queue-system-test-redis';
const PORT = '6397';
const URL_FILE = join(tmpdir(), 'queue-system-test-redis-url.txt');
const URL = `redis://127.0.0.1:${PORT}`;

async function ping(url: string, timeoutMs = 2000): Promise<boolean> {
  const client = new Redis(url, {
    lazyConnect: true,
    connectTimeout: timeoutMs,
    maxRetriesPerRequest: 1,
    retryStrategy: () => null,
  });
  client.on('error', () => {
    // probe clients intentionally fail while the container boots; swallow.
  });
  try {
    await client.connect();
    const pong = await client.ping();
    return pong === 'PONG';
  } catch {
    return false;
  } finally {
    client.disconnect();
  }
}

async function waitForPing(url: string, attempts = 60, intervalMs = 500): Promise<boolean> {
  for (let i = 0; i < attempts; i += 1) {
    if (await ping(url)) return true;
    await new Promise((resolve) => setTimeout(resolve, intervalMs));
  }
  return false;
}

export default async function setup(): Promise<() => Promise<void>> {
  const explicit = process.env.REDIS_URL;
  if (explicit) {
    if (!(await ping(explicit))) {
      throw new Error(
        `REDIS_URL=${explicit} is set but not reachable. Start Redis (docker compose up -d redis) or fix the URL.`,
      );
    }
    process.env.TEST_REDIS_URL_FILE = URL_FILE;
    writeFileSync(URL_FILE, explicit);
    return async () => {};
  }

  // Reuse a running container from a previous run when possible.
  if (await ping(URL)) {
    process.env.REDIS_URL = URL;
    process.env.TEST_REDIS_URL_FILE = URL_FILE;
    writeFileSync(URL_FILE, URL);
    return async () => {};
  }

  // Reuse an existing container (running or stopped), else start a fresh one.
  const inspect = spawnSync('docker', ['inspect', '-f', '{{.State.Status}}', CONTAINER_NAME], {
    encoding: 'utf8',
  });
  const status = inspect.stdout?.trim();
  if (status === 'exited') {
    await spawnSyncAsync('docker', ['start', CONTAINER_NAME]);
  } else if (!status) {
    const run = spawnSync(
      'docker',
      ['run', '-d', '--name', CONTAINER_NAME, '-p', `127.0.0.1:${PORT}:6379`, 'redis:7.4-alpine'],
      { encoding: 'utf8' },
    );
    if (run.status !== 0) {
      throw new Error(
        `Could not provision a Redis container for tests: ${run.stderr?.trim() ?? run.stdout?.trim()}. ` +
          'Start one manually (docker compose up -d redis) or set REDIS_URL.',
      );
    }
  }

  if (!(await waitForPing(URL))) {
    throw new Error('Test Redis container started but never became reachable.');
  }

  process.env.REDIS_URL = URL;
  process.env.TEST_REDIS_URL_FILE = URL_FILE;
  writeFileSync(URL_FILE, URL);

  return async () => {
    if (existsSync(URL_FILE)) {
      try {
        // Only tear down the container we started ourselves; leave user-provided instances alone.
        const startedByUs = readFileSync(URL_FILE, 'utf8').trim() === URL;
        if (startedByUs) {
          spawnSync('docker', ['stop', CONTAINER_NAME], { encoding: 'utf8' });
        }
      } catch {
        // best-effort teardown
      }
    }
  };
}

function spawnSyncAsync(cmd: string, args: string[]): Promise<void> {
  return new Promise((resolve, reject) => {
    const child = spawn(cmd, args, { stdio: 'ignore' });
    child.on('error', reject);
    child.on('exit', (code) => (code === 0 ? resolve() : reject(new Error(`${cmd} exited ${code}`))));
  });
}

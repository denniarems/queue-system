import { Inject, Injectable, Logger, OnApplicationShutdown } from '@nestjs/common';
import { Job, Queue, Worker } from 'bullmq';
import { Redis } from 'ioredis';
import { APP_CONFIG } from '../config/app-config.js';
import type { AppConfig } from '../config/app-config.js';
import { CorrelationService } from '../common/event-bus.js';
import {
  DLQ_JOB_NAME,
  DLQ_QUEUE_NAME,
  PaymentJobData,
  PaymentPriority,
  PRIORITY_TO_NUMERIC,
  QUEUE_JOB_NAME,
} from '../domain/payment.js';

interface GatewayRuntime {
  gatewayId: string;
  queueName: string;
  queue: Queue<PaymentJobData>;
  queueClient: Redis;
  workers: Array<{ worker: Worker<PaymentJobData>; client: Redis }>;
}

export interface EnqueuePaymentJobInput {
  gatewayId: string;
  paymentId: string;
  correlationId?: string;
  priority: PaymentPriority;
  maxRetries: number;
  delayMs?: number;
  jobId: string;
}

export interface DlqEntry {
  paymentId: string;
  correlationId?: string;
  reason: string;
  deadLetteredAt: string;
}

export interface QueueDepths {
  gatewayId: string;
  waiting: number;
  active: number;
  delayed: number;
  failed: number;
}

export type PaymentJobProcessor = (job: Job<PaymentJobData>) => Promise<unknown>;
export type PaymentJobFailureHandler = (jobData: PaymentJobData, error: Error) => Promise<void>;

/**
 * Owns the BullMQ lifecycle for the whole application (ADR 0001):
 *  - a dedicated queue per gateway (`bull:payments:{gatewayId}` key namespace),
 *  - dynamic worker pools that can scale per gateway,
 *  - a single terminal dead letter queue (`bull:payments:dlq`),
 *  - graceful shutdown that lets in-flight jobs finish.
 *
 * The queue module is intentionally free of payment-domain dependencies: the
 * processing function and the failure handler are registered by the payments
 * module at startup (`setProcessor` / `setFailureHandler`).
 */
@Injectable()
export class QueueManager implements OnApplicationShutdown {
  private readonly logger = new Logger(QueueManager.name);
  private readonly runtimes = new Map<string, GatewayRuntime>();
  private dlq?: Queue<DlqEntry>;
  private dlqClient?: Redis;
  private processor?: PaymentJobProcessor;
  private failureHandler?: PaymentJobFailureHandler;

  constructor(
    @Inject(APP_CONFIG) private readonly config: AppConfig,
    private readonly correlation: CorrelationService,
  ) {}

  setProcessor(processor: PaymentJobProcessor): void {
    this.processor = processor;
  }

  setFailureHandler(handler: PaymentJobFailureHandler): void {
    this.failureHandler = handler;
  }

  getGatewayIds(): string[] {
    return [...this.runtimes.keys()];
  }

  queueNameFor(gatewayId: string): string {
    return gatewayId
      .toLowerCase()
      .replace(/[^a-z0-9-]/g, '-')
      .slice(0, 60);
  }

  private queueOpts() {
    return { prefix: this.config.queue.prefix };
  }

  private newClient(): Redis {
    return new Redis(this.config.redis.url, { maxRetriesPerRequest: null });
  }

  /** Lazily provision the queue + worker pool for a gateway. */
  async ensureGateway(gatewayId: string): Promise<GatewayRuntime> {
    const existing = this.runtimes.get(gatewayId);
    if (existing) return existing;

    const queueName = this.queueNameFor(gatewayId);
    const queueClient = this.newClient();
    const queue = new Queue<PaymentJobData>(queueName, { ...this.queueOpts(), connection: queueClient });
    queue.on('error', (err) => this.logger.error(`queue ${gatewayId} error`, err.message));
    await queue.waitUntilReady();

    const runtime: GatewayRuntime = { gatewayId, queueName, queue, queueClient, workers: [] };
    this.runtimes.set(gatewayId, runtime);
    await this.scaleWorkers(runtime, this.config.queue.workerPoolSize);
    this.logger.log(`provisioned queue+worker pool for gateway ${gatewayId} (${queueName})`);
    return runtime;
  }

  private async scaleWorkers(runtime: GatewayRuntime, target: number): Promise<void> {
    while (runtime.workers.length < target) {
      const client = this.newClient();
      const worker = new Worker<PaymentJobData>(
        runtime.queueName,
        (job) =>
          this.correlation.enter(job.data.correlationId ?? job.data.paymentId, () =>
            this.processor ? this.processor(job) : Promise.reject(new Error('payment processor not registered')),
          ),
        { ...this.queueOpts(), connection: client, concurrency: 1 },
      );
      worker.on('error', (err) => this.logger.error(`worker ${runtime.gatewayId} error`, err.message));
      worker.on('failed', (job, err) => {
        // BullMQ emits 'failed' after EVERY failed attempt, not only when the
        // retry budget is spent (it retries internally with backoff). Only
        // route to the DLQ once attempts are truly exhausted.
        if (!job) return;
        const attempts = job.opts?.attempts ?? 1;
        const exhausted = job.attemptsMade >= attempts;
        if (exhausted && this.failureHandler) {
          void this.failureHandler(job.data, err).catch((e) =>
            this.logger.error(`failure handler failed for ${job.data.paymentId}`, e.message),
          );
        }
      });
      await worker.waitUntilReady();
      runtime.workers.push({ worker, client });
    }
    while (runtime.workers.length > target) {
      const extra = runtime.workers.pop();
      if (extra) {
        await extra.worker.close();
        extra.client.disconnect();
      }
    }
  }

  /** Pause queue processing for a specific gateway or all active gateways. */
  async pause(gatewayId?: string): Promise<void> {
    if (gatewayId) {
      const runtime = await this.ensureGateway(gatewayId);
      await runtime.queue.pause();
    } else {
      for (const runtime of this.runtimes.values()) {
        await runtime.queue.pause();
      }
    }
  }

  /** Resume queue processing for a specific gateway or all active gateways. */
  async resume(gatewayId?: string): Promise<void> {
    if (gatewayId) {
      const runtime = await this.ensureGateway(gatewayId);
      await runtime.queue.resume();
    } else {
      for (const runtime of this.runtimes.values()) {
        await runtime.queue.resume();
      }
    }
  }

  /** Enqueue a payment job with priority/delay semantics (ADR 0001). */
  async enqueue(input: EnqueuePaymentJobInput): Promise<void> {
    const runtime = await this.ensureGateway(input.gatewayId);
    const backoff = this.config.queue;
    await runtime.queue.add(
      QUEUE_JOB_NAME,
      { paymentId: input.paymentId, correlationId: input.correlationId },
      {
        jobId: input.jobId,
        priority: PRIORITY_TO_NUMERIC[input.priority],
        delay: input.delayMs ? Math.max(1, Math.round(input.delayMs)) : 0,
        attempts: input.maxRetries + 1,
        backoff: { type: 'exponential', delay: backoff.backoffBaseMs, jitter: backoff.backoffJitter },
        removeOnComplete: true,
      },
    );
  }

  /** Terminal queue for unrecoverable payments. */
  private async dlqQueue(): Promise<Queue<DlqEntry>> {
    if (this.dlq) return this.dlq;
    this.dlqClient = this.newClient();
    this.dlq = new Queue<DlqEntry>(DLQ_QUEUE_NAME, { ...this.queueOpts(), connection: this.dlqClient });
    this.dlq.on('error', (err) => this.logger.error(`dlq error`, err.message));
    await this.dlq.waitUntilReady();
    return this.dlq;
  }

  async routeToDlq(entry: DlqEntry): Promise<void> {
    const dlq = await this.dlqQueue();
    await dlq.add(DLQ_JOB_NAME, entry, { jobId: entry.paymentId, attempts: 1 });
  }

  async listDlq(): Promise<Array<DlqEntry & { jobId?: string }>> {
    const dlq = await this.dlqQueue();
    const jobs = await dlq.getJobs(['waiting', 'delayed', 'active', 'failed', 'completed'], 0, 200, false);
    return jobs
      .map((job) => ({ ...job.data, jobId: job.id }))
      .sort((a, b) => a.deadLetteredAt.localeCompare(b.deadLetteredAt));
  }

  /** Current waiting/active/delayed/failed depths of every gateway queue + DLQ. */
  async collectQueueDepths(): Promise<{ gateways: Array<QueueDepths & { queueName: string }>; dlq: QueueDepths }> {
    const gateways: Array<QueueDepths & { queueName: string }> = [];
    for (const runtime of this.runtimes.values()) {
      const counts = await runtime.queue.getJobCounts('waiting', 'prioritized', 'active', 'delayed', 'failed');
      gateways.push({
        gatewayId: runtime.gatewayId,
        queueName: runtime.queueName,
        // BullMQ stores jobs with a numeric priority in the 'prioritized' set;
        // both represent queued-but-not-active work for depth monitoring.
        waiting: (counts.waiting ?? 0) + (counts.prioritized ?? 0),
        active: counts.active ?? 0,
        delayed: counts.delayed ?? 0,
        failed: counts.failed ?? 0,
      });
    }
    const dlqQueue = await this.dlqQueue();
    const dlqCounts = await dlqQueue.getJobCounts('waiting', 'active', 'delayed', 'failed');
    return {
      gateways,
      dlq: {
        gatewayId: DLQ_QUEUE_NAME,
        waiting: dlqCounts.waiting ?? 0,
        active: dlqCounts.active ?? 0,
        delayed: dlqCounts.delayed ?? 0,
        failed: dlqCounts.failed ?? 0,
      },
    };
  }

  /** Dynamically scale the worker pool of a gateway (ticket 02). */
  async setWorkerPoolSize(gatewayId: string, size: number): Promise<void> {
    if (size < 0 || !Number.isInteger(size)) throw new Error('worker pool size must be a non-negative integer');
    const runtime = await this.ensureGateway(gatewayId);
    await this.scaleWorkers(runtime, size);
    this.logger.log(`gateway ${gatewayId} worker pool -> ${runtime.workers.length}`);
  }

  getWorkerPoolSize(gatewayId: string): number {
    return this.runtimes.get(gatewayId)?.workers.length ?? 0;
  }

  /** Graceful shutdown: finish in-flight jobs, then release queues and clients. */
  async onApplicationShutdown(): Promise<void> {
    const started = Date.now();
    const tasks: Array<Promise<void>> = [];
    for (const runtime of this.runtimes.values()) {
      tasks.push(...runtime.workers.map(({ worker }) => closeWorkerGracefully(worker)));
      tasks.push(runtime.queue.close());
    }
    if (this.dlq) tasks.push(this.dlq.close());
    await Promise.allSettled(tasks);
    for (const runtime of this.runtimes.values()) {
      runtime.queueClient.disconnect();
      for (const { client } of runtime.workers) client.disconnect();
    }
    this.dlqClient?.disconnect();
    this.logger.log(`queue manager shut down in ${Date.now() - started}ms`);
  }
}

/**
 * BullMQ's graceful close waits for in-flight jobs; force-close after a safety
 * budget so a wedged job cannot hang application shutdown forever.
 */
async function closeWorkerGracefully(worker: Worker): Promise<void> {
  const graceMs = 15_000;
  await new Promise<void>((resolve) => {
    const timer = setTimeout(() => {
      worker.close(true).then(resolve, resolve);
    }, graceMs);
    worker.close().then(
      () => {
        clearTimeout(timer);
        resolve();
      },
      () => {
        clearTimeout(timer);
        resolve();
      },
    );
  });
}

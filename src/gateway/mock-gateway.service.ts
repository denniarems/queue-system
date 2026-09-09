import { Inject, Injectable } from '@nestjs/common';
import { APP_CONFIG } from '../config/app-config.js';
import type { AppConfig } from '../config/app-config.js';
import { classifyHttpStatus } from '../domain/errors.js';
import type { Payment } from '../domain/payment.js';
import {
  GatewayCallContext,
  GatewayChargeResult,
  GatewayRefundResult,
  MockGatewayBehavior,
  MockStep,
  OK_BEHAVIOR,
  PaymentGateway,
} from './gateway.types.js';

export interface MockGatewayStats {
  charges: number;
  refunds: number;
  failedCharges: number;
  httpStatusHistogram: Record<number, number>;
  /** Correlation ids seen on outbound charge calls (propagation check). */
  correlationIds: string[];
}

/**
 * Deterministic stand-in for real payment providers. Behavior is fully
 * scriptable per gateway: latency ranges, sequences of HTTP responses
 * (429/5xx/permanent 4xx), outage windows — so tests can induce rate limits,
 * transient faults and permanent declines without external dependencies.
 */
export class MockGateway implements PaymentGateway {
  readonly id: string;
  private behavior: MockGatewayBehavior = structuredClone(OK_BEHAVIOR);
  private stepIndex = 0;
  private refundStepIndex = 0;
  private readonly transactions = new Map<string, 'succeeded' | 'failed'>();
  readonly stats: MockGatewayStats = {
    charges: 0,
    refunds: 0,
    failedCharges: 0,
    httpStatusHistogram: {},
    correlationIds: [],
  };

  private refundSteps: MockStep[] = [];

  constructor(id: string) {
    this.id = id;
  }

  configure(behavior: Partial<MockGatewayBehavior>): void {
    this.behavior = {
      latencyMinMs: behavior.latencyMinMs ?? 5,
      latencyMaxMs: behavior.latencyMaxMs ?? 20,
      steps: behavior.steps ?? [],
      after: behavior.after ?? OK_BEHAVIOR.after,
      refundSteps: behavior.refundSteps ?? [],
    };
    this.stepIndex = 0;
    this.refundStepIndex = 0;
    this.refundSteps = behavior.refundSteps ?? [];
  }

  /** Keep scripted failure steps but return to default success afterwards. */
  reset(): void {
    this.configure({ ...OK_BEHAVIOR, latencyMinMs: 5, latencyMaxMs: 20 });
  }

  async charge(payment: Payment, context?: GatewayCallContext): Promise<GatewayChargeResult> {
    const behavior = this.behavior;
    const step = this.nextStep(behavior);
    this.stats.charges += 1;
    if (context?.correlationId) this.stats.correlationIds.push(context.correlationId);
    const latencyMs = step.kind === 'ok' ? this.latency(behavior, step) : this.latency(behavior, undefined);
    await sleep(latencyMs);
    if (step.kind === 'fail') {
      this.stats.failedCharges += 1;
      this.tally(step.httpStatus);
      return {
        ok: false,
        latencyMs,
        failure: {
          code: step.code ?? classifyHttpStatus(step.httpStatus),
          message: `gateway ${this.id} rejected with HTTP ${step.httpStatus}`,
          httpStatus: step.httpStatus,
          retryable: step.retryable ?? (step.httpStatus === 429 || step.httpStatus >= 500),
        },
      };
    }
    this.tally(200);
    const transactionId = `${this.id}_txn_${Date.now().toString(36)}_${this.stats.charges}`;
    this.transactions.set(transactionId, 'succeeded');
    return { ok: true, transactionId, httpStatus: 200, latencyMs };
  }

  async refund(transactionId: string): Promise<GatewayRefundResult> {
    this.stats.refunds += 1;
    const step = this.nextRefundStep();
    const latencyMs = step ? this.latency(this.behavior, step) : this.latency(this.behavior, undefined);
    await sleep(latencyMs);
    if (step?.kind === 'fail') {
      this.tally(step.httpStatus);
      this.transactions.set(transactionId, 'failed');
      return {
        ok: false,
        latencyMs,
        failure: {
          code: step.code ?? classifyHttpStatus(step.httpStatus),
          message: `gateway ${this.id} refund rejected with HTTP ${step.httpStatus}`,
          httpStatus: step.httpStatus,
          retryable: step.retryable ?? false,
        },
      };
    }
    return { ok: true, refundId: `refund_${transactionId}_${this.stats.refunds}`, httpStatus: 200, latencyMs };
  }

  async getStatus(transactionId: string): Promise<{ transactionId: string; status: 'succeeded' | 'failed' }> {
    return { transactionId, status: this.transactions.get(transactionId) ?? 'failed' };
  }

  private nextRefundStep(): MockStep | undefined {
    if (this.refundStepIndex < this.refundSteps.length) {
      const step = this.refundSteps[this.refundStepIndex];
      this.refundStepIndex += 1;
      return step;
    }
    return undefined;
  }

  private nextStep(behavior: MockGatewayBehavior): MockStep {
    if (this.stepIndex < behavior.steps.length) {
      const step = behavior.steps[this.stepIndex];
      this.stepIndex += 1;
      return step;
    }
    return behavior.after;
  }

  private latency(behavior: MockGatewayBehavior, step?: MockStep): number {
    if (step?.kind === 'ok' && step.latencyMs !== undefined) return step.latencyMs;
    const min = behavior.latencyMinMs;
    const max = behavior.latencyMaxMs;
    if (max <= min) return min;
    return min + Math.random() * (max - min);
  }

  private tally(httpStatus: number): void {
    this.stats.httpStatusHistogram[httpStatus] = (this.stats.httpStatusHistogram[httpStatus] ?? 0) + 1;
  }
}

/**
 * Per-gateway registry of PaymentGateway instances. Registered gateways come
 * from configuration (`GATEWAYS` env var); unknown ids are provisioned lazily
 * so the queue manager can accept any gateway dynamically.
 */
@Injectable()
export class MockGatewayRegistry {
  private readonly instances = new Map<string, MockGateway>();

  constructor(@Inject(APP_CONFIG) private readonly config: AppConfig) {}

  get(gatewayId: string): MockGateway {
    let gateway = this.instances.get(gatewayId);
    if (!gateway) {
      gateway = new MockGateway(gatewayId);
      gateway.configure({ latencyMinMs: 5, latencyMaxMs: 20 });
      this.instances.set(gatewayId, gateway);
    }
    return gateway;
  }

  configure(gatewayId: string, behavior: Partial<MockGatewayBehavior>): void {
    this.get(gatewayId).configure(behavior);
  }

  reset(gatewayId: string): void {
    this.get(gatewayId).reset();
  }

  resetAll(): void {
    for (const gateway of this.instances.values()) gateway.reset();
  }
}

function sleep(ms: number): Promise<void> {
  return new Promise((resolve) => setTimeout(resolve, ms));
}

export type { PaymentGateway };

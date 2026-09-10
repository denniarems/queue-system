/**
 * Payment Gateway contract (the controlled seam of the whole system) plus the
 * deterministic mock implementation used for development and in tests.
 */
import type { Payment } from '../domain/payment.js';
import type { PaymentErrorCode } from '../domain/errors.js';

export interface GatewayFailure {
  code: PaymentErrorCode;
  message: string;
  httpStatus: number;
}

export type GatewayChargeResult =
  | { ok: true; transactionId: string; httpStatus: number; latencyMs: number }
  | { ok: false; failure: GatewayFailure; latencyMs: number };

export type GatewayRefundResult =
  | { ok: true; refundId: string; httpStatus: number; latencyMs: number }
  | { ok: false; failure: GatewayFailure; latencyMs: number };

export interface PaymentGateway {
  readonly id: string;
  /** Execute the gateway charge (saga step 2). */
  charge(payment: Payment, context?: GatewayCallContext): Promise<GatewayChargeResult>;
  /** Refund/void a previously successful charge (saga compensation). */
  refund(transactionId: string): Promise<GatewayRefundResult>;
  /** Poll the status of a previously issued transaction. */
  getStatus(transactionId: string): Promise<{ transactionId: string; status: 'succeeded' | 'failed' }>;
}

/** Metadata propagated with the outbound gateway call (correlation header). */
export interface GatewayCallContext {
  correlationId?: string;
}

/** Deterministic failure script consumed by the mock gateway, one entry per charge call. */
export type MockStep =
  | { kind: 'ok'; latencyMs?: number }
  | { kind: 'fail'; httpStatus: number; code?: PaymentErrorCode };

export interface MockGatewayBehavior {
  /** Random latency bounds in ms applied to successful calls. */
  latencyMinMs: number;
  latencyMaxMs: number;
  /** Queue of scripted responses; when exhausted `after` is used forever. */
  steps: MockStep[];
  after: MockStep;
  /** Optional scripted responses for refund calls (compensation paths). */
  refundSteps?: MockStep[];
}

export const OK_BEHAVIOR: MockGatewayBehavior = {
  latencyMinMs: 10,
  latencyMaxMs: 30,
  steps: [],
  after: { kind: 'ok' },
};

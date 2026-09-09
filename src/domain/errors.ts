/**
 * Error taxonomy for payment processing. Transient errors are retried by the
 * worker (BullMQ exponential backoff + jitter); permanent errors never retry
 * and route the payment to the Dead Letter Queue (ticket 05).
 */

export const ERROR_CODES = {
  CIRCUIT_OPEN: 'circuit_open',
  RATE_LIMITED: 'rate_limited',
  GATEWAY_UNAVAILABLE: 'gateway_unavailable',
  GATEWAY_TIMEOUT: 'gateway_timeout',
  SERVER_ERROR: 'server_error',
  NETWORK_ERROR: 'network_error',
  CARD_DECLINED: 'card_declined',
  INVALID_CARD: 'invalid_card',
  INSUFFICIENT_FUNDS: 'insufficient_funds',
  FRAUD: 'fraud',
  REFUND_FAILED: 'refund_failed',
  SETTLE_FAILED: 'settle_failed',
  RESERVE_FAILED: 'reserve_failed',
  UNKNOWN: 'unknown',
} as const;
export type PaymentErrorCode = (typeof ERROR_CODES)[keyof typeof ERROR_CODES];

export const RETRYABLE_CODES: ReadonlySet<PaymentErrorCode> = new Set([
  ERROR_CODES.CIRCUIT_OPEN,
  ERROR_CODES.RATE_LIMITED,
  ERROR_CODES.GATEWAY_UNAVAILABLE,
  ERROR_CODES.GATEWAY_TIMEOUT,
  ERROR_CODES.SERVER_ERROR,
  ERROR_CODES.NETWORK_ERROR,
]);

export class PaymentProcessingError extends Error {
  constructor(
    message: string,
    readonly code: PaymentErrorCode,
    readonly retryable: boolean,
    readonly httpStatus?: number,
    readonly gatewayId?: string,
  ) {
    super(message);
    this.name = 'PaymentProcessingError';
  }
}

export function isTransient(code: PaymentErrorCode): boolean {
  return RETRYABLE_CODES.has(code);
}

export function classifyHttpStatus(httpStatus: number): PaymentErrorCode {
  if (httpStatus === 429) return ERROR_CODES.RATE_LIMITED;
  if (httpStatus === 503 || httpStatus === 502) return ERROR_CODES.GATEWAY_UNAVAILABLE;
  if (httpStatus >= 500) return ERROR_CODES.SERVER_ERROR;
  if (httpStatus === 400) return ERROR_CODES.CARD_DECLINED;
  if (httpStatus === 402) return ERROR_CODES.INSUFFICIENT_FUNDS;
  if (httpStatus === 401) return ERROR_CODES.FRAUD;
  return ERROR_CODES.UNKNOWN;
}

export function toPaymentProcessingError(
  raw: unknown,
  gatewayId: string,
  fallbackCode: PaymentErrorCode = ERROR_CODES.UNKNOWN,
): PaymentProcessingError {
  if (raw instanceof PaymentProcessingError) return raw;
  const code = fallbackCode;
  return new PaymentProcessingError(
    raw instanceof Error ? raw.message : String(raw),
    code,
    isTransient(code),
    undefined,
    gatewayId,
  );
}

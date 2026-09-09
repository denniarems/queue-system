import { Logger } from '@nestjs/common';
import type { CorrelationService } from './event-bus.js';

type CorrelationReader = Pick<CorrelationService, 'current'>;

/**
 * Logger that binds the ambient correlation id to every emitted line. Call it
 * from code running inside a CorrelationService context (HTTP middleware or
 * the queue worker wrapper), so distributed traces are greppable in logs.
 * Constructed via a factory in CommonModule to avoid a module cycle.
 */
export class TraceLogger {
  constructor(private readonly correlation: CorrelationReader) {}

  log(message: string, context?: string): void {
    Logger.log(this.format(message), context);
  }

  warn(message: string, context?: string): void {
    Logger.warn(this.format(message), context);
  }

  error(message: string, context?: string): void {
    Logger.error(this.format(message), context);
  }

  private format(message: string): string {
    const correlationId = this.correlation.current();
    return correlationId ? `[corr:${correlationId}] ${message}` : message;
  }
}

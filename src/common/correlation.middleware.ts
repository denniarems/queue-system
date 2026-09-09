import { Injectable, NestMiddleware } from '@nestjs/common';
import { NextFunction, Request, Response } from 'express';
import { CorrelationService, newCorrelationId } from './event-bus.js';

/**
 * Binds every HTTP request to a correlation id (header `X-Correlation-Id` or a
 * generated one), echoes it back on the response and propagates it through the
 * async context so services, queue jobs, spans and audit records stay linked.
 */
@Injectable()
export class CorrelationMiddleware implements NestMiddleware {
  constructor(private readonly correlation: CorrelationService) {}

  use(req: Request, res: Response, next: NextFunction): void {
    const incoming = req.headers['x-correlation-id'];
    const correlationId = typeof incoming === 'string' && incoming.length > 0 ? incoming : newCorrelationId();
    res.setHeader('x-correlation-id', correlationId);
    this.correlation.enter(correlationId, () => next());
  }
}

import { Inject, Injectable } from '@nestjs/common';
import { context, Span, SpanStatusCode, trace, Tracer } from '@opentelemetry/api';
import { AsyncLocalStorageContextManager } from '@opentelemetry/context-async-hooks';
import { BasicTracerProvider, InMemorySpanExporter, SimpleSpanProcessor, SpanExporter } from '@opentelemetry/sdk-trace-base';
import { OTLPTraceExporter } from '@opentelemetry/exporter-trace-otlp-http';
import { APP_CONFIG } from '../config/app-config.js';
import type { AppConfig } from '../config/app-config.js';

export const SPAN_NAMES = {
  PROCESS: 'payment.process',
  ENQUEUE: 'payment.enqueue',
  RESERVE: 'payment.saga.reserve',
  CHARGE: 'payment.saga.charge',
  SETTLE: 'payment.saga.settle',
  COMPENSATION: 'payment.saga.compensation',
  DLQ: 'payment.dead_letter',
} as const;

export type SpanAttributes = Record<string, string | number | boolean | undefined>;

interface OtelRuntime {
  tracer: Tracer;
  exporter?: InMemorySpanExporter;
}

let runtime: OtelRuntime | undefined;

/** Register the OTel SDK exactly once per process (tests boot many apps). */
function ensureRuntime(serviceName: string, otlpEndpoint?: string): OtelRuntime {
  if (runtime) return runtime;
  context.setGlobalContextManager(new AsyncLocalStorageContextManager());
  let exporter: SpanExporter;
  let inMemory: InMemorySpanExporter | undefined;
  if (otlpEndpoint) {
    exporter = new OTLPTraceExporter({ url: otlpEndpoint });
  } else {
    inMemory = new InMemorySpanExporter();
    exporter = inMemory;
  }
  const provider = new BasicTracerProvider({ spanProcessors: [new SimpleSpanProcessor(exporter)] });
  trace.setGlobalTracerProvider(provider);
  runtime = { tracer: trace.getTracer(serviceName), exporter: inMemory };
  return runtime;
}

/**
 * Lightweight OpenTelemetry wrapper. The SDK registers once per process:
 * spans flow to an OTLP HTTP exporter when `OTEL_EXPORTER_OTLP_ENDPOINT` is
 * configured, otherwise they land in an in-memory exporter (used by the test
 * suite to assert the produced trace structure). The async-hooks context
 * manager propagates parent/child spans across promise boundaries.
 */
@Injectable()
export class TracingService {
  private readonly otel: OtelRuntime;

  constructor(@Inject(APP_CONFIG) config: AppConfig) {
    this.otel = ensureRuntime(config.tracing.serviceName, config.tracing.otlpEndpoint);
  }

  /** Run `fn` inside a child span with the given attributes. */
  async withSpan<T>(
    name: string,
    attributes: SpanAttributes,
    fn: (span: Span) => Promise<T>,
  ): Promise<T> {
    const activeContext = context.active();
    const span = this.otel.tracer.startSpan(name, { attributes: cleanAttributes(attributes) }, activeContext);
    const spanContext = trace.setSpan(activeContext, span);
    try {
      const result = await context.with(spanContext, () => fn(span));
      span.setStatus({ code: SpanStatusCode.OK });
      return result;
    } catch (err) {
      span.recordException(err instanceof Error ? err : new Error(String(err)));
      span.setStatus({ code: SpanStatusCode.ERROR });
      throw err;
    } finally {
      span.end();
    }
  }

  /** Finished spans recorded by the in-memory exporter (tests/observability). */
  listSpans(): Array<{ name: string; attributes: Record<string, unknown>; status: string }> {
    if (!this.otel.exporter) return [];
    return this.otel.exporter.getFinishedSpans().map((span) => ({
      name: span.name,
      attributes: { ...span.attributes },
      status: String(span.status.code),
    }));
  }
}

function cleanAttributes(attributes: SpanAttributes): Record<string, string | number | boolean> {
  const clean: Record<string, string | number | boolean> = {};
  for (const [key, value] of Object.entries(attributes)) {
    if (value !== undefined) clean[key] = value;
  }
  return clean;
}

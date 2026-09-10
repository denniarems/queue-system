import { Logger } from '@nestjs/common';
import { NestFactory } from '@nestjs/core';
import { IoAdapter } from '@nestjs/platform-socket.io';
import { DocumentBuilder, SwaggerModule } from '@nestjs/swagger';
import { AppModule } from './app.module.js';

async function bootstrap() {
  const app = await NestFactory.create(AppModule);
  app.useWebSocketAdapter(new IoAdapter(app));
  app.enableShutdownHooks();

  const config = new DocumentBuilder()
    .setTitle('Distributed Payment Processing Queue System')
    .setDescription(
      'Production-grade distributed payment processing system built with NestJS, BullMQ, Redis, and WebSocket telemetry. ' +
        'Features atomic idempotency, 3-tier priority queues, scheduled jobs, multi-gateway routing, circuit breaking, ' +
        '4-phase forward-and-reverse Saga coordination, and dead-letter queue (DLQ) inspection.',
    )
    .setVersion('1.0.0')
    .addTag('Payments', 'Ingestion, idempotent replay, scheduling, and lifecycle queries')
    .addTag('Queues', 'Queue management and Dead Letter Queue (DLQ) inspection')
    .addTag('Metrics', 'Operational telemetry, throughput, percentiles, and alerts')
    .addTag('Health', 'Health checks and service status')
    .build();

  const document = SwaggerModule.createDocument(app, config);
  SwaggerModule.setup('docs', app, document, {
    customSiteTitle: 'Payment Queue System API Docs',
    jsonDocumentUrl: 'docs-json',
  });

  const port = process.env.PORT ?? 3000;
  await app.listen(port);
  Logger.log(`queue-system listening on http://localhost:${port}`, 'bootstrap');
  Logger.log(`Swagger UI available at http://localhost:${port}/docs`, 'bootstrap');
}

await bootstrap();

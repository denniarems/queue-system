import { MiddlewareConsumer, Module, NestModule } from '@nestjs/common';
import { AppController } from './app.controller.js';
import { AppService } from './app.service.js';
import { ConfigModule } from './config/config.module.js';
import { RedisModule } from './redis/redis.module.js';
import { PaymentsModule } from './payments/payments.module.js';
import { GatewayModule } from './gateway/gateway.module.js';
import { QueueModule } from './queue/queue.module.js';
import { MetricsModule } from './metrics/metrics.module.js';
import { TracingModule } from './tracing/tracing.module.js';
import { CorrelationMiddleware } from './common/correlation.middleware.js';

@Module({
  imports: [ConfigModule, RedisModule, GatewayModule, QueueModule, PaymentsModule, MetricsModule, TracingModule],
  controllers: [AppController],
  providers: [AppService, CorrelationMiddleware],
})
export class AppModule implements NestModule {
  configure(consumer: MiddlewareConsumer): void {
    consumer.apply(CorrelationMiddleware).forRoutes('*');
  }
}

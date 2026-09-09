import { Controller, Get, Module } from '@nestjs/common';
import { QueueModule } from '../queue/queue.module.js';
import { MetricsCollector } from './metrics-collector.service.js';
import { MetricsGateway } from './metrics.gateway.js';

@Controller('queues')
export class MetricsController {
  constructor(private readonly collector: MetricsCollector) {}

  /** Live operational snapshot: TPS, error rate, P95/P99, queue depths, alerts. */
  @Get('metrics')
  async metrics() {
    return this.collector.snapshot();
  }
}

@Module({
  imports: [QueueModule],
  controllers: [MetricsController],
  providers: [MetricsCollector, MetricsGateway],
  exports: [MetricsCollector],
})
export class MetricsModule {}

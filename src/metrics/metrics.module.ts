import { Controller, Get, Module } from '@nestjs/common';
import { ApiOperation, ApiResponse, ApiTags } from '@nestjs/swagger';
import { MetricsSnapshotResponseDto } from '../payments/dto/payment-response.dto.js';
import { QueueModule } from '../queue/queue.module.js';
import { MetricsCollector } from './metrics-collector.service.js';
import { MetricsGateway } from './metrics.gateway.js';

@ApiTags('Metrics')
@Controller('queues')
export class MetricsController {
  constructor(private readonly collector: MetricsCollector) {}

  /** Live operational snapshot: TPS, error rate, P95/P99, queue depths, alerts. */
  @Get('metrics')
  @ApiOperation({
    summary: 'Get live operational metrics snapshot',
    description:
      'Returns real-time operational telemetry including TPS, error rate, P95/P99 latencies, gateway queue depths, and threshold alerts.',
  })
  @ApiResponse({
    status: 200,
    description: 'Live metrics snapshot',
    type: MetricsSnapshotResponseDto,
  })
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

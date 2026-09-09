import { Module, OnModuleInit } from '@nestjs/common';
import { GatewayModule } from '../gateway/gateway.module.js';
import { QueueModule } from '../queue/queue.module.js';
import { QueueManager } from '../queue/queue-manager.service.js';
import { AuditLogService } from './audit-log.service.js';
import { IdempotencyService } from './idempotency.service.js';
import { PaymentProcessor } from './payment.processor.js';
import { PaymentSagaService } from './payment-saga.service.js';
import { PaymentService } from './payment.service.js';
import { PaymentStore } from './payment-store.service.js';
import { PaymentsController, QueuesController } from './payments.controller.js';
import { SettlementLedger } from './settlement-ledger.service.js';

@Module({
  imports: [QueueModule, GatewayModule],
  controllers: [PaymentsController, QueuesController],
  providers: [
    PaymentService,
    PaymentStore,
    IdempotencyService,
    AuditLogService,
    SettlementLedger,
    PaymentSagaService,
    PaymentProcessor,
  ],
  exports: [PaymentService, PaymentStore, IdempotencyService, AuditLogService, PaymentProcessor],
})
export class PaymentsModule implements OnModuleInit {
  constructor(
    private readonly queueManager: QueueManager,
    private readonly processor: PaymentProcessor,
  ) {}

  /** Bind the payment domain logic to the queue workers before traffic starts. */
  onModuleInit(): void {
    this.queueManager.setProcessor((job) => this.processor.process(job));
    this.queueManager.setFailureHandler((jobData, error) => this.processor.handleJobFailure(jobData, error));
  }
}

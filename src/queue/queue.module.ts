import { Module } from '@nestjs/common';
import { CommonModule } from '../common/event-bus.js';
import { QueueManager } from './queue-manager.service.js';

@Module({
  imports: [CommonModule],
  providers: [QueueManager],
  exports: [QueueManager],
})
export class QueueModule {}

import { Global, Module, OnApplicationShutdown, Inject } from '@nestjs/common';
import { Redis } from 'ioredis';
import { APP_CONFIG } from '../config/app-config.js';
import type { AppConfig } from '../config/app-config.js';

export const REDIS_CLIENT = Symbol('REDIS_CLIENT');

/**
 * Shared application Redis client. BullMQ keeps its own dedicated clients
 * (see src/queue/bull-connection.ts); this client is used for the payment
 * store, the idempotency records and the audit log.
 */
@Global()
@Module({
  providers: [
    {
      provide: REDIS_CLIENT,
      inject: [APP_CONFIG],
      useFactory: (config: AppConfig) => new Redis(config.redis.url, { maxRetriesPerRequest: null }),
    },
    {
      provide: 'RedisLifecycle',
      useFactory: (client: Redis) => new RedisLifecycle(client),
      inject: [REDIS_CLIENT],
    },
  ],
  exports: [REDIS_CLIENT],
})
export class RedisModule {}

export class RedisLifecycle implements OnApplicationShutdown {
  constructor(@Inject(REDIS_CLIENT) private readonly client: Redis) {}

  async onApplicationShutdown(): Promise<void> {
    this.client.disconnect();
  }
}

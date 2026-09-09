# Senior Problem 1: Distributed Payment Processing Queue System

## Duration: 90 minutes (60 mins coding + 30 mins architecture)

## Problem Statement
Design and implement a production-ready payment processing queue system handling high-volume transactions with fault tolerance, monitoring, and real-time updates.

## Context
Your system processes 50K+ payments/hour from multiple sources. It must handle failures gracefully, provide real-time visibility, and scale horizontally.

## Part 1: Implementation (60 minutes)

### Existing Infrastructure
```typescript
// Available tools
import { Queue, Worker, QueueScheduler } from 'bullmq';
import { Injectable } from '@nestjs/common';
import { EventEmitter2 } from '@nestjs/event-emitter';
import { Socket } from 'socket.io';

// Database schemas
interface Payment {
  id: string;
  amount: number;
  currency: string;
  customerId: string;
  gatewayId: string;
  priority: 'high' | 'normal' | 'low';
  retryCount: number;
  maxRetries: number;
  status: PaymentStatus;
  metadata: Record<string, any>;
  createdAt: Date;
  processedAt?: Date;
  failureReason?: string;
}

enum PaymentStatus {
  QUEUED = 'queued',
  PROCESSING = 'processing',
  COMPLETED = 'completed',
  FAILED = 'failed',
  DEAD_LETTER = 'dead_letter'
}

// External gateway interface (mocked)
interface PaymentGateway {
  process(payment: Payment): Promise<GatewayResponse>;
  getStatus(transactionId: string): Promise<GatewayStatus>;
}
```

### Requirements

#### A. Queue Architecture (20 mins)
1. Implement multi-tier queue system:
   - Priority queues (high/normal/low)
   - Separate queues per gateway
   - Dead letter queue for failed items
   - Scheduled/delayed payment queue
2. Create QueueManager service:
   - Dynamic queue creation
   - Queue metrics collection
   - Graceful shutdown handling
   - Worker pool management
3. Implement rate limiting:
   - Per-gateway rate limits
   - Adaptive rate limiting based on failures
   - Token bucket algorithm

#### B. Processing Logic (20 mins)
1. Build PaymentProcessor:
   - Idempotency checks (using Redis)
   - Exponential backoff with jitter
   - Circuit breaker per gateway
   - Concurrent processing limits
2. Error handling strategies:
   - Transient vs permanent failures
   - Partial failure recovery
   - Compensation transactions
   - Audit trail for all operations
3. Implement saga pattern:
   - Multi-step payment processing
   - Rollback mechanisms
   - State persistence

#### C. Monitoring & Observability (20 mins)
1. Create MetricsCollector:
   - Queue depth and latency
   - Processing rate (TPS)
   - Success/failure rates
   - P95/P99 latencies
2. Real-time dashboard data:
   - WebSocket updates for queue status
   - Payment flow visualization
   - Alert thresholds and triggers
   - Historical trend analysis
3. Distributed tracing:
   - Correlation IDs across services
   - Span creation for each step
   - Integration with OpenTelemetry

## Part 2: Architecture Discussion (30 minutes)

### System Design Topics
1. **Scaling Strategy**
   - How would you scale to 500K payments/hour?
   - Database sharding approach
   - Queue partitioning strategy
   - Auto-scaling triggers

2. **Reliability & Resilience**
   - Multi-region deployment
   - Disaster recovery plan
   - Data consistency guarantees
   - Handling network partitions

3. **Performance Optimization**
   - Batch processing strategies
   - Caching layer design
   - Database query optimization
   - Message compression

4. **Security Considerations**
   - PCI compliance requirements
   - Encryption at rest and in transit
   - Audit logging requirements
   - Access control and isolation

5. **Testing Strategy**
   - Chaos engineering approach
   - Load testing methodology
   - Contract testing with gateways
   - Monitoring test coverage

### Code Review Questions
- Explain your choice of data structures
- Discuss trade-offs in your implementation
- How would you handle a gateway outage?
- Describe your debugging approach for production issues

## Evaluation Criteria
- **Architecture (30%)**
  - System design clarity
  - Scalability considerations
  - Fault tolerance patterns

- **Implementation (25%)**
  - Code quality and organization
  - Error handling completeness
  - Performance awareness

- **Production Readiness (20%)**
  - Monitoring/observability
  - Testing approach
  - Deployment considerations

- **Problem Solving (15%)**
  - Handling edge cases
  - Creative solutions
  - Trade-off analysis

- **Communication (10%)**
  - Clear explanation of choices
  - Asking clarifying questions
  - Documentation quality

## Expected Discussion Points
- Why BullMQ vs other queue systems?
- How to prevent duplicate processing?
- Database vs Redis for state storage
- Microservices vs monolithic approach
- Event sourcing considerations

## AI Usage Notes
- Evaluate ability to adapt AI suggestions to specific requirements
- Look for understanding beyond boilerplate code
- Check if they question/improve AI-generated patterns
- Assess integration of multiple AI suggestions into cohesive solution

# Hydra - Actor System Design

## Overview
Hydra is a distributed actor system built on NATS JetStream, designed for scalability and resilience. The system provides virtual actor capabilities with automatic actor placement, resurrection, and state management.

## Core Components

### Actor Model
- Each actor has:
  - Unique ID and type
  - Isolated message channel
  - Custom message handler
  - Transport for communication
  - State manager for persistence
  - Error handler for message processing failures
- Message processing is sequential and isolated
- Actors communicate only through messages
- Actor lifecycle is managed by the system

### Actor System
- Centralized entry point for actor management
- Handles actor registration, creation, and discovery
- Manages actor resurrection through queued processing
- Provides actor type registration with custom configurations
- Maintains active actor directory for local lookups

### State Management
Three-tier approach for robustness:

1. **Registration (Persistent)**
   - Stored in durable KV store (ActorKV)
   - Contains actor metadata
   - Used for actor discovery
   - Key format: `{region}/{bucket}/{type}/{id}`

2. **Liveness (Ephemeral)**
   - Separate KV store with TTL
   - Heartbeat-based health tracking
   - Automatic cleanup of dead actors
   - TTL = HeartbeatInterval * MissedThreshold

3. **Actor State (Persistent)**
   - Stored in ActorKV
   - Independent of bucket assignment
   - Key format: `{type}/{id}/state`
   - Raw byte storage (no format assumptions)
   - Actor controls serialization format
   - Survives actor resurrection and bucket rebalancing
   - Direct access without bucket lookup

### Transport Layer
- Message routing via NATS JetStream
- Consumer setup per actor
- Actor mailbox monitoring
- Automatic message replay for resurrection
- Heartbeat tracking for liveness

### Control Plane

#### Membership Management
- Tracks system nodes across regions
- Heartbeat-based health monitoring
- Propagates membership changes
- Enables dynamic scaling
- Self-healing membership detection

#### Bucket Management
- Shards actors using consistent hashing
- Distributes load across nodes
- Rebalances on membership changes
- Ensures fair bucket distribution
- Handles bucket ownership transitions

#### Death Detection & Resurrection
Primary: KV Watch
- Immediate detection through liveness expiry
- Watches KV delete/expire events
- Real-time actor death notification

Backup: Safety Check
- Periodic full scan (10s interval)
- Catches missed KV updates
- Ensures consistency

Resurrection Process:
- Mailbox monitoring for pending messages
- Concurrent resurrection with controlled parallelism
- State recovery from persistent storage
- Automatic resumption of message processing

### Telemetry & Observability
- OpenTelemetry integration
- Contextual logging with structured data
- Performance metrics collection
- Distributed tracing support
- Debug-level visibility into system operations

## Key Design Decisions

1. **Separation of Concerns**
   - Registration separate from liveness
   - Clear failure detection boundaries
   - Independent scaling of components

2. **TTL-based Liveness**
   - Self-cleaning through TTL
   - No explicit deregistration needed
   - Fast failure detection

3. **Bucket-based Sharding**
   - Deterministic actor placement
   - Efficient scaling and rebalancing
   - Region-aware distribution
   - Stabilization windows for rebalancing

4. **State Management Strategy**
   - State storage independent of bucket assignment
   - Raw byte storage without format restrictions
   - Actor controls own serialization format (JSON, Protobuf, custom, etc.)
   - Enables clean actor resurrection
   - Simplifies bucket rebalancing
   - Direct state access pattern
   - State persistence survives actor migrations

5. **KV Store Separation**
   - ActorKV: Registration and state (permanent)
   - LivenessKV: Health tracking (TTL-based)
   - Clear separation of concerns
   - Self-cleaning liveness tracking
   - Simplified backup/restore

6. **Concurrency Control**
   - Message processing queue per actor
   - Controlled resurrection concurrency
   - Task-based work distribution
   - Wait groups for clean shutdown
   - Mutex protection for shared resources

7. **Cluster Resilience**
   - Actor migration during node failures
   - Message retention during downtime
   - State preservation across restarts
   - Automatic actor resurrection
   - Split-brain detection and mitigation
   - Protection against duplicate actor creation with liveness checks
   - Transport layer verification ensures actors aren't active elsewhere before creation

## Current Limitations and Challenges

1. **Message Delivery Guarantees**
   - At-least-once delivery semantics
   - Message loss possible during prolonged cluster partitions
   - No transactional processing

2. **State Consistency**
   - No distributed transactions
   - Local state may diverge temporarily from stored state
   - Last-write-wins semantics for conflicts

## Future Considerations
1. Exactly-once message delivery semantics
2. Cross-region communication optimization
3. Custom shard allocation strategies
4. Transactional state updates
5. Actor versioning and migration
6. Client-side caching and batching
7. Backpressure mechanisms
8. Resource isolation and quotas

_Note: This is a living document that will evolve with the system._
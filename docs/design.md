# Hydra - Actor System Design

## Overview
Hydra is a distributed actor system built on NATS JetStream, designed for scalability and resilience.

## Core Components

### Actor Model
- Each actor has:
  - Unique ID and type
  - Isolated message channel
  - Custom message handler
  - Transport for communication
- Message processing is sequential and isolated
- Actors communicate only through messages

### State Management
Two-tier approach for robustness:

1. **Registration (Persistent)**
   - Stored in durable KV store
   - Contains actor metadata
   - Used for actor discovery
   - Key format: `{bucket}/{type}/{id}`

2. **Liveness (Ephemeral)**
   - Separate KV store with TTL
   - Heartbeat-based health tracking
   - Automatic cleanup of dead actors
   - TTL = HeartbeatInterval * MissedThreshold

### Control Plane

#### Membership Management
- Tracks system nodes across regions
- Heartbeat-based health monitoring
- Propagates membership changes
- Enables dynamic scaling

#### Bucket Management
- Shards actors using consistent hashing
- Distributes load across nodes
- Rebalances on membership changes
- Ensures fair bucket distribution

#### Death Detection
Primary: KV Watch
- Immediate detection through liveness expiry
- Watches KV delete/expire events
- Real-time actor death notification

Backup: Safety Check
- Periodic full scan (10s interval)
- Catches missed KV updates
- Ensures consistency

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

4. **Safety Mechanisms**
   - Buffered channels for async operations
   - Backup health checks
   - Graceful shutdown handling

## Future Considerations
1. Actor state persistence
2. Message delivery guarantees
3. Cross-region communication
4. Custom shard allocation strategies

_Note: This is a living document that will evolve with the system._
# Hydra Architecture

## Actor system

The actor system is the core component that manages actor lifecycle, messaging, and coordination across nodes. It is responsible for:

- Initializing and managing NATS JetStream connections for messaging and state storage
- Setting up telemetry (logging, tracing) infrastructure
- Managing the control plane for cluster coordination
- Creating and managing actors through the actor factory
- Handling graceful shutdown of components

Key components:

- NATS JetStream stream for actor messaging
- NATS KeyValue store for actor state persistence
- Control plane for cluster membership and bucket ownership
- Telemetry setup for observability
- Actor factory for creating new actors

Configuration options include:

- System ID and region
- NATS connection settings
- Message stream configuration
- Actor state store configuration 
- Control plane settings
- Retry intervals

The actor system provides a clean abstraction over the distributed infrastructure, allowing actors to communicate transparently without knowledge of physical locations.


## Actor

The actor is the core component that represents an entity in the system. It is responsible for:

- Receiving messages from the transport
- Processing messages using the handler
- Sending messages to the transport
- Maintaining its own state

Key components:

- Transport for sending and receiving messages
- Handler for processing messages
- State for maintaining actor state

## Control plane

The control plane is responsible for:

### Membership

The membership is the component that manages the membership of the cluster. It is responsible for:

- Maintaining the membership of the cluster
- Handling node joins and leaves
- Handling node failures

### Bucket ownership

The bucket ownership is the component that manages the ownership of buckets across nodes. It is responsible for:

- Maintaining the ownership of buckets across nodes
- Handling bucket splits and merges
- Handling bucket rebalancing

## Telemetry

The telemetry is the component that provides observability for the system. It is responsible for:

- Setting up logging and tracing infrastructure using OTel
- Collecting and reporting metrics
# Roadmap

## Local k8s setup

- [ ] Containerize the system
- [ ] Setup local distributed actor system cluster with kind
- [ ] Create Helm chart for the system

## Observability

- [x] Setup logger
- [x] Setup tracer
- [ ] Define metrics
    - [ ] Define metrics for actor system
    - [ ] Define metrics for actor
    - [ ] Define metrics for bucket ownership
    - [ ] Define metrics for bucket manager
    - [ ] Define metrics for membership
    - [ ] Define metrics for actor activation/deactivation/idling
- [ ] Observability for all actor location in a single pane of glass


## Actor

- [ ] Idle actor graceful shutdown with TTL
- [ ] Add example of message processing with protobuf

## Actor system

- [ ] Add support for non-persistent actor
- [ ] Distributed choas test for actor system cluster

## Bucket ownership

- [ ] Adding bucket splitting/merging for better load balancing
- [ ] Hot bucket detection and rebalancing
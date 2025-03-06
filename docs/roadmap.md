# Roadmap

## Local k8s setup

- [] Containerize the system
- [] Setup local distributed actor system cluster with kind

## Observability

- [] Add tracing
- [] Define metrics
    - [] Define metrics for actor system
    - [] Define metrics for actor
    - [] Define metrics for bucket ownership
    - [] Define metrics for bucket manager
    - [] Define metrics for membership
    - [] Define metrics for actor activation/deactivation/idling
- [] Observability for all actor location in a single pane of glass

## Bucket ownership

- [] Adding bucket splitting/merging for better load balancing
- [] Implementing hot bucket detection and rebalancing
- [] Adding configurable bucket counts per node based on capacity
![Build Status](https://github.com/pragyandas/hydra/actions/workflows/build.yml/badge.svg)

<img width="803" height="356" alt="hydra logo" src="https://github.com/user-attachments/assets/623dd1a4-a042-40be-a934-cce986f61d85" />

# Hydra
An actor framework for Go - built on NATS ⚡

Hydra is an opinionated, highly configurable, massively scalable actor framework for Go to build resilient applications with location-transparent messaging, automatic scaling, and fault tolerance.

## Features
- Transparent addressing
- Virtual actors
- Seamless scalability
- Robust resilience
- Highly configurable
- Automatic shutdown and resurrection of actors
- Built-in load balancing
- Built-in telemetry

## Testing

```
go test -v ./test/e2e/...
```

## Configuration

Check `Config` type in `actorsystem/types.go` for all possible configurations, and the carefully chosen defaults.

**Please note:** This is the system-level configuration. Actor-level configuration can be overridden during actor registration. See `ActorTypeConfig` in `actor/types.go`

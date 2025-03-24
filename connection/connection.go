package connection

import (
	"context"
	"fmt"

	"github.com/nats-io/nats.go"
	"github.com/nats-io/nats.go/jetstream"
	"github.com/pragyandas/hydra/telemetry"
	"go.uber.org/zap"
)

type Config struct {
	MessageStreamConfig   jetstream.StreamConfig
	KVConfig              jetstream.KeyValueConfig
	ActorLivenessKVConfig jetstream.KeyValueConfig
}

type Connection struct {
	NC              *nats.Conn
	JS              jetstream.JetStream
	Stream          jetstream.Stream
	StreamName      string
	ActorKV         jetstream.KeyValue
	ActorLivenessKV jetstream.KeyValue
}

func New(ctx context.Context, NATSURL string, ConnectOpts []nats.Option) (*Connection, error) {
	logger := telemetry.GetLogger(ctx, "connection-new")

	nc, err := nats.Connect(NATSURL, ConnectOpts...)
	if err != nil {
		logger.Error("failed to connect to NATS", zap.Error(err))
		return nil, fmt.Errorf("failed to connect to NATS: %w", err)
	}

	js, err := jetstream.New(nc)
	if err != nil {
		nc.Close()
		logger.Error("failed to create JetStream context", zap.Error(err))
		return nil, fmt.Errorf("failed to create JetStream context: %w", err)
	}

	return &Connection{
		NC: nc,
		JS: js,
	}, nil
}

func (c *Connection) Initialize(ctx context.Context, config Config) error {
	logger := telemetry.GetLogger(ctx, "connection-initialize")

	stream, err := c.JS.CreateStream(ctx, config.MessageStreamConfig)
	if err != nil {
		logger.Error("failed to create stream", zap.Error(err))
		return fmt.Errorf("failed to create stream: %w", err)
	}
	c.Stream = stream
	c.StreamName = config.MessageStreamConfig.Name

	kv, err := c.JS.CreateKeyValue(ctx, config.KVConfig)
	if err != nil {
		if err == jetstream.ErrBucketExists {
			kv, err = c.JS.KeyValue(ctx, config.KVConfig.Bucket)
			if err != nil {
				logger.Error("failed to get existing KV store", zap.Error(err))
				return fmt.Errorf("failed to get existing KV store: %w", err)
			}
		} else {
			logger.Error("failed to create KV store", zap.Error(err))
			return fmt.Errorf("failed to create KV store: %w", err)
		}
	}
	c.ActorKV = kv

	actorLivenessKV, err := c.JS.CreateKeyValue(ctx, config.ActorLivenessKVConfig)
	if err != nil {
		if err == jetstream.ErrBucketExists {
			actorLivenessKV, err = c.JS.KeyValue(ctx, config.ActorLivenessKVConfig.Bucket)
			if err != nil {
				logger.Error("failed to get existing actor liveness KV store", zap.Error(err))
				return fmt.Errorf("failed to get existing actor liveness KV store: %w", err)
			}
		} else {
			logger.Error("failed to create actor liveness KV store", zap.Error(err))
			return fmt.Errorf("failed to create actor liveness KV store: %w", err)
		}
	}
	c.ActorLivenessKV = actorLivenessKV

	return nil
}

func (c *Connection) IsKeyNotFound(err error) bool {
	return err == jetstream.ErrKeyNotFound
}

func (c *Connection) Close(ctx context.Context) {
	logger := telemetry.GetLogger(ctx, "connection-close")
	logger.Info("closing connection")

	c.NC.Close()
}

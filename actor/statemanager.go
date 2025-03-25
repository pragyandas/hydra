package actor

import (
	"context"
	"fmt"

	"github.com/pragyandas/hydra/connection"
)

type StateManager struct {
	actor           *Actor
	connection      *connection.Connection
	stateSerializer StateSerializer
}

func NewStateManager(actor *Actor, connection *connection.Connection, stateSerializer StateSerializer) *StateManager {
	return &StateManager{
		actor:           actor,
		connection:      connection,
		stateSerializer: stateSerializer,
	}
}

func (s *StateManager) Save(ctx context.Context, state any) error {
	key := fmt.Sprintf("%s/%s/%s", s.actor.Type(), s.actor.ID(), "state")
	data, err := s.stateSerializer.Serialize(state)
	if err != nil {
		return fmt.Errorf("failed to serialize state: %w", err)
	}
	_, err = s.connection.ActorKV.Put(ctx, key, data)
	if err != nil {
		return fmt.Errorf("failed to save state: %w", err)
	}
	return nil
}

func (s *StateManager) Load(ctx context.Context) (any, error) {
	key := fmt.Sprintf("%s/%s/%s", s.actor.Type(), s.actor.ID(), "state")
	data, err := s.connection.ActorKV.Get(ctx, key)
	if err != nil {
		if s.connection.IsKeyNotFound(err) {
			return nil, nil
		}
		return nil, fmt.Errorf("failed to load state: %w", err)
	}
	state, err := s.stateSerializer.Deserialize(data.Value())
	if err != nil {
		return nil, fmt.Errorf("failed to deserialize state: %w", err)
	}
	return state, nil
}

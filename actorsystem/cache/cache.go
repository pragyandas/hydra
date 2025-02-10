package cache

import (
	"context"
	"encoding/json"
	"fmt"
	"sync"

	"github.com/nats-io/nats.go/jetstream"
	"github.com/pragyandas/hydra/transport"
)

type Cache struct {
	kv        jetstream.KeyValue
	locations map[string]*ActorLocation       // key -> location
	watchers  map[string]jetstream.KeyWatcher // key -> watcher
	mu        sync.RWMutex
	ctx       context.Context
	cancel    context.CancelFunc
}

func NewCache(ctx context.Context, kv jetstream.KeyValue) (*Cache, error) {
	cacheCtx, cancel := context.WithCancel(ctx)
	return &Cache{
		kv:        kv,
		locations: make(map[string]*ActorLocation),
		watchers:  make(map[string]jetstream.KeyWatcher),
		ctx:       cacheCtx,
		cancel:    cancel,
	}, nil
}

func (c *Cache) GetLocation(actorType, actorID string) (*ActorLocation, error) {
	key := fmt.Sprintf("actors/%s/%s", actorType, actorID)

	// Check cache first
	c.mu.RLock()
	// TODO: Review if we should check if the actor is active
	if loc, ok := c.locations[key]; ok && loc.Status == transport.Active {
		c.mu.RUnlock()
		return loc, nil
	}
	c.mu.RUnlock()

	// Not in cache, get from KV store
	entry, err := c.kv.Get(c.ctx, key)
	if err != nil {
		return nil, fmt.Errorf("actor not found: %w", err)
	}

	var location ActorLocation
	if err := json.Unmarshal(entry.Value(), &location); err != nil {
		return nil, err
	}

	if location.Status != transport.Active {
		return nil, fmt.Errorf("actor is not active")
	}

	// Cache the location and setup watcher
	if err := c.cacheLocation(key, &location); err != nil {
		return nil, err
	}

	return &location, nil
}

func (c *Cache) cacheLocation(key string, location *ActorLocation) error {
	c.mu.Lock()
	defer c.mu.Unlock()

	// Add to cache
	c.locations[key] = location

	// Setup watcher if not already watching
	if _, exists := c.watchers[key]; !exists {
		watcher, err := c.kv.Watch(c.ctx, key)
		if err != nil {
			delete(c.locations, key)
			return fmt.Errorf("failed to setup watcher: %w", err)
		}

		c.watchers[key] = watcher
		go c.handleUpdates(key, watcher)
	}

	return nil
}

func (c *Cache) handleUpdates(key string, watcher jetstream.KeyWatcher) {
	for {
		select {
		case <-c.ctx.Done():
			return
		case update := <-watcher.Updates():
			if update == nil {
				continue
			}

			var location ActorLocation
			if err := json.Unmarshal(update.Value(), &location); err != nil {
				continue
			}

			c.mu.Lock()
			if location.Status == transport.Active {
				c.locations[key] = &location
			} else {
				delete(c.locations, key)
				if w, exists := c.watchers[key]; exists {
					w.Stop()
					delete(c.watchers, key)
				}
			}
			c.mu.Unlock()
		}
	}
}

func (c *Cache) InvalidateLocation(actorType, actorID string) {
	key := fmt.Sprintf("actors/%s/%s", actorType, actorID)

	c.mu.Lock()
	delete(c.locations, key)
	if w, exists := c.watchers[key]; exists {
		w.Stop()
		delete(c.watchers, key)
	}
	c.mu.Unlock()
}

func (c *Cache) Close() {
	c.cancel()
	c.mu.Lock()
	for _, w := range c.watchers {
		w.Stop()
	}
	c.watchers = nil
	c.locations = nil
	c.mu.Unlock()
}

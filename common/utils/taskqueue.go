package utils

import (
	"context"
	"sync"

	"github.com/pragyandas/hydra/telemetry"
	"go.uber.org/zap"
)

type Task struct {
	key string
	fn  func()
}

func NewTask(key string, fn func()) Task {
	return Task{
		key: key,
		fn:  fn,
	}
}

type TaskQueue struct {
	taskSet   map[string]struct{}
	tasks     []Task
	taskMux   sync.RWMutex
	semaphore chan struct{}
}

func NewTaskQueue(concurrency int) *TaskQueue {
	if concurrency <= 0 {
		concurrency = 10
	}

	return &TaskQueue{
		taskSet:   make(map[string]struct{}),
		tasks:     make([]Task, 0),
		taskMux:   sync.RWMutex{},
		semaphore: make(chan struct{}, concurrency),
	}
}

func (t *TaskQueue) Add(ctx context.Context, task Task) {
	logger := telemetry.GetLogger(ctx, "taskqueue-add")
	t.taskMux.Lock()
	defer t.taskMux.Unlock()

	if _, exists := t.taskSet[task.key]; exists {
		logger.Debug("task already exists", zap.String("key", task.key))
		return
	}

	t.taskSet[task.key] = struct{}{}
	t.tasks = append(t.tasks, task)
}

func (t *TaskQueue) Run(ctx context.Context) {
	logger := telemetry.GetLogger(ctx, "taskqueue-run")
	for {
		if len(t.tasks) == 0 {
			continue
		}
		select {
		case t.semaphore <- struct{}{}:
			t.taskMux.Lock()

			// Get the task out
			task := t.tasks[0]
			t.tasks = t.tasks[1:]
			delete(t.taskSet, task.key)

			t.taskMux.Unlock()

			// Run the task
			go func() {
				task.fn()
				<-t.semaphore
				logger.Debug("task completed", zap.String("key", task.key))
			}()
		case <-ctx.Done():
			return
		}
	}
}

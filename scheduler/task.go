package scheduler

import (
	"context"
	"sync"

	"sr.ht/moyanhao/bedrock-metaserver/errors"
	"sr.ht/moyanhao/bedrock-metaserver/operation"
)

// TaskPriority defines task priority levels
type TaskPriority int

const (
	PriorityLow    TaskPriority = 1
	PriorityMedium TaskPriority = 5
	PriorityHigh   TaskPriority = 10
	PriorityUrgent TaskPriority = 100
)

// Task defines the task interface
type Task interface {
	// ID returns the task ID
	ID() string
	// Priority returns the task priority
	Priority() TaskPriority
	// Execute runs the task
	Execute(ctx context.Context) error
	// Wait waits for the task to complete
	Wait()
	// IsDone checks if the task is completed
	IsDone() bool
	// GetError gets the task error
	GetError() error
	// Reason returns the task reason, used to prevent duplicate submission of tasks with the same reason
	Reason() string
}

// BaseTask provides the basic task implementation
type BaseTask struct {
	id         string
	priority   TaskPriority
	reason     string
	operations []operation.Operation
	waitGroup  sync.WaitGroup
	done       bool
	err        error
	doneMutex  sync.RWMutex
	errMutex   sync.RWMutex
}

// NewBaseTask creates a new base task
func NewBaseTask(id string, priority TaskPriority, reason string) *BaseTask {
	return &BaseTask{
		id:         id,
		priority:   priority,
		reason:     reason,
		operations: make([]operation.Operation, 0),
	}
}

// Reason returns the task reason
func (t *BaseTask) Reason() string {
	return t.reason
}

// ID returns the task ID
func (t *BaseTask) ID() string {
	return t.id
}

// Priority returns the task priority
func (t *BaseTask) Priority() TaskPriority {
	return t.priority
}

// AddOperation adds an operation to the task
func (t *BaseTask) AddOperation(op operation.Operation) {
	t.operations = append(t.operations, op)
}

// Execute runs all operations in the task
func (t *BaseTask) Execute(ctx context.Context) error {
	for _, op := range t.operations {
		t.waitGroup.Add(1)
		go func(operation operation.Operation) {
			defer t.waitGroup.Done()
			err := operation.Execute(ctx)
			if err != nil {
				t.setError(err)
			}
		}(op)
	}

	// Wait for all operations to complete
	t.waitGroup.Wait()
	t.setDone(true)
	return t.GetError()
}

// Wait waits for the task to complete
func (t *BaseTask) Wait() {
	t.waitGroup.Wait()
}

// IsDone checks if the task is completed
func (t *BaseTask) IsDone() bool {
	t.doneMutex.RLock()
	defer t.doneMutex.RUnlock()
	return t.done
}

// GetError gets the task error
func (t *BaseTask) GetError() error {
	t.errMutex.RLock()
	defer t.errMutex.RUnlock()
	return t.err
}

// setDone sets the task completion status
func (t *BaseTask) setDone(done bool) {
	t.doneMutex.Lock()
	t.done = done
	t.doneMutex.Unlock()
}

// setError sets the task error
func (t *BaseTask) setError(err error) {
	t.errMutex.Lock()
	if t.err == nil {
		t.err = err
	}
	t.errMutex.Unlock()
}

// SyncTask synchronous task implementation

type SyncTask struct {
	BaseTask
}

// NewSyncTask creates a new synchronous task
func NewSyncTask(id string, priority TaskPriority, reason string) *SyncTask {
	return &SyncTask{
		BaseTask: *NewBaseTask(id, priority, reason),
	}
}

// Execute runs the synchronous task, executing all operations sequentially
func (t *SyncTask) Execute(ctx context.Context) error {
	for _, op := range t.operations {
		err := op.Execute(ctx)
		if err != nil {
			t.setError(err)
			return err
		}
	}
	t.setDone(true)
	return nil
}

// AsyncTask asynchronous task implementation

type AsyncTask struct {
	BaseTask
}

// NewAsyncTask creates a new asynchronous task
func NewAsyncTask(id string, priority TaskPriority, reason string) *AsyncTask {
	return &AsyncTask{
		BaseTask: *NewBaseTask(id, priority, reason),
	}
}

// Execute runs the asynchronous task, executing all operations in parallel
func (t *AsyncTask) Execute(ctx context.Context) error {
	return t.BaseTask.Execute(ctx)
}

// TaskFactory task creation factory function
func TaskFactory(taskType string, id string, priority TaskPriority, reason string) (Task, error) {
	switch taskType {
	case "sync":
		return NewSyncTask(id, priority, reason), nil
	case "async":
		return NewAsyncTask(id, priority, reason), nil
	default:
		return nil, errors.Newf(errors.ErrCodeInvalidArgument, "unknown task type: %s", taskType)
	}
}

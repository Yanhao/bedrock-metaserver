package operation

import (
	"context"
	"sync"
)

// OperationType defines operation types
type OperationType string

const (
	OpCreateShard    OperationType = "CreateShard"
	OpDeleteShard    OperationType = "DeleteShard"
	OpSplitShard     OperationType = "SplitShard"
	OpMergeShard     OperationType = "MergeShard"
	OpMigrateShard   OperationType = "MigrateShard"
	OpTransferLeader OperationType = "TransferLeader"
)

// Operation defines the operation interface
type Operation interface {
	// Type returns the operation type
	Type() OperationType
	// Execute runs the operation, returns error information
	Execute(ctx context.Context) error
	// GetDataServer gets the data server address associated with the operation
	GetDataServer() string
}

// BaseOperation provides the basic operation implementation
type BaseOperation struct {
	opType     OperationType
	dataServer string
	shardID    uint64
	storageID  uint32
	priority   int
	waitGroup  *sync.WaitGroup
	doneChan   chan struct{}
	err        error
}

// NewBaseOperation creates a new base operation
func NewBaseOperation(opType OperationType, dataServer string, priority int) *BaseOperation {
	return &BaseOperation{
		opType:     opType,
		dataServer: dataServer,
		priority:   priority,
		waitGroup:  &sync.WaitGroup{},
		doneChan:   make(chan struct{}),
	}
}

// Type returns the operation type
func (b *BaseOperation) Type() OperationType {
	return b.opType
}

// GetDataServer gets the data server address associated with the operation
func (b *BaseOperation) GetDataServer() string {
	return b.dataServer
}

// Execute basic execution method, subclasses need to override
func (b *BaseOperation) Execute(ctx context.Context) error {
	return nil
}

// SetError sets the operation error
func (b *BaseOperation) SetError(err error) {
	b.err = err
}

// GetError gets the operation error
func (b *BaseOperation) GetError() error {
	return b.err
}

// Wait waits for the operation to complete
func (b *BaseOperation) Wait() {
	b.waitGroup.Wait()
}

// Done marks the operation as completed
func (b *BaseOperation) Done() {
	close(b.doneChan)
	b.waitGroup.Done()
}

// WaitChan returns the channel for operation completion
func (b *BaseOperation) WaitChan() <-chan struct{} {
	return b.doneChan
}

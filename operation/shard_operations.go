package operation

import (
	"context"
	log "github.com/sirupsen/logrus"

	"sr.ht/moyanhao/bedrock-metaserver/clients/dataserver"
)

// CreateShardOperation create shard operation
type CreateShardOperation struct {
	BaseOperation
	dataServer string
	shardID    uint64
	minKey     []byte
	maxKey     []byte
}

// NewCreateShardOperation creates a new create shard operation
func NewCreateShardOperation(dataServer string, shardID uint64, minKey, maxKey []byte, priority int) *CreateShardOperation {
	return &CreateShardOperation{
		BaseOperation: BaseOperation{
			priority:   priority,
			dataServer: dataServer,
		},
		dataServer: dataServer,
		shardID:    shardID,
		minKey:     minKey,
		maxKey:     maxKey,
	}
}

// Execute performs the create shard operation
func (op *CreateShardOperation) Execute(ctx context.Context) error {
	cli, err := dataserver.GetDataServerConns().GetApiClient(op.dataServer)
	if err != nil {
		log.Errorf("failed to get dataserver client: %v", err)
		return err
	}

	return cli.CreateShard(op.shardID, op.minKey, op.maxKey)
}

// DeleteShardOperation delete shard operation
type DeleteShardOperation struct {
	BaseOperation
	dataServer string
	shardID    uint64
}

// NewDeleteShardOperation creates a new delete shard operation
func NewDeleteShardOperation(dataServer string, shardID uint64, priority int) *DeleteShardOperation {
	return &DeleteShardOperation{
		BaseOperation: BaseOperation{
			priority:   priority,
			dataServer: dataServer,
		},
		dataServer: dataServer,
		shardID:    shardID,
	}
}

// Execute performs the delete shard operation
func (op *DeleteShardOperation) Execute(ctx context.Context) error {
	cli, err := dataserver.GetDataServerConns().GetApiClient(op.dataServer)
	if err != nil {
		log.Errorf("failed to get dataserver client: %v", err)
		return err
	}

	return cli.DeleteShard(op.shardID)
}

// SplitShardOperation split shard operation
type SplitShardOperation struct {
	BaseOperation
	dataServer string
	shardID    uint64
	newShardID uint64
}

// NewSplitShardOperation creates a new split shard operation
func NewSplitShardOperation(dataServer string, shardID uint64, newShardID uint64, priority int) *SplitShardOperation {
	return &SplitShardOperation{
		BaseOperation: BaseOperation{
			priority:   priority,
			dataServer: dataServer,
		},
		dataServer: dataServer,
		shardID:    shardID,
		newShardID: newShardID,
	}
}

// Execute performs the split shard operation
func (op *SplitShardOperation) Execute(ctx context.Context) error {
	cli, err := dataserver.GetDataServerConns().GetApiClient(op.dataServer)
	if err != nil {
		log.Errorf("failed to get dataserver client: %v", err)
		return err
	}

	return cli.SplitShard(op.shardID, op.newShardID)
}

// MergeShardOperation merge shard operation
type MergeShardOperation struct {
	BaseOperation
	dataServer string
	shardID    uint64
	targetID   uint64
}

// NewMergeShardOperation creates a new merge shard operation
func NewMergeShardOperation(dataServer string, shardID uint64, targetID uint64, priority int) *MergeShardOperation {
	return &MergeShardOperation{
		BaseOperation: BaseOperation{
			priority:   priority,
			dataServer: dataServer,
		},
		dataServer: dataServer,
		shardID:    shardID,
		targetID:   targetID,
	}
}

// Execute performs the merge shard operation
func (op *MergeShardOperation) Execute(ctx context.Context) error {
	cli, err := dataserver.GetDataServerConns().GetApiClient(op.dataServer)
	if err != nil {
		log.Errorf("failed to get dataserver client: %v", err)
		return err
	}

	return cli.MergeShard(op.shardID, op.targetID)
}

// MigrateShardOperation migrate shard operation
type MigrateShardOperation struct {
	BaseOperation
	dataServer string
	shardID    uint64
	targetID   uint64
	targetAddr string
}

// NewMigrateShardOperation creates a new migrate shard operation
func NewMigrateShardOperation(dataServer string, shardID uint64, targetID uint64, targetAddr string, priority int) *MigrateShardOperation {
	return &MigrateShardOperation{
		BaseOperation: BaseOperation{
			priority:   priority,
			dataServer: dataServer,
		},
		dataServer: dataServer,
		shardID:    shardID,
		targetID:   targetID,
		targetAddr: targetAddr,
	}
}

// Execute performs the migrate shard operation
func (op *MigrateShardOperation) Execute(ctx context.Context) error {
	cli, err := dataserver.GetDataServerConns().GetApiClient(op.dataServer)
	if err != nil {
		log.Errorf("failed to get dataserver client: %v", err)
		return err
	}

	return cli.MigrateShard(op.shardID, op.targetID, op.targetAddr)
}

// TransferLeaderOperation transfer shard leader operation
type TransferLeaderOperation struct {
	BaseOperation
	dataServer string
	shardID    uint64
	replicates []string
}

// NewTransferLeaderOperation creates a new transfer shard leader operation
func NewTransferLeaderOperation(dataServer string, shardID uint64, replicates []string, priority int) *TransferLeaderOperation {
	return &TransferLeaderOperation{
		BaseOperation: BaseOperation{
			priority:   priority,
			dataServer: dataServer,
		},
		dataServer: dataServer,
		shardID:    shardID,
		replicates: replicates,
	}
}

// Execute performs the transfer shard leader operation
func (op *TransferLeaderOperation) Execute(ctx context.Context) error {
	cli, err := dataserver.GetDataServerConns().GetApiClient(op.dataServer)
	if err != nil {
		log.Errorf("failed to get dataserver client: %v", err)
		return err
	}

	return cli.TransferShardLeader(op.shardID, op.replicates)
}

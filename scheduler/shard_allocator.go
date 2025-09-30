package scheduler

import (
	"errors"
	"fmt"
	"math"
	"math/big"
	"math/rand"
	"sync"

	log "github.com/sirupsen/logrus"

	"sr.ht/moyanhao/bedrock-metaserver/manager"
	"sr.ht/moyanhao/bedrock-metaserver/model"
	"sr.ht/moyanhao/bedrock-metaserver/operation"
)

const (
	MaxAllocateTimes = 6
	MaxValueSize     = 1024
)

var (
	MinKey = []byte{0x0}
	MaxKey = []byte{}
)

func init() {
	MaxKey = make([]byte, 128)
	for i := range MaxKey {
		MaxKey[i] = 0xFF
	}
}

type ShardAllocator struct {
}

func NewShardAllocator() *ShardAllocator {
	return &ShardAllocator{}
}

var (
	shardAllocator     *ShardAllocator
	shardAllocatorOnce sync.Once
)

func GetShardAllocator() *ShardAllocator {
	shardAllocatorOnce.Do(func() {
		shardAllocator = NewShardAllocator()
	})

	return shardAllocator
}

const (
	DefaultReplicatesCount = 3
)

func (sa *ShardAllocator) AllocateNewStorage(name string, rangeCount uint32) (*model.Storage, error) {
	storage, err := manager.GetStorageManager().CreateNewStorage(name)
	if err != nil {
		return nil, err
	}

	if err := sa.ExpandStorage(storage.ID, 1); err != nil {
		return nil, err
	}

	if err := manager.GetShardManager().PutShardIDByKey(storage.ID, MaxKey, model.GenerateShardID(storage.ID, 0)); err != nil {
		return nil, err
	}

	splitLoopCount := uint32(math.Sqrt(float64(rangeCount)))
	for i := 0; i < int(splitLoopCount); i++ {
		shardIDs, err := manager.GetShardManager().GetShardIDsInStorage(storage.ID)
		if err != nil {
			return nil, err
		}

		for _, shardID := range shardIDs {
			err := sa.SplitShard(shardID)
			if err != nil {
				return nil, err
			}
		}
	}

	return storage, nil
}

func (sa *ShardAllocator) AllocateShardReplicates(shardID model.ShardID, count int, minKey, maxKey []byte) ([]string, error) {
	var selectedDataServers []string

	// Get global scheduler
	scheduler := GetTaskScheduler()

	for i, times := count, MaxAllocateTimes; i > 0 && times > 0; {
		log.Infof("i: %v, times: %v", i, times)
		viableDataServers := manager.GetDataServerManager().GenerateViableDataServer(selectedDataServers)
		if len(viableDataServers) < i {
			return nil, errors.New("dataserver is not enough to allocate shard")
		}

		server := viableDataServers[rand.Intn(len(viableDataServers))]
		log.Infof("allocate shard: 0x%016x on dataserver: %s", shardID, server)

		// Create sync task
		taskID := fmt.Sprintf("allocate-shard-%d-%s", shardID, server)
		task := NewSyncTask(taskID, PriorityUrgent, fmt.Sprintf("allocate-shard-%d", shardID))

		// Create create shard operation
		createOp := operation.NewCreateShardOperation(
			server,
			uint64(shardID),
			minKey,
			maxKey,
			int(PriorityUrgent),
		)
		task.AddOperation(createOp)

		// Submit task to scheduler
		scheduler.SubmitTask(task)

		// Wait for task completion (allocator needs to wait synchronously)
		err := scheduler.WaitForTask(taskID)
		if err != nil {
			log.Warnf("failed to create shard %s (ID: 0x%016x) from dataserver %v, remaining attempts: %d, err: %v",
				fmt.Sprintf("allocate-shard-%d", shardID), shardID, server, times-1, err)
			times--
			continue
		}

		err = manager.GetShardManager().AddShardReplicates(shardID, []string{server})
		if err != nil {
			return nil, err
		}

		selectedDataServers = append(selectedDataServers, server)

		i--
		times--
	}

	if len(selectedDataServers) < count {
		return selectedDataServers, errors.New("not enough replicates")
	}

	err := manager.GetShardManager().ReSelectLeader(shardID)
	if err != nil {
		log.Errorf("failed to select leader of shard, shard id: 0x016%x, err: %v", shardID, err)
		return selectedDataServers, err
	}

	log.Infof("successfully create new shard replicate: shard_id: 0x%016x, addr: %v", shardID, selectedDataServers)

	return selectedDataServers, nil
}

func (sa *ShardAllocator) ExpandStorage(storageID model.StorageID, count uint32) error {
	for i := count; i > 0; i-- {
		shardIds, err := manager.GetShardManager().GetShardIDsInStorage(storageID)
		if err != nil {
			return err
		}

		if len(shardIds) == 0 {
			shard, err := manager.GetShardManager().CreateNewShard(storageID)
			if err != nil {
				return err
			}

			shard.RangeKeyEnd = MaxKey
			shard.RangeKeyStart = MinKey

			err = manager.GetShardManager().PutShard(shard)
			if err != nil {
				return err
			}

			log.Infof("new shard, id: 0x%016x", shard.ID())

			_, err = sa.AllocateShardReplicates(shard.ID(), DefaultReplicatesCount, MinKey, MaxKey)
			if err != nil {
				return err
			}
		}

		for _, shardID := range shardIds {
			if err := sa.SplitShard(shardID); err != nil {
				return err
			}
		}
	}

	return nil
}

func (sa *ShardAllocator) SplitShard(shardID model.ShardID) error {
	shard, err := manager.GetShardManager().GetShard(shardID)
	if err != nil {
		log.Warnf("failed to get shard, err: %v", err)
		return err
	}

	middleKey := shard.SplitShardRangeKey()

	newShard, err := manager.GetShardManager().CreateNewShard(shard.SID)
	if err != nil {
		return err
	}

	shard.RangeKeyEnd = middleKey
	newShard.RangeKeyStart = middleKey
	newShard.RangeKeyEnd = shard.RangeKeyStart

	err = manager.GetShardManager().PutShard(shard)
	if err != nil {
		return err
	}

	err = manager.GetShardManager().PutShard(newShard)
	if err != nil {
		return err
	}

	log.Infof("new shard, id: 0x%016x", newShard.ID())

	// Get global scheduler
	scheduler := GetTaskScheduler()

	// Perform shard operation on each replicate
	for replicateAddr := range shard.Replicates {
		// Create sync task
		taskID := fmt.Sprintf("split-shard-%d-%d-%s", shard.ID(), newShard.ID(), replicateAddr)
		task := NewSyncTask(taskID, PriorityHigh, fmt.Sprintf("split-shard-%d", shard.ID()))

		// Create split shard operation
		splitOp := operation.NewSplitShardOperation(
			replicateAddr,
			uint64(shard.ID()),
			uint64(newShard.ID()),
			int(PriorityHigh),
		)
		task.AddOperation(splitOp)

		// Submit task to scheduler
		scheduler.SubmitTask(task)

		// Wait for task completion (allocator needs to wait synchronously)
		err := scheduler.WaitForTask(taskID)
		if err != nil {
			log.Errorf("failed to split shard on dataserver %v, err: %v", replicateAddr, err)
			return err
		}
	}

	return nil
}

func (sa *ShardAllocator) MergeShardByKey(storageID model.StorageID, key []byte) error {
	shm := manager.GetShardManager()

	shardID, err := shm.GetShardIDByKey(storageID, key)
	if err != nil {
		return err
	}

	shard, err := shm.GetShard(shardID)
	if err != nil {
		return err
	}

	if shard.ValueSize() > MaxValueSize/2 {
		return nil
	}

	max := big.NewInt(0)
	max.SetBytes(shard.RangeKeyStart)
	max.Sub(max, big.NewInt(2))

	prevMaxKey := max.Bytes()
	prevShardID, err := shm.GetShardIDByKey(storageID, prevMaxKey)
	if err != nil {
		return err
	}
	prevShard, err := shm.GetShard(prevShardID)
	if err != nil {
		return err
	}

	if prevShard.ValueSize() < MaxValueSize/2 {
		return sa.doMergeShard(shard, prevShard)
	}

	min := big.NewInt(0)
	min.SetBytes(shard.RangeKeyEnd)
	min.Add(min, big.NewInt(1))

	nexMinKey := min.Bytes()
	nextShardID, err := shm.GetShardIDByKey(storageID, nexMinKey)
	if err != nil {
		return err
	}
	nextShard, err := shm.GetShard(nextShardID)
	if err != nil {
		return err
	}

	if nextShard.ValueSize() < MaxValueSize/2 {
		return sa.doMergeShard(shard, nextShard)
	}

	return nil
}

func (sa *ShardAllocator) doMergeShard(aShard, bShard *model.Shard) error {
	aShard.RangeKeyEnd = bShard.RangeKeyEnd

	// Get global scheduler
	scheduler := GetTaskScheduler()

	// Perform merge operation on each replicate
	for addr := range aShard.Replicates {
		// Create sync task
		taskID := fmt.Sprintf("merge-shard-%d-%d-%s", aShard.ID(), bShard.ID(), addr)
		task := NewSyncTask(taskID, PriorityHigh, fmt.Sprintf("merge-shard-%d-%d", aShard.ID(), bShard.ID()))

		// Create merge shard operation
		mergeOp := operation.NewMergeShardOperation(
			addr,
			uint64(aShard.ID()),
			uint64(bShard.ID()),
			int(PriorityHigh),
		)
		task.AddOperation(mergeOp)

		// Submit task to scheduler
		scheduler.SubmitTask(task)

		// Wait for task completion (allocator needs to wait synchronously)
		err := scheduler.WaitForTask(taskID)
		if err != nil {
			log.Errorf("failed to merge shard on dataserver %v, err: %v", addr, err)
			return err
		}
	}

	shm := manager.GetShardManager()
	shm.ShardDelete(bShard.ID())

	shm.PutShard(aShard)

	shm.RemoveShardRangeByKey(aShard.SID, aShard.RangeKeyEnd)

	return nil
}

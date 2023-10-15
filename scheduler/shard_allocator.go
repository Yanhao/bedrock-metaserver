package scheduler

import (
	"errors"
	"math"
	"math/big"
	"math/rand"
	"net"
	"sync"

	log "github.com/sirupsen/logrus"

	"sr.ht/moyanhao/bedrock-metaserver/clients/dataserver"
	"sr.ht/moyanhao/bedrock-metaserver/manager"
	"sr.ht/moyanhao/bedrock-metaserver/model"
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
	MaxKey = make([]byte, 512)
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

func generateViableDataServer(selected []string) []string {
	var ret []string

	dm := manager.GetDataServerManager()
	dataservers := dm.GetDataServersCopy()

outer:
	for addr, ds := range dataservers {
		if ds.IsOverLoaded() {
			continue
		}

		host, _, err := net.SplitHostPort(addr)
		if err != nil {
			continue
		}

		for _, s := range selected {
			if s == addr {
				continue outer
			}

			shost, _, err := net.SplitHostPort(s)
			if err != nil {
				continue outer
			}

			if shost == host {
				//FIXME: remove the following comments
				// continue outer
			}
		}

		ret = append(ret, addr)
	}

	return ret
}

func randomSelect(dataServers []string) string {
	length := len(dataServers)
	if length == 0 {
		log.Warnf("input dataserver is empty")
		return ""
	}

	return dataServers[rand.Intn(length)]
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

func (sa *ShardAllocator) AllocateShardReplicates(shardID model.ShardID, count int) ([]string, error) {
	var selectedDataServers []string
	conns := dataserver.GetDataServerConns()

	for i, times := count, MaxAllocateTimes; i > 0 && times > 0; {
		log.Infof("i: %v, times: %v", i, times)
		viableDataServers := generateViableDataServer(selectedDataServers)
		if len(viableDataServers) < i {
			return nil, errors.New("dataserver is not enough to allocate shard")
		}

		server := randomSelect(viableDataServers)
		log.Infof("allocate shard: 0x%016x on dataserver: %s", shardID, server)

		dataServerCli, _ := conns.GetApiClient(server)

		err := dataServerCli.CreateShard(uint64(shardID))
		if err != nil {
			log.Warnf("failed to create shard from dataserver %v, err: %v", server, err)

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

		_, err = sa.AllocateShardReplicates(shard.ID(), DefaultReplicatesCount)
		if err != nil {
			return err
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

	_, err = sa.AllocateShardReplicates(newShard.ID(), DefaultReplicatesCount)
	if err != nil {
		return err
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

	conns := dataserver.GetDataServerConns()
	aDs, _ := conns.GetApiClient(aShard.Leader)
	err := aDs.MigrateShard(uint64(aShard.ID()), uint64(bShard.ID()), bShard.Leader)
	if err != nil {
		return err
	}

	bDs, _ := conns.GetApiClient(bShard.Leader)

	bDs.MergeShard(uint64(aShard.ID()), uint64(bShard.ID()))

	shm := manager.GetShardManager()
	shm.ShardDelete(bShard.ID())

	shm.PutShard(aShard)

	shm.RemoveShardRangeByKey(aShard.SID, aShard.RangeKeyEnd)

	return nil
}

package scheduler

import (
	"errors"
	"math"
	"math/big"
	"math/rand"
	"net"
	"sync"

	"sr.ht/moyanhao/bedrock-metaserver/common/log"
	"sr.ht/moyanhao/bedrock-metaserver/dataserver"
	"sr.ht/moyanhao/bedrock-metaserver/metadata"
)

const (
	MAX_ALLOCATE_TIMES = 6
	MAX_VALUE_SIZE     = 1024
)

var (
	MIN_KEY = []byte{0x0}
	MAX_KEY = []byte{}
)

func init() {
	MAX_KEY = make([]byte, 512, 512)
	for i := range MAX_KEY {
		MAX_KEY[i] = 0xFF
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

	dm := metadata.GetDataServerManager()
	dataservers := dm.DataServersCopy()

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
		log.Warn("input dataserver is empty")
		return ""
	}

	return dataServers[rand.Intn(length)]
}

const (
	DefaultReplicatesCount = 3
)

func (sa *ShardAllocator) AllocatorNewStorage(name string, rangeCount uint32) (*metadata.Storage, error) {
	sm := metadata.GetStorageManager()
	storage, err := sm.CreateNewStorage(name)
	if err != nil {
		return nil, err
	}

	err = sa.ExpandStorage(storage.ID, 1)
	if err != nil {
		return nil, err
	}

	err = storage.PutShardIDByKey(MAX_KEY, metadata.GenerateShardID(storage.ID, 0))
	if err != nil {
		return nil, err
	}

	splitLoopCount := uint32(math.Sqrt(float64(rangeCount)))
	for i := 0; i < int(splitLoopCount); i++ {
		shardIDs, err := storage.GetShardIDs()
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

func (sa *ShardAllocator) AllocateShardReplicates(shardID metadata.ShardID, count int) ([]string, error) {
	var selectedDataServers []string
	conns := dataserver.GetDataServerConns()

	sm := metadata.GetShardManager()

	for i, times := count, MAX_ALLOCATE_TIMES; i > 0 && times > 0; {
		log.Info("i: %v, times: %v", i, times)
		viableDataServers := generateViableDataServer(selectedDataServers)
		if len(viableDataServers) < i {
			return nil, errors.New("dataserver is not enough to allocate shard")
		}

		server := randomSelect(viableDataServers)
		log.Info("allocate shard: 0x%016x on dataserver: %s", shardID, server)

		dataServerCli, _ := conns.GetApiClient(server)

		err := dataServerCli.CreateShard(uint64(shardID))
		if err != nil {
			log.Warn("failed to create shard from dataserver %v, err: %v", server, err)

			times--
			continue
		}

		err = sm.AddShardReplicates(shardID, []string{server})
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

	err := sm.ReSelectLeader(shardID)
	if err != nil {
		log.Error("failed to select leader of shard, shard id: 0x016%x, err: %v", shardID, err)
		return selectedDataServers, err
	}

	log.Info("successfully create new shard replicate: shard_id: 0x%016x, addr: %v", shardID, selectedDataServers)

	return selectedDataServers, nil
}

func (sa *ShardAllocator) ExpandStorage(storageID metadata.StorageID, count uint32) error {
	sm := metadata.GetShardManager()

	for i := count; i > 0; {
		shard, err := sm.CreateNewShard(storageID)
		if err != nil {
			return err
		}

		shard.RangeKeyMax = MAX_KEY
		shard.RangeKeyMin = MIN_KEY

		err = sm.PutShard(shard)
		if err != nil {
			return err
		}

		log.Info("new shard, id: 0x%016x", shard.ID())

		_, err = sa.AllocateShardReplicates(shard.ID(), DefaultReplicatesCount)
		if err != nil {
			return err
		}

		i--
	}

	return nil
}

func (sa *ShardAllocator) SplitShard(shardID metadata.ShardID) error {
	sm := metadata.GetShardManager()
	shard, err := sm.GetShardCopy(shardID)
	if err != nil {
		log.Warn("failed to get shard, err: %v", err)
		return err
	}

	middleKey := shard.SplitShardRangeKey()

	newShard, err := sm.CreateNewShard(shard.SID)
	if err != nil {
		return err
	}

	shard.RangeKeyMax = middleKey
	newShard.RangeKeyMin = middleKey
	newShard.RangeKeyMax = shard.RangeKeyMin

	err = sm.PutShard(shard)
	if err != nil {
		return err
	}

	err = sm.PutShard(newShard)
	if err != nil {
		return err
	}

	log.Info("new shard, id: 0x%016x", newShard.ID())

	_, err = sa.AllocateShardReplicates(newShard.ID(), DefaultReplicatesCount)
	if err != nil {
		return err
	}

	return nil
}

func (sa *ShardAllocator) MergeShardByKey(storageID metadata.StorageID, key []byte) error {
	stm := metadata.GetStorageManager()
	st, err := stm.GetStorage(storageID)
	if err != nil {
		return err
	}

	shardID, err := st.GetShardIDByKey(key)
	if err != nil {
		return err
	}

	shm := metadata.GetShardManager()
	shard, err := shm.GetShardCopy(shardID)
	if err != nil {
		return err
	}

	if shard.ValueSize() > MAX_VALUE_SIZE/2 {
		return nil
	}

	max := big.NewInt(0)
	max.SetBytes(shard.RangeKeyMin)
	max.Sub(max, big.NewInt(2))

	prevMaxKey := max.Bytes()
	prevShardID, err := st.GetShardIDByKey(prevMaxKey)
	if err != nil {
		return err
	}
	prevShard, err := shm.GetShardCopy(prevShardID)
	if err != nil {
		return err
	}

	if prevShard.ValueSize() < MAX_VALUE_SIZE/2 {
		return sa.doMergeShard(shard, prevShard)
	}

	min := big.NewInt(0)
	min.SetBytes(shard.RangeKeyMax)
	min.Add(min, big.NewInt(1))

	nexMinKey := min.Bytes()
	nextShardID, err := st.GetShardIDByKey(nexMinKey)
	if err != nil {
		return err
	}
	nextShard, err := shm.GetShardCopy(nextShardID)
	if err != nil {
		return err
	}

	if nextShard.ValueSize() < MAX_VALUE_SIZE/2 {
		return sa.doMergeShard(shard, nextShard)
	}

	return nil
}

func (sa *ShardAllocator) doMergeShard(aShard, bShard *metadata.Shard) error {
	aShard.RangeKeyMax = bShard.RangeKeyMax

	conns := dataserver.GetDataServerConns()
	aDs, _ := conns.GetApiClient(aShard.Leader)
	err := aDs.MigrateShard(uint64(aShard.ID()), uint64(bShard.ID()), bShard.Leader)
	if err != nil {
		return err
	}

	bDs, _ := conns.GetApiClient(bShard.Leader)

	bDs.MergeShard(uint64(aShard.ID()), uint64(bShard.ID()))

	shm := metadata.GetShardManager()
	shm.ShardDelete(bShard.ID())

	shm.PutShard(aShard)

	stm := metadata.GetStorageManager()
	st, _ := stm.GetStorage(aShard.SID)

	st.RmoveShardRnageByKey(aShard.RangeKeyMax)

	return nil
}

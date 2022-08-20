package scheduler

import (
	"errors"
	"math/rand"
	"net"
	"sync"

	"sr.ht/moyanhao/bedrock-metaserver/common/log"
	"sr.ht/moyanhao/bedrock-metaserver/dataserver"
	"sr.ht/moyanhao/bedrock-metaserver/metadata"
)

const (
	MAX_ALLOCATE_TIMES = 6
)

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

func (sa *ShardAllocator) AllocatorNewStorage(name string) (*metadata.Storage, error) {
	sm := metadata.GetStorageManager()
	storage, err := sm.CreateNewStorage(name)
	if err != nil {
		return nil, err
	}

	err = sa.ExpandStorage(storage.ID, 3)
	if err != nil {
		return nil, err
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
		log.Error("failed to select leader of shard, shard id: %v, err: %v", shardID, err)
		return selectedDataServers, err
	}

	log.Info("successfully create new shard replicate: shard_id: %v, addr: %v", shardID, selectedDataServers)

	return selectedDataServers, nil
}

func (sa *ShardAllocator) ExpandStorage(storageID metadata.StorageID, count uint32) error {
	sm := metadata.GetShardManager()

	for i := count; i > 0; {
		shard, err := sm.CreateNewShard(storageID)
		if err != nil {
			return err
		}

		log.Info("new shard, id: %v", shard.ID())

		addrs, err := sa.AllocateShardReplicates(shard.ID(), DefaultReplicatesCount)
		if err != nil {
			return err
		}

		for _, addr := range addrs {
			shard.AddReplicates([]string{addr})
		}

		i--
	}

	return nil
}

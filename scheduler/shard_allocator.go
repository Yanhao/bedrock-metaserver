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

type ShardAllocator struct {
}

func NewShardAllocator() *ShardAllocator {
	return &ShardAllocator{}
}

var (
	shardAllocator    *ShardAllocator
	shardAllcatorOnce sync.Once
)

func GetShardAllocator() *ShardAllocator {
	shardAllcatorOnce.Do(func() {
		shardAllocator = NewShardAllocator()
	})

	return shardAllocator
}

func generateViableDataServer(selected []string) []string {
	var ret []string

outer:
	for addr, ds := range metadata.DataServers {
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
				continue outer
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
	defaultReplicatesCount = 3
)

func (sa *ShardAllocator) AllocatorNewStorage() (*metadata.Storage, error) {
	storage, err := metadata.CreateNewStorage()
	if err != nil {
		return nil, err
	}

	err = sa.ExpandStorage(storage, 10)
	if err != nil {
		return nil, err
	}

	return storage, nil
}

func (sa *ShardAllocator) AllocateShardReplicates(shardID metadata.ShardID) ([]string, error) {
	var selectedDataServers []string
	conns := dataserver.GetDataServerConns()

	for i := defaultReplicatesCount; i > 0; {
		viableDataServers := generateViableDataServer(selectedDataServers)
		if len(viableDataServers) < i {
			return nil, errors.New("dataserver is not enough to allocate shard")
		}

		server := randomSelect(viableDataServers)

		dataServerCli := conns.GetApiClient(server)

		err := dataServerCli.CreateShard(uint64(shardID))
		if err != nil {
			log.Warn("failed to delete shard from dataserver, err: %v", err)

			continue
		}

		selectedDataServers = append(selectedDataServers, server)
		i--
	}
	return selectedDataServers, nil
}

func (sa *ShardAllocator) ExpandStorage(storage *metadata.Storage, count uint32) error {
	sm := metadata.GetShardManager()
	shard, err := sm.CreateNewShard(storage)
	if err != nil {
		return err
	}

	for i := count; i > 0; {
		addrs, err := sa.AllocateShardReplicates(shard.ID)
		if err != nil {
			return err
		}

		for _, addr := range addrs {
			shard.Replicates[addr] = struct{}{}
		}

		i--
	}

	err = sm.PutShard(shard)
	if err != nil {
		return err
	}

	return nil
}

package metadata

import (
	"errors"
	"math/rand"
	"sync"
	"time"

	cache "github.com/hashicorp/golang-lru"

	"sr.ht/moyanhao/bedrock-metaserver/common/log"
	"sr.ht/moyanhao/bedrock-metaserver/dataserver"
)

// const (
// 	KvLastShardIDkey = "/last_shard_id"
// )

// var lastShardID uint64

// func LoadLastShardID() error {
// 	ec := kv.GetEtcdClient()
// 	resp, err := ec.Get(context.Background(), KvLastShardIDkey)
// 	if err != nil {
// 		log.Error("failed load last storage id from etcd, err: %v", err)
// 		return err
// 	}

// 	for _, kv := range resp.Kvs {

// 		sID, err := strconv.ParseUint(string(kv.Value), 10, 64)
// 		if err != nil {
// 			return err
// 		}
// 		lastStorageID = sID
// 		break
// 	}

// 	return nil
// }

// func SaveLastShardID() error {
// 	ec := kv.GetEtcdClient()
// 	_, err := ec.Put(context.Background(), KvLastShardIDkey, strconv.FormatUint(lastShardID, 10))
// 	if err != nil {
// 		return err
// 	}

// 	return nil
// }

type ShardID uint64

type Shard struct {
	ID              ShardID
	SID             StorageID
	Replicates      map[string]struct{}
	ReplicaUpdateTs time.Time
	IsDeleted       bool
	DeleteTs        time.Time
	CreateTs        time.Time
	Leader          string
	LeaderChangeTs  time.Time
}

func (sd *Shard) Info() {
}

func (sd *Shard) RemoveReplicates(addrs []string) {
	for _, addr := range addrs {
		delete(sd.Replicates, addr)
	}

}

func (sd *Shard) AddReplicates(addrs []string) {
	for _, addr := range addrs {
		sd.Replicates[addr] = struct{}{}
	}
}

func (sd *Shard) markDelete() {
	sd.IsDeleted = true
	sd.DeleteTs = time.Now()
}

func (sd *Shard) markUndelete() {
	sd.IsDeleted = false
	sd.DeleteTs = time.Time{}
}

func (sd *Shard) Repair() {
}

type shardOption struct {
	leaderAddr string
}

type shardOpFunc func(*shardOption)

func WithLeader(addr string) shardOpFunc {
	return func(opt *shardOption) {
		opt.leaderAddr = addr
	}
}

func (sd *Shard) ReSelectLeader(ops ...shardOpFunc) error {
	conns := dataserver.GetDataServerConns()
	dataSerCli, _ := conns.GetApiClient(sd.Leader)
	// FIXME error handling

	var opts shardOption
	for _, opf := range ops {
		opf(&opts)
	}

	nextLeader := opts.leaderAddr
	if nextLeader == "" {
		var candidates []string
		for addr := range sd.Replicates {
			if addr != sd.Leader {
				candidates = append(candidates, addr)
			}
		}

		if len(candidates) == 0 {
			return errors.New("no enought candidates")
		}

		nextLeader = candidates[rand.Intn(len(candidates))]
	}

	err := dataSerCli.TransferShardLeader(uint64(sd.ID), nextLeader)
	if err != nil {
		return err
	}
	return nil

}

type ShardManager struct {
	shardsCache *cache.Cache
}

func NewShardManager() *ShardManager {
	c, err := cache.New(102400)
	if err != nil {
		panic("create cache failed")
	}

	return &ShardManager{
		shardsCache: c,
	}
}

var (
	shardManager     *ShardManager
	shardManagerOnce sync.Once
)

func GetShardManager() *ShardManager {
	shardManagerOnce.Do(func() {
		shardManager = NewShardManager()
	})

	return shardManager
}

func (sm *ShardManager) ClearCache() {
	sm.shardsCache.Purge()
}

func (sm *ShardManager) GetShard(shardID ShardID) (*Shard, error) {
	v, ok := sm.shardsCache.Get(shardID)
	if ok {
		return v.(*Shard), nil
	}

	shard, err := getShardFromKv(shardID)
	if err != nil {
		return nil, err
	}

	_ = sm.shardsCache.Add(shardID, shard)

	return shard, nil
}

func (sm *ShardManager) PutShard(shard *Shard) error {
	err := putShardToKv(shard)
	if err != nil {
		return err
	}

	sm.updateCache(shard)

	return nil
}

func (sm *ShardManager) MarkDelete(shard *Shard) error {
	shard.markDelete()
	sm.updateCache(shard)

	return sm.PutShard(shard)
}

func (sm *ShardManager) MarkUndelete(shard *Shard) error {
	shard.markUndelete()
	sm.updateCache(shard)

	return sm.PutShard(shard)
}

func (sm *ShardManager) updateCache(shard *Shard) {
	_ = sm.shardsCache.Add(shard.ID, shard)
}

func generateShardID(storageID StorageID, shardIndex uint32) ShardID {
	shardID := (uint64(storageID) << 32) & (uint64(shardIndex))

	return ShardID(shardID)
}

func (sm *ShardManager) CreateNewShard(storage *Storage) (*Shard, error) {
	shardIndex := storage.LastShardIndex + 1
	storage.LastShardIndex++

	err := putStorageToKv(storage)
	if err != nil {
		return nil, err
	}

	shardID := generateShardID(storage.ID, shardIndex)

	shard := &Shard{
		ID:              shardID,
		SID:             storage.ID,
		CreateTs:        time.Now(),
		IsDeleted:       false,
		ReplicaUpdateTs: time.Now(),
		Replicates:      map[string]struct{}{},
	}

	err = putShardToKv(shard)
	if err != nil {
		return nil, err
	}

	return shard, nil
}

func (sm *ShardManager) DeleteShard(shardID ShardID) error {
	shard, err := sm.GetShard(shardID)
	if err != nil {
		return err
	}

	conns := dataserver.GetDataServerConns()

	for addr := range shard.Replicates {
		dataServerCli, _ := conns.GetApiClient(addr)

		err := dataServerCli.DeleteShard(uint64(shardID))
		if err != nil {
			log.Warn("failed to delete shard from dataserver, err: %v", err)
			return err
		}
	}

	err = deleteShardFromKv(shard)
	if err != nil {
		return err
	}

	sm.shardsCache.Remove(shardID)

	return nil
}

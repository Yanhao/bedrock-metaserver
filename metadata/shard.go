package metadata

import (
	"context"
	"strconv"
	"sync"
	"time"

	cache "github.com/hashicorp/golang-lru"
	"sr.ht/moyanhao/bedrock-metaserver/common/log"
	"sr.ht/moyanhao/bedrock-metaserver/kv"
)

const (
	KvLastShardIDkey = "/last_shard_id"
)

var lastShardID uint64

func LoadLastShardID() error {
	ec := kv.GetEtcdClient()
	resp, err := ec.Get(context.Background(), KvLastShardIDkey)
	if err != nil {
		log.Error("failed load last storage id from etcd, err: %v", err)
		return err
	}

	for _, kv := range resp.Kvs {

		sID, err := strconv.ParseUint(string(kv.Value), 10, 64)
		if err != nil {
			return err
		}
		lastStorageID = sID
		break
	}

	return nil
}

func SaveLastShardID() error {
	ec := kv.GetEtcdClient()
	_, err := ec.Put(context.Background(), KvLastShardIDkey, strconv.FormatUint(lastShardID, 10))
	if err != nil {
		return err
	}

	return nil
}

type ShardID uint64

type Shard struct {
	ID              ShardID
	SID             StorageID
	Replicates      map[string]struct{}
	ReplicaUpdateTs time.Time
	IsDeleted       bool
	DeleteDTs       time.Time
	CreatedTs       time.Time
}

func (sd *Shard) Info() {
}

func (sd *Shard) Transfer() {
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

func (sd *Shard) MarkDelete() {
	sd.IsDeleted = true
	sd.DeleteDTs = time.Now()
}

func (sd *Shard) MarkUndelete() {
	sd.IsDeleted = false
	sd.DeleteDTs = time.Time{}
}

func (sd *Shard) Repair() {
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

	shard, err := GetShard(shardID)
	if err != nil {
		return nil, err
	}

	_ = sm.shardsCache.Add(shardID, shard)

	return shard, nil
}

func (sm *ShardManager) PutShard(shard *Shard) error {
	err := PutShard(shard)
	if err != nil {
		return err
	}

	_ = sm.shardsCache.Add(shard.ID, shard)

	return nil
}

func (sm *ShardManager) MarkDelete(shard *Shard) error {
	shard.MarkDelete()
	sm.updateCache(shard)

	return sm.PutShard(shard)
}

func (sm *ShardManager) MarkUndelete(shard *Shard) error {
	shard.MarkUndelete()
	sm.updateCache(shard)

	return sm.PutShard(shard)
}

func (sm *ShardManager) updateCache(shard *Shard) {
	_ = sm.shardsCache.Add(shard.ID, shard)
}

func (sm *ShardManager) CreateNewShard(storageID StorageID, addrs []string) (*Shard, error) {
	id := lastShardID + 1
	lastShardID++

	shard := &Shard{
		ID:              ShardID(id),
		SID:             storageID,
		CreatedTs:       time.Now(),
		IsDeleted:       false,
		ReplicaUpdateTs: time.Now(),
	}
	for _, addr := range addrs {
		shard.Replicates[addr] = struct{}{}
	}

	err := PutShard(shard)
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

	err = DeleteShard(shard)
	if err != nil {
		return err
	}

	sm.shardsCache.Remove(shardID)

	return nil
}

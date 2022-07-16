package metadata

import (
	"context"
	"errors"
	"fmt"
	"math/rand"
	"sync"
	"time"

	cache "github.com/hashicorp/golang-lru"
	"github.com/jinzhu/copier"

	"sr.ht/moyanhao/bedrock-metaserver/common/log"
	"sr.ht/moyanhao/bedrock-metaserver/dataserver"
	"sr.ht/moyanhao/bedrock-metaserver/kv"
)

type ShardISN uint32
type ShardID uint64

type Shard struct {
	ISN             ShardISN
	SID             StorageID
	Replicates      map[string]struct{}
	ReplicaUpdateTs time.Time
	IsDeleted       bool
	DeleteTs        time.Time
	CreateTs        time.Time
	Leader          string
	LeaderChangeTs  time.Time

	lock sync.RWMutex
}

func (sd *Shard) Info() string {
	return fmt.Sprintf("%+v", sd)
}

func (sd *Shard) RemoveReplicates(addrs []string) {
	for _, addr := range addrs {
		delete(sd.Replicates, addr)
	}
}

func (sd *Shard) AddReplicates(addrs []string) {
	sd.lock.Lock()
	defer sd.lock.Unlock()

	for _, addr := range addrs {
		sd.Replicates[addr] = struct{}{}
	}
}

func (sd *Shard) ID() ShardID {
	return GenerateShardID(sd.SID, sd.ISN)
}

func (sd *Shard) markDelete() {
	sd.lock.Lock()
	defer sd.lock.Unlock()

	sd.IsDeleted = true
	sd.DeleteTs = time.Now()
}

func (sd *Shard) markUndelete() {
	sd.lock.Lock()
	defer sd.lock.Unlock()

	sd.IsDeleted = false
	sd.DeleteTs = time.Time{}
}

func (sd *Shard) Copy() *Shard {
	sd.lock.RLock()
	defer sd.lock.RUnlock()

	var ret Shard
	copier.Copy(&ret, sd)

	return &ret
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
			return errors.New("no enough candidates")
		}

		nextLeader = candidates[rand.Intn(len(candidates))]
	}

	replicates := []string{}
	for addr := range sd.Replicates {
		replicates = append(replicates, addr)
	}
	err := dataSerCli.TransferShardLeader(uint64(sd.ID()), replicates)
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

func (sm *ShardManager) GetShardCopy(shardID ShardID) (*Shard, error) {
	shard, err := sm.GetShard(shardID)
	if err != nil {
		return nil, err
	}

	return shard.Copy(), nil
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

func GenerateShardID(storageID StorageID, shardISN ShardISN) ShardID {
	shardID := (uint64(storageID) << 32) & (uint64(shardISN))

	return ShardID(shardID)
}

func (sm *ShardManager) CreateNewShard(storage *Storage) (*Shard, error) {
	shardISN := storage.FetchAddLastISN()

	err := putStorageToKv(storage)
	if err != nil {
		return nil, err
	}

	shard := &Shard{
		ISN:             shardISN,
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

	sm.shardsCache.Add(shard.ID, shard)
	return shard, nil
}

func (sm *ShardManager) CreateNewShardByIDs(storageID StorageID, shardISN ShardISN) (*Shard, error) {
	shard := &Shard{
		ISN:             shardISN,
		SID:             storageID,
		CreateTs:        time.Now(),
		IsDeleted:       false,
		ReplicaUpdateTs: time.Now(),
		Replicates:      map[string]struct{}{},
	}
	err := putShardToKv(shard)
	if err != nil {
		return nil, err
	}

	sm.shardsCache.Add(shard.ID(), shard)
	return shard, nil
}

func (sm *ShardManager) ShardDelete(shardID ShardID) error {
	shard, err := sm.GetShard(shardID)
	if err != nil {
		return err
	}

	conns := dataserver.GetDataServerConns()

	for addr := range shard.Replicates {
		dataServerCli, _ := conns.GetApiClient(addr)

		err := dataServerCli.DeleteShard(uint64(shardID), 0)
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

func RemoveShardInDataServer(addr string, id ShardID) error {
	key := ShardInDataServerKey(addr, id)
	ec := kv.GetEtcdClient()

	_, err := ec.Delete(context.Background(), key)
	if err != nil {
		return err
	}

	return nil
}

func AddShardInDataServer(addr string, id ShardID) error {
	key := ShardInDataServerKey(addr, id)
	ec := kv.GetEtcdClient()

	_, err := ec.Put(context.Background(), key, "")
	if err != nil {
		return err
	}

	return nil
}

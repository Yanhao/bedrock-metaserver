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

type (
	ShardISN uint32
	ShardID  uint64
)

type Shard struct {
	ISN             ShardISN // internal serial number
	SID             StorageID
	Replicates      map[string]struct{}
	ReplicaUpdateTs time.Time
	IsDeleted       bool
	DeleteTs        time.Time
	CreateTs        time.Time
	Leader          string
	LeaderChangeTs  time.Time

	lock sync.RWMutex `copier:"-"`
}

func GenerateShardID(storageID StorageID, shardISN ShardISN) ShardID {
	shardID := (uint64(storageID) << 32) & (uint64(shardISN))

	return ShardID(shardID)
}

func (sd *Shard) Info() string {
	sd.lock.RLock()
	defer sd.lock.RUnlock()

	return fmt.Sprintf(
		"Shard{ ISN: %08x, StorageID: %08x, IsDeleted: %v, Leader: %s, LeaderChangeTs: %v, ReplicateUpdateTs: %v, DeleteTs: %v, CreateTs: %v }",
		sd.ISN,
		sd.SID,
		sd.IsDeleted,
		sd.Leader,
		sd.LeaderChangeTs,
		sd.ReplicaUpdateTs,
		sd.DeleteTs,
		sd.CreateTs,
	)
}

func (sd *Shard) ID() ShardID {
	sd.lock.RLock()
	defer sd.lock.RUnlock()

	return GenerateShardID(sd.SID, sd.ISN)
}

func (sd *Shard) RemoveReplicates(addrs []string) {
	sd.lock.Lock()
	defer sd.lock.Unlock()

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

func (sd *Shard) ChangeLeader(addr string) {
	sd.lock.Lock()
	defer sd.lock.Unlock()

	if sd.Leader != addr {
		sd.Leader = addr
		sd.LeaderChangeTs = time.Now()
	}
}

func (sd *Shard) Copy() *Shard {
	sd.lock.RLock()
	defer sd.lock.RUnlock()

	var ret Shard
	copier.Copy(&ret, sd)

	return &ret
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

func (sm *ShardManager) CreateNewShard(storageID StorageID) (*Shard, error) {
	stm := GetStorageManager()
	shardISN, err := stm.FetchAddStorageLastISN(storageID)
	if err != nil {
		return nil, err
	}

	return sm.CreateNewShardByIDs(storageID, shardISN)
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
	err := kvPutShard(shard)
	if err != nil {
		return nil, err
	}

	sm.shardsCache.Add(shard.ID(), shard)

	return shard, nil
}

func (sm *ShardManager) GetShard(shardID ShardID) (*Shard, error) {
	v, ok := sm.shardsCache.Get(shardID)
	if ok {
		return v.(*Shard), nil
	}

	shard, err := kvGetShard(shardID)
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
	err := kvPutShard(shard)
	if err != nil {
		return err
	}

	sm.updateCache(shard)

	return nil
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

	err = kvDeleteShard(shard)
	if err != nil {
		return err
	}

	sm.shardsCache.Remove(shardID)

	return nil
}

func (sm *ShardManager) MarkDelete(shardID ShardID) error {
	shard, err := sm.GetShard(shardID)
	if err != nil {
		return nil
	}

	shard.markDelete()

	return sm.PutShard(shard)
}

func (sm *ShardManager) MarkUndelete(shardID ShardID) error {
	shard, err := sm.GetShard(shardID)
	if err != nil {
		return nil
	}

	shard.markUndelete()

	return sm.PutShard(shard)
}

func (sm *ShardManager) updateCache(shard *Shard) {
	_ = sm.shardsCache.Add(shard.ID, shard)
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

func (sm *ShardManager) ReSelectLeader(shardID ShardID, ops ...shardOpFunc) error {
	shard, err := sm.GetShardCopy(shardID)
	if err != nil {
		return err
	}

	var opts shardOption
	for _, opf := range ops {
		opf(&opts)
	}

	newLeader := opts.leaderAddr
	if newLeader == "" {
		var candidates []string
		for addr := range shard.Replicates {
			if addr != shard.Leader {
				candidates = append(candidates, addr)
			}
		}

		if len(candidates) == 0 {
			return errors.New("no enough candidates")
		}

		newLeader = candidates[rand.Intn(len(candidates))]
	}

	conns := dataserver.GetDataServerConns()
	dataSerCli, err := conns.GetApiClient(shard.Leader)
	if err != nil {
		return nil
	}

	replicates := []string{}
	for addr := range shard.Replicates {
		replicates = append(replicates, addr)
	}
	err = dataSerCli.TransferShardLeader(uint64(shard.ID()), replicates)
	if err != nil {
		return err
	}

	shard, err = sm.GetShard(shardID)
	if err != nil {
		return nil
	}

	shard.ChangeLeader(newLeader)
	sm.updateCache(shard)

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

func RemoveShardInDataServer(addr string, id ShardID) error {
	key := ShardInDataServerKey(addr, id)
	ec := kv.GetEtcdClient()

	_, err := ec.Delete(context.Background(), key)
	if err != nil {
		return err
	}

	return nil
}

func GetShardIDsInDataServer(addr string) ([]ShardID, error) {
	return kvGetShardIDsInDataServer(addr)
}

package manager

import (
	"errors"
	"math/rand"
	"sync"
	"time"

	cache "github.com/hashicorp/golang-lru/v2"
	log "github.com/sirupsen/logrus"

	"sr.ht/moyanhao/bedrock-metaserver/dal"
	"sr.ht/moyanhao/bedrock-metaserver/dataserver"
	"sr.ht/moyanhao/bedrock-metaserver/model"
)

type ShardManager struct {
	shardsCache *cache.Cache[model.ShardID, *model.Shard]
}

func NewShardManager() *ShardManager {
	c, err := cache.New[model.ShardID, *model.Shard](102400)
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

func (sm *ShardManager) updateCache(shard *model.Shard) {
	_ = sm.shardsCache.Add(shard.ID(), shard)
}

func (sm *ShardManager) ClearCache() {
	sm.shardsCache.Purge()
}

func (sm *ShardManager) CreateNewShard(storageID model.StorageID) (*model.Shard, error) {
	stm := GetStorageManager()
	shardISN, err := stm.FetchAddStorageLastISN(storageID)
	if err != nil {
		return nil, err
	}

	return sm.CreateNewShardByIDs(storageID, shardISN)
}

func (sm *ShardManager) CreateNewShardByIDs(storageID model.StorageID, shardISN model.ShardISN) (*model.Shard, error) {
	shard := &model.Shard{
		ISN:             shardISN,
		SID:             storageID,
		CreateTs:        time.Now(),
		IsDeleted:       false,
		ReplicaUpdateTs: time.Now(),
		Replicates:      map[string]struct{}{},
	}

	err := dal.KvPutShard(shard)
	if err != nil {
		return nil, err
	}

	sm.shardsCache.Add(shard.ID(), shard)

	return shard, nil
}

func (sm *ShardManager) GetShard(shardID model.ShardID) (*model.Shard, error) {
	v, ok := sm.shardsCache.Get(shardID)
	if ok {
		return v, nil
	}

	shard, err := dal.KvGetShard(shardID)
	if err != nil {
		return nil, err
	}

	_ = sm.shardsCache.Add(shardID, shard)

	return shard.Copy(), nil
}

func (sm *ShardManager) PutShard(shard *model.Shard) error {
	return sm.putShard(shard)
}

func (sm *ShardManager) putShard(shard *model.Shard) error {
	err := dal.KvPutShard(shard)
	if err != nil {
		return err
	}

	sm.updateCache(shard)

	return nil
}

func (sm *ShardManager) ShardDelete(shardID model.ShardID) error {
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

	err = dal.KvDeleteShard(shard)
	if err != nil {
		return err
	}

	sm.shardsCache.Remove(shardID)

	return nil
}

func (sm *ShardManager) MarkDelete(shardID model.ShardID) error {
	shard, err := sm.GetShard(shardID)
	if err != nil {
		return nil
	}

	shard.IsDeleted = true
	shard.DeleteTs = time.Now()

	return sm.putShard(shard)
}

func (sm *ShardManager) MarkUndelete(shardID model.ShardID) error {
	shard, err := sm.GetShard(shardID)
	if err != nil {
		return nil
	}

	shard.IsDeleted = false
	shard.DeleteTs = time.Time{}

	return sm.putShard(shard)
}

func (sm *ShardManager) AddShardReplicates(shardID model.ShardID, addrs []string) error {
	shard, err := sm.GetShard(shardID)
	if err != nil {
		return err
	}

	for _, addr := range addrs {
		shard.Replicates[addr] = struct{}{}
	}

	for _, addr := range addrs {
		if err := dal.KvAddShardInDataServer(addr, shardID); err != nil {
			return err
		}
	}

	return sm.putShard(shard)
}

func (sm *ShardManager) RemoveShardReplicates(shardID model.ShardID, addrs []string) error {
	shard, err := sm.GetShard(shardID)
	if err != nil {
		return err
	}

	for _, addr := range addrs {
		delete(shard.Replicates, addr)
	}

	for _, addr := range addrs {
		if err := dal.KvRemoveShardInDataServer(addr, shardID); err != nil {
			return err
		}
	}

	return sm.putShard(shard)
}

func (sm *ShardManager) RemoveShardRangeByKey(storageID model.StorageID, key []byte) error {
	return dal.KvRemoveShardRangeByKey(storageID, key)
}

func (sm *ShardManager) GetShardIDByKey(storageID model.StorageID, key []byte) (model.ShardID, error) {
	return dal.KvGetShardIDByKey(storageID, key)
}

func (sm *ShardManager) PutShardIDByKey(storageID model.StorageID, key []byte, shardID model.ShardID) error {
	return dal.KvPutShardIDByKey(storageID, key, shardID)
}

func (sm *ShardManager) GetShardIDsInDataServer(addr string) ([]model.ShardID, error) {
	return dal.KvGetShardIDsInDataServer(addr)
}

func (sm *ShardManager) UpdateShardInDataServer(addr string, id model.ShardID, ts int64) error {
	return dal.KvPutShardInDataServer(addr, id, ts)
}

func (sm *ShardManager) GetShardIDsInStorage(storageID model.StorageID) ([]model.ShardID, error) {
	return dal.KvGetShardsInStorage(storageID)
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

func (sm *ShardManager) ReSelectLeader(shardID model.ShardID, ops ...shardOpFunc) error {
	shard, err := sm.GetShard(shardID)
	if err != nil {
		log.Error("%v", err)
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
	log.Info("new shard leader: %v", newLeader)

	conns := dataserver.GetDataServerConns()
	dataSerCli, err := conns.GetApiClient(newLeader)
	if err != nil {
		log.Error("failed to get connection, err: %v", err)
		return nil
	}

	var replicates []string
	for addr := range shard.Replicates {
		if addr != newLeader {
			replicates = append(replicates, addr)
		}
	}
	err = dataSerCli.TransferShardLeader(uint64(shard.ID()), replicates)
	if err != nil {
		log.Error("failed to transfer leader, err: %v", err)
		return err
	}

	shard, err = sm.GetShard(shardID)
	if err != nil {
		log.Error("failed to get shard, err: %v", err)
		return nil
	}

	shard.ChangeLeader(newLeader)

	return dal.KvPutShard(shard)
}

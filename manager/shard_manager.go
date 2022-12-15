package manager

import (
	"errors"
	"math/rand"
	"sync"
	"time"

	cache "github.com/hashicorp/golang-lru"

	"sr.ht/moyanhao/bedrock-metaserver/dal/dao"
	"sr.ht/moyanhao/bedrock-metaserver/dataserver"
	"sr.ht/moyanhao/bedrock-metaserver/model"
	"sr.ht/moyanhao/bedrock-metaserver/utils/log"
)

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

	err := dao.KvPutShard(shard)
	if err != nil {
		return nil, err
	}

	sm.shardsCache.Add(shard.ID(), shard)

	return shard, nil
}

func (sm *ShardManager) GetShard(shardID model.ShardID) (*model.Shard, error) {
	v, ok := sm.shardsCache.Get(shardID)
	if ok {
		return v.(*model.Shard), nil
	}

	shard, err := dao.KvGetShard(shardID)
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
	err := dao.KvPutShard(shard)
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

	err = dao.KvDeleteShard(shard)
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
		if err := dao.KvAddShardInDataServer(addr, shardID); err != nil {
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
		if err := dao.KvRemoveShardInDataServer(addr, shardID); err != nil {
			return err
		}
	}

	return sm.putShard(shard)
}

func (sm *ShardManager) RmoveShardRnageByKey(storageID model.StorageID, key []byte) error {
	return dao.KvRmoveShardRangeByKey(storageID, key)
}

func (sm *ShardManager) GetShardIDByKey(storageID model.StorageID, key []byte) (model.ShardID, error) {
	return dao.KvGetShardIDByKey(storageID, key)
}

func (sm *ShardManager) PutShardIDByKey(storageID model.StorageID, key []byte, shardID model.ShardID) error {
	return dao.KvPutShardIDByKey(storageID, key, shardID)
}

func (sm *ShardManager) GetShardIDsInDataServer(addr string) ([]model.ShardID, error) {
	return dao.KvGetShardIDsInDataServer(addr)
}

func (sm *ShardManager) UpdateShardInDataServer(addr string, id model.ShardID, ts int64) error {
	return dao.KvPutShardInDataServer(addr, id, ts)
}

func (sm *ShardManager) GetShardIDsInStorage(storageID model.StorageID) ([]model.ShardID, error) {
	return dao.KvGetShardsInStorage(storageID)
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

	replicates := []string{}
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

	return dao.KvPutShard(shard)
}

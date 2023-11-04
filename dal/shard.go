package dal

import (
	"context"
	"errors"
	"fmt"
	"strconv"

	log "github.com/sirupsen/logrus"
	client "go.etcd.io/etcd/client/v3"

	"sr.ht/moyanhao/bedrock-metaserver/kv_engine"
	"sr.ht/moyanhao/bedrock-metaserver/model"
)

var (
	ErrNoSuchShard = errors.New("no such shard")
)

const (
	KvPrefixShard               = "/shards/"
	KvPrefixShardsInDataServer  = "/shards_in_dataserver/"
	KvPrefixShardsInStorage     = "/shards_in_storage/"
	KvPrefixShardRangeByStorage = "/shard_range_by_storage/"
)

func shardKey(shardID model.ShardID) string {
	return fmt.Sprintf("%s0x%016x", KvPrefixShard, shardID)
}

func shardInDataServerPrefixKey(addr string) string {
	return fmt.Sprintf("%s%s/", KvPrefixShardsInDataServer, addr)
}

func shardInStoragePrefixKey(storageID model.StorageID) string {
	return fmt.Sprintf("%s0x%08x/", KvPrefixShardsInStorage, storageID)
}

func shardInDataServerKey(addr string, shardID model.ShardID) string {
	return fmt.Sprintf("%s0x%016x", shardInDataServerPrefixKey(addr), shardID)
}

func shardInStorageKey(storageID model.StorageID, shardID model.ShardISN) string {
	return fmt.Sprintf("%s0x%08x", shardInStoragePrefixKey(storageID), shardID)
}

func shardRangeInStoragePrefix(storageID model.StorageID) string {
	return fmt.Sprintf("%s0x%08x/", KvPrefixShardRangeByStorage, storageID)
}

func shardRangeInStorageKey(storageID model.StorageID, key []byte) string {
	return fmt.Sprintf("%s0x%08x/%s", KvPrefixShardRangeByStorage, storageID, string(key))
}

func KvGetShard(shardID model.ShardID) (*model.Shard, error) {
	resp, err := kv_engine.GetEtcdClient().KV.Get(context.Background(), shardKey(shardID))
	if err != nil || resp.Count == 0 {
		return nil, ErrNoSuchShard
	}

	if resp.Count != 1 {
		return nil, errors.New("")
	}

	var shard model.Shard
	for _, item := range resp.Kvs {
		err := shard.UnmarshalJSON(item.Value)
		if err != nil {
			return nil, err
		}
	}

	return &shard, nil
}

func KvPutShard(shard *model.Shard) error {
	value, err := shard.MarshalJSON()
	if err != nil {
		log.Warnf("failed to encode shard to pb, shard=%v", shard)
		return err
	}

	keys := []string{shardKey(shard.ID()), shardInStorageKey(shard.SID, shard.ISN)}
	values := []string{string(value), ""}

	for addr := range shard.Replicates {
		keys = append(keys, shardInDataServerKey(addr, shard.ID()))
		values = append(values, "")
	}

	var ops []client.Op
	for i := range keys {
		ops = append(ops, client.OpPut(keys[i], values[i]))
	}

	_, err = kv_engine.GetEtcdClient().Txn(context.Background()).If().Then(ops...).Commit()
	if err != nil {
		log.Warnf("failed to store shard to etcd, shard=%v", shard)
		return err

	}
	return nil
}

func KvDeleteShard(shard *model.Shard) error {
	keys := []string{shardKey(shard.ID())}

	for addr := range shard.Replicates {
		keys = append(keys, shardInDataServerKey(addr, shard.ID()))
	}

	var ops []client.Op
	for _, key := range keys {
		ops = append(ops, client.OpDelete(key))
	}

	_, err := kv_engine.GetEtcdClient().Txn(context.Background()).If().Then(ops...).Commit()
	if err != nil {
		return err
	}

	return nil
}

func KvGetShardsInStorage(storageID model.StorageID) ([]model.ShardID, error) {
	resp, err := kv_engine.GetEtcdClient().Get(context.Background(), shardInStoragePrefixKey(storageID), client.WithPrefix())
	if err != nil {
		return nil, err
	}

	var shardIDs []model.ShardID
	for _, kv := range resp.Kvs {
		key := string(kv.Key)

		var id uint32
		keyTemplate := fmt.Sprintf("%s0x%08x/0x%%08x", KvPrefixShardsInStorage, storageID)
		_, _ = fmt.Sscanf(keyTemplate, key, &id)

		shardIDs = append(shardIDs, model.GenerateShardID(storageID, model.ShardISN(id)))
	}

	return shardIDs, nil
}

// FIXME: consider support specify count
func KvGetShardIDsInDataServer(addr string) ([]model.ShardID, error) {
	resp, err := kv_engine.GetEtcdClient().Get(context.Background(), shardInDataServerPrefixKey(addr), client.WithPrefix())
	if err != nil {
		return nil, err
	}

	var shardIDs []model.ShardID
	for _, kv := range resp.Kvs {
		key := string(kv.Key)

		var id uint64
		keyTemplate := fmt.Sprintf("%s%s/0x%%016x", KvPrefixShardsInDataServer, addr)
		_, _ = fmt.Sscanf(keyTemplate, key, &id)

		shardIDs = append(shardIDs, model.ShardID(id))
	}

	return shardIDs, nil
}

func KvPutShardInDataServer(addr string, id model.ShardID, ts int64) error {
	key := shardInDataServerKey(addr, id)

	_, err := kv_engine.GetEtcdClient().Put(context.Background(), key, strconv.FormatInt(ts, 10))
	if err != nil {
		return err
	}

	return nil
}

// zrange
func KvGetShardIDByKey(storageID model.StorageID, key []byte) (model.ShardID, []byte /* range start */, error) {
	resp, err := kv_engine.GetEtcdClient().Get(context.Background(), shardRangeInStorageKey(storageID, key), client.WithPrefix())
	if err != nil {
		return 0, []byte{}, err
	}

	if resp.Count == 0 {
		return 0, []byte{}, errors.New("no such key")
	}

	kv := resp.Kvs[0]

	var shardID model.ShardID
	_, _ = fmt.Sscanf("0x%016x", string(kv.Value), &shardID)
	var rangeStart string
	_, _ = fmt.Sscanf(string(kv.Key), shardRangeInStoragePrefix(storageID)+"/%s", &rangeStart)

	return shardID, []byte(rangeStart), nil
}

// zadd
func KvPutShardIDByKey(storageID model.StorageID, key []byte, shardID model.ShardID) error {
	value := fmt.Sprintf("0x%016x", shardID)

	_, err := kv_engine.GetEtcdClient().Put(context.Background(), shardRangeInStorageKey(storageID, key), value)
	if err != nil {
		return err
	}

	return nil
}

// zrembyscore
func KvRemoveShardRangeByKey(storageID model.StorageID, key []byte) error {
	_, err := kv_engine.GetEtcdClient().Delete(context.Background(), shardRangeInStorageKey(storageID, key))
	return err
}

type ShardRange struct {
	ShardID    model.ShardID
	RangeStart []byte
	RangeEnd   []byte
	LeaderAddr string
	Addrs      []string
}

func KvScanShardsBySID(storageID model.StorageID, rangeStart []byte) ([]ShardRange, error) {
	keyPrefix := shardRangeInStorageKey(storageID, rangeStart)
	resp, err := kv_engine.GetEtcdClient().Get(context.Background(), keyPrefix, client.WithPrefix(), client.WithLimit(100))
	if err != nil {
		log.Errorf("get key with prefix failed, err: %v", err)
		return nil, err
	}

	var ret []ShardRange
	for _, kv := range resp.Kvs {
		var shardID uint64
		_, _ = fmt.Sscanf(string(kv.Value), "0x%016x", &shardID)

		shard, err :=
			KvGetShard(model.ShardID(shardID))
		if err != nil {
			return nil, err
		}

		var addrs []string
		for rep := range shard.Replicates {
			addrs = append(addrs, rep)
		}

		ret = append(ret, ShardRange{
			ShardID:    model.ShardID(shardID),
			RangeStart: []byte(shard.RangeKeyStart),
			RangeEnd:   []byte(shard.RangeKeyEnd),
			LeaderAddr: shard.Leader,
			Addrs:      addrs,
		})
	}

	return ret, nil
}

func KvAddShardInDataServer(addr string, id model.ShardID) error {
	key := shardInDataServerKey(addr, id)

	_, err := kv_engine.GetEtcdClient().Put(context.Background(), key, "0")
	if err != nil {
		return err
	}

	return nil
}

func KvRemoveShardInDataServer(addr string, id model.ShardID) error {
	key := shardInDataServerKey(addr, id)

	_, err := kv_engine.GetEtcdClient().Delete(context.Background(), key)
	if err != nil {
		return err
	}

	return nil
}

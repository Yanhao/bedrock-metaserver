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
	ErrNoSuchShard = errors.New("")
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

func shardRangeInStorageKey(storageID model.StorageID, key []byte) string {
	return fmt.Sprintf("%s0x%08x/%s", KvPrefixShardRangeByStorage, storageID, string(key))
}

func KvGetShard(shardID model.ShardID) (*model.Shard, error) {
	ec := kv_engine.GetEtcdClient()

	resp, err := ec.KV.Get(context.Background(), shardKey(shardID))
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
		log.Warn("failed to encode shard to pb, shard=%v", shard)
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

	ec := kv_engine.GetEtcdClient()
	_, err = ec.Txn(context.Background()).If().Then(ops...).Commit()
	if err != nil {
		log.Warn("failed to store shard to etcd, shard=%v", shard)
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

	ec := kv_engine.GetEtcdClient()
	_, err := ec.Txn(context.Background()).If().Then(ops...).Commit()
	if err != nil {
		return err
	}

	return nil
}

func KvGetShardsInStorage(storageID model.StorageID) ([]model.ShardID, error) {
	ec := kv_engine.GetEtcdClient()

	resp, err := ec.Get(context.Background(), shardInStoragePrefixKey(storageID), client.WithPrefix())
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
	ec := kv_engine.GetEtcdClient()

	resp, err := ec.Get(context.Background(), shardInDataServerPrefixKey(addr), client.WithPrefix())
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
	ec := kv_engine.GetEtcdClient()

	_, err := ec.Put(context.Background(), key, strconv.FormatInt(ts, 10))
	if err != nil {
		return err
	}

	return nil
}

func KvGetShardIDByKey(storageID model.StorageID, key []byte) (model.ShardID, error) {
	ec := kv_engine.GetEtcdClient()

	resp, err := ec.Get(context.Background(), shardRangeInStorageKey(storageID, key), client.WithPrefix())
	if err != nil {
		return 0, err
	}

	if resp.Count == 0 {
		return 0, errors.New("no such key")
	}

	kv := resp.Kvs[0]

	var shardID model.ShardID
	_, _ = fmt.Sscanf("0x%016x", string(kv.Value), &shardID)

	return shardID, nil
}

func KvPutShardIDByKey(storageID model.StorageID, key []byte, shardID model.ShardID) error {
	value := fmt.Sprintf("0x%016x", shardID)

	ec := kv_engine.GetEtcdClient()

	_, err := ec.Put(context.Background(), shardRangeInStorageKey(storageID, key), value)
	if err != nil {
		return err
	}

	return nil
}

func KvRemoveShardRangeByKey(storageID model.StorageID, key []byte) error {
	ec := kv_engine.GetEtcdClient()
	_, err := ec.Delete(context.Background(), shardRangeInStorageKey(storageID, key))
	return err
}

func KvAddShardInDataServer(addr string, id model.ShardID) error {
	ec := kv_engine.GetEtcdClient()
	key := shardInDataServerKey(addr, id)

	_, err := ec.Put(context.Background(), key, "0")
	if err != nil {
		return err
	}

	return nil
}

func KvRemoveShardInDataServer(addr string, id model.ShardID) error {
	ec := kv_engine.GetEtcdClient()
	key := shardInDataServerKey(addr, id)

	_, err := ec.Delete(context.Background(), key)
	if err != nil {
		return err
	}

	return nil
}

package dao

import (
	"context"
	"errors"
	"fmt"
	"strconv"

	client "go.etcd.io/etcd/client/v3"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/timestamppb"

	"sr.ht/moyanhao/bedrock-metaserver/dal/dto"
	"sr.ht/moyanhao/bedrock-metaserver/kvengine"
	"sr.ht/moyanhao/bedrock-metaserver/model"
	"sr.ht/moyanhao/bedrock-metaserver/utils/log"
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
	ec := kvengine.GetEtcdClient()

	resp, err := ec.KV.Get(context.Background(), shardKey(shardID))
	if err != nil || resp.Count == 0 {
		return nil, ErrNoSuchShard
	}

	if resp.Count != 1 {
		return nil, errors.New("")
	}

	pbShard := &dto.Shard{}
	for _, item := range resp.Kvs {
		err := proto.Unmarshal(item.Value, pbShard)
		if err != nil {
			return nil, err
		}
	}

	shard := &model.Shard{}
	shard.ISN = model.ShardISN(pbShard.Isn)
	for _, rep := range pbShard.Replicates {
		shard.Replicates[rep] = struct{}{}
	}
	shard.ReplicaUpdateTs = pbShard.ReplicaUpdateTs.AsTime()
	shard.Leader = pbShard.Leader
	shard.RangeKeyMax = pbShard.RangeKeyMax
	shard.RangeKeyMin = pbShard.RangeKeyMin

	return shard, nil
}

func KvPutShard(shard *model.Shard) error {
	pbShard := &dto.Shard{
		Isn:             uint32(shard.ISN),
		ReplicaUpdateTs: timestamppb.New(shard.ReplicaUpdateTs),
		Leader:          shard.Leader,
		RangeKeyMax:     shard.RangeKeyMax,
		RangeKeyMin:     shard.RangeKeyMin,
	}
	for _, rep := range pbShard.Replicates {
		shard.Replicates[rep] = struct{}{}
	}

	value, err := proto.Marshal(pbShard)
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

	ec := kvengine.GetEtcdClient()
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

	ec := kvengine.GetEtcdClient()
	_, err := ec.Txn(context.Background()).If().Then(ops...).Commit()
	if err != nil {
		return err
	}

	return nil
}

func KvGetShardsInStorage(storageID model.StorageID) ([]model.ShardID, error) {
	ec := kvengine.GetEtcdClient()

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
	ec := kvengine.GetEtcdClient()

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
	ec := kvengine.GetEtcdClient()

	_, err := ec.Put(context.Background(), key, strconv.FormatInt(ts, 10))
	if err != nil {
		return err
	}

	return nil
}

func KvGetShardIDByKey(storageID model.StorageID, key []byte) (model.ShardID, error) {
	ec := kvengine.GetEtcdClient()

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

	ec := kvengine.GetEtcdClient()

	_, err := ec.Put(context.Background(), shardRangeInStorageKey(storageID, key), value)
	if err != nil {
		return err
	}

	return nil
}

func KvRmoveShardRangeByKey(storageID model.StorageID, key []byte) error {
	ec := kvengine.GetEtcdClient()
	_, err := ec.Delete(context.Background(), string(key))
	return err
}

func KvAddShardInDataServer(addr string, id model.ShardID) error {
	ec := kvengine.GetEtcdClient()
	key := shardInDataServerKey(addr, id)

	_, err := ec.Put(context.Background(), key, "0")
	if err != nil {
		return err
	}

	return nil
}

func KvRemoveShardInDataServer(addr string, id model.ShardID) error {
	ec := kvengine.GetEtcdClient()
	key := shardInDataServerKey(addr, id)

	_, err := ec.Delete(context.Background(), key)
	if err != nil {
		return err
	}

	return nil
}

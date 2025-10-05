package dal

import (
	"context"
	"fmt"
	"strconv"

	log "github.com/sirupsen/logrus"
	client "go.etcd.io/etcd/client/v3"

	"sr.ht/moyanhao/bedrock-metaserver/errors"
	"sr.ht/moyanhao/bedrock-metaserver/meta_store"
	"sr.ht/moyanhao/bedrock-metaserver/model"
)

var (
	ErrNoSuchShard = errors.ErrNoSuchShard
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
	etcdClient, err := meta_store.GetEtcdClient()
	if err != nil {
		return nil, errors.Wrap(err, errors.ErrCodeSystem, "failed to get etcd client")
	}
	resp, err := etcdClient.KV.Get(context.Background(), shardKey(shardID))
	if err != nil {
		return nil, errors.Wrapf(err, errors.ErrCodeDatabase, "failed to get shard from etcd, shardID=0x%016x", shardID)
	}
	if resp.Count == 0 {
		return nil, ErrNoSuchShard
	}

	if resp.Count != 1 {
		log.Errorf("Multiple shard entries found for shardID=0x%016x, count=%d", shardID, resp.Count)
		return nil, errors.New(errors.ErrCodeDatabase, "multiple shard entries found")
	}

	var shard model.Shard
	for _, item := range resp.Kvs {
		err := shard.UnmarshalJSON(item.Value)
		if err != nil {
			return nil, errors.Wrapf(err, errors.ErrCodeInternal, "failed to unmarshal shard data, shardID=0x%016x", shardID)
		}
	}

	return &shard, nil
}

func KvPutShard(shard *model.Shard) error {
	value, err := shard.MarshalJSON()
	if err != nil {
		log.Warnf("failed to encode shard to JSON, shard=%v, err=%v", shard, err)
		return errors.Wrap(err, errors.ErrCodeInternal, "failed to encode shard data")
	}

	keys := []string{shardKey(shard.ID())}
	values := []string{string(value)}

	for addr := range shard.Replicates {
		keys = append(keys, shardInDataServerKey(addr, shard.ID()))
		values = append(values, "")
	}

	var ops []client.Op
	for i := range keys {
		ops = append(ops, client.OpPut(keys[i], values[i]))
	}

	etcdClient, err := meta_store.GetEtcdClient()
	if err != nil {
		return errors.Wrap(err, errors.ErrCodeSystem, "failed to get etcd client")
	}
	_, err = etcdClient.Txn(context.Background()).If().Then(ops...).Commit()
	if err != nil {
		log.Warnf("failed to store shard to etcd, shard=%v, err=%v", shard, err)
		return errors.Wrap(err, errors.ErrCodeDatabase, "failed to store shard to etcd")
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

	etcdClient, err := meta_store.GetEtcdClient()
	if err != nil {
		return errors.Wrap(err, errors.ErrCodeSystem, "failed to get etcd client")
	}
	_, err = etcdClient.Txn(context.Background()).If().Then(ops...).Commit()
	if err != nil {
		log.Errorf("failed to delete shard from etcd, shardID=0x%016x, err=%v", shard.ID(), err)
		return errors.Wrap(err, errors.ErrCodeDatabase, "failed to delete shard from etcd")
	}

	return nil
}

func KvGetShardsInStorage(storageID model.StorageID) ([]model.ShardID, error) {
	etcdClient, err := meta_store.GetEtcdClient()
	if err != nil {
		return nil, errors.Wrap(err, errors.ErrCodeSystem, "failed to get etcd client")
	}
	resp, err := etcdClient.Get(context.Background(), shardInStoragePrefixKey(storageID), client.WithPrefix())
	if err != nil {
		log.Errorf("failed to get shards in storage, storageID=0x%08x, err=%v", storageID, err)
		return nil, errors.Wrap(err, errors.ErrCodeDatabase, "failed to get shards in storage")
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
	etcdClient, err := meta_store.GetEtcdClient()
	if err != nil {
		return nil, errors.Wrap(err, errors.ErrCodeSystem, "failed to get etcd client")
	}
	resp, err := etcdClient.Get(context.Background(), shardInDataServerPrefixKey(addr), client.WithPrefix())
	if err != nil {
		log.Errorf("failed to get shard IDs in data server, addr=%s, err=%v", addr, err)
		return nil, errors.Wrap(err, errors.ErrCodeDatabase, "failed to get shard IDs in data server")
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

	etcdClient, err := meta_store.GetEtcdClient()
	if err != nil {
		return errors.Wrap(err, errors.ErrCodeSystem, "failed to get etcd client")
	}
	_, err = etcdClient.Put(context.Background(), key, strconv.FormatInt(ts, 10))
	if err != nil {
		log.Errorf("failed to put shard in data server, addr=%s, shardID=0x%016x, err=%v", addr, id, err)
		return errors.Wrap(err, errors.ErrCodeDatabase, "failed to put shard in data server")
	}

	return nil
}

// zrange
func KvGetShardIDByKey(storageID model.StorageID, key []byte) (model.ShardID, []byte /* range start */, error) {
	etcdClient, err := meta_store.GetEtcdClient()
	if err != nil {
		return 0, []byte{}, errors.Wrap(err, errors.ErrCodeSystem, "failed to get etcd client")
	}
	resp, err := etcdClient.Get(context.Background(), shardRangeInStorageKey(storageID, key), client.WithPrefix())
	if err != nil {
		log.Errorf("failed to get shard ID by key, storageID=0x%08x, key=%s, err=%v", storageID, string(key), err)
		return 0, []byte{}, errors.Wrap(err, errors.ErrCodeDatabase, "failed to get shard ID by key")
	}

	if resp.Count == 0 {
		return 0, []byte{}, errors.New(errors.ErrCodeNotFound, "no such key")
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

	etcdClient, err := meta_store.GetEtcdClient()
	if err != nil {
		return errors.Wrap(err, errors.ErrCodeSystem, "failed to get etcd client")
	}
	_, err = etcdClient.Put(context.Background(), shardRangeInStorageKey(storageID, key), value)
	if err != nil {
		log.Errorf("failed to put shard ID by key, storageID=0x%08x, key=%s, shardID=0x%016x, err=%v", storageID, string(key), shardID, err)
		return errors.Wrap(err, errors.ErrCodeDatabase, "failed to put shard ID by key")
	}

	return nil
}

// zrembyscore
func KvRemoveShardRangeByKey(storageID model.StorageID, key []byte) error {
	etcdClient, err := meta_store.GetEtcdClient()
	if err != nil {
		return errors.Wrap(err, errors.ErrCodeSystem, "failed to get etcd client")
	}
	_, err = etcdClient.Delete(context.Background(), shardRangeInStorageKey(storageID, key))
	if err != nil {
		log.Errorf("failed to remove shard range by key, storageID=0x%08x, key=%s, err=%v", storageID, string(key), err)
		return errors.Wrap(err, errors.ErrCodeDatabase, "failed to remove shard range by key")
	}
	return nil
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
	etcdClient, err := meta_store.GetEtcdClient()
	if err != nil {
		return nil, errors.Wrap(err, errors.ErrCodeSystem, "failed to get etcd client")
	}
	resp, err := etcdClient.Get(context.Background(), keyPrefix, client.WithPrefix(), client.WithLimit(100))
	if err != nil {
		log.Errorf("get key with prefix failed, storageID=0x%08x, rangeStart=%s, err: %v", storageID, string(rangeStart), err)
		return nil, errors.Wrap(err, errors.ErrCodeDatabase, "failed to scan shards by SID")
	}

	var ret []ShardRange
	for _, kv := range resp.Kvs {
		var shardID uint64
		_, _ = fmt.Sscanf(string(kv.Value), "0x%016x", &shardID)

		shard, err :=
			KvGetShard(model.ShardID(shardID))
		if err != nil {
			log.Errorf("failed to get shard, shardID=0x%016x, err=%v", shardID, err)
			return nil, errors.Wrap(err, errors.ErrCodeInternal, "failed to get shard during scan")
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

	etcdClient, err := meta_store.GetEtcdClient()
	if err != nil {
		return errors.Wrap(err, errors.ErrCodeSystem, "failed to get etcd client")
	}
	_, err = etcdClient.Put(context.Background(), key, "0")
	if err != nil {
		log.Errorf("failed to add shard in data server, addr=%s, shardID=0x%016x, err=%v", addr, id, err)
		return errors.Wrap(err, errors.ErrCodeDatabase, "failed to add shard in data server")
	}

	return nil
}

func KvRemoveShardInDataServer(addr string, id model.ShardID) error {
	key := shardInDataServerKey(addr, id)

	etcdClient, err := meta_store.GetEtcdClient()
	if err != nil {
		return errors.Wrap(err, errors.ErrCodeSystem, "failed to get etcd client")
	}
	_, err = etcdClient.Delete(context.Background(), key)
	if err != nil {
		log.Errorf("failed to remove shard in data server, addr=%s, shardID=0x%016x, err=%v", addr, id, err)
		return errors.Wrap(err, errors.ErrCodeDatabase, "failed to remove shard in data server")
	}

	return nil
}

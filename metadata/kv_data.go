package metadata

import (
	"context"
	"errors"
	"fmt"
	"net"
	"strconv"
	"time"

	client "go.etcd.io/etcd/client/v3"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/timestamppb"

	"sr.ht/moyanhao/bedrock-metaserver/common/log"
	"sr.ht/moyanhao/bedrock-metaserver/kv"
	"sr.ht/moyanhao/bedrock-metaserver/metadata/pbdata"
)

var (
	ErrNoSuchShard = errors.New("no such shard")
)

// ==================== shard dao =========================================

func kvGetShard(shardID ShardID) (*Shard, error) {
	ec := kv.GetEtcdClient()

	resp, err := ec.KV.Get(context.Background(), ShardKey(shardID))
	if err != nil || resp.Count == 0 {
		return nil, ErrNoSuchShard
	}

	if resp.Count != 1 {
		return nil, errors.New("")
	}

	pbShard := &pbdata.Shard{}
	for _, item := range resp.Kvs {
		err := proto.Unmarshal(item.Value, pbShard)
		if err != nil {
			return nil, err
		}
	}

	shard := &Shard{}
	shard.ISN = ShardISN(pbShard.Isn)
	for _, rep := range pbShard.Replicates {
		shard.Replicates[rep] = struct{}{}
	}
	shard.ReplicaUpdateTs = pbShard.ReplicaUpdateTs.AsTime()
	shard.Leader = pbShard.Leader
	shard.RangeKeyMax = pbShard.RangeKeyMax
	shard.RangeKeyMin = pbShard.RangeKeyMin

	return shard, nil
}

func kvPutShard(shard *Shard) error {
	pbShard := &pbdata.Shard{
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

	keys := []string{ShardKey(shard.ID()), ShardInStorageKey(shard.SID, shard.ISN)}
	values := []string{string(value), ""}

	for addr := range shard.Replicates {
		keys = append(keys, ShardInDataServerKey(addr, shard.ID()))
		values = append(values, "")
	}

	var ops []client.Op
	for i := range keys {
		ops = append(ops, client.OpPut(keys[i], values[i]))
	}

	ec := kv.GetEtcdClient()
	_, err = ec.Txn(context.Background()).If().Then(ops...).Commit()
	if err != nil {
		log.Warn("failed to store shard to etcd, shard=%v", shard)
		return err

	}
	return nil
}

func kvDeleteShard(shard *Shard) error {
	keys := []string{ShardKey(shard.ID())}

	for addr := range shard.Replicates {
		keys = append(keys, ShardInDataServerKey(addr, shard.ID()))
	}

	var ops []client.Op
	for _, key := range keys {
		ops = append(ops, client.OpDelete(key))
	}

	ec := kv.GetEtcdClient()
	_, err := ec.Txn(context.Background()).If().Then(ops...).Commit()
	if err != nil {
		return err
	}

	return nil
}

// ==================== dataserver dao =========================================

func kvGetDataServer(addr string) (*DataServer, error) {
	ec := kv.GetEtcdClient()
	resp, err := ec.KV.Get(context.Background(), DataServerKey(addr))
	if err != nil || resp.Count == 0 {
		return nil, ErrNoSuchShard
	}

	if resp.Count != 1 {
		return nil, errors.New("")
	}

	pbDataServer := &pbdata.DataServer{}
	for _, item := range resp.Kvs {
		err := proto.Unmarshal(item.Value, pbDataServer)
		if err != nil {
			return nil, err
		}
	}

	hostStr, portStr, err := net.SplitHostPort(addr)
	if err != nil {
		log.Warn("parse address failed, err: %v", err)
	}

	dataServer := &DataServer{
		Ip:              hostStr,
		Port:            portStr,
		Free:            pbDataServer.GetFree(),
		Capacity:        pbDataServer.GetCapacity(),
		Status:          LiveStatusActive,
		LastHeartBeatTs: time.Time{},
	}

	return dataServer, nil
}

func kvHasDataServer(addr string) (bool, error) {
	ec := kv.GetEtcdClient()
	resp, err := ec.Get(context.Background(), DataServerKey(addr))
	if err != nil {
		return false, err
	}

	if resp.Count != 0 {
		return true, nil
	}

	return false, nil
}

func kvPutDataServer(dataserver *DataServer) error {
	var status pbdata.DataServer_LiveStatus
	if dataserver.Status == LiveStatusActive {
		status = pbdata.DataServer_ACTIVE
	} else if dataserver.Status == LiveStatusInactive {
		status = pbdata.DataServer_INACTIVE
	} else if dataserver.Status == LiveStatusOffline {
		status = pbdata.DataServer_OFFLINE
	}

	pbDataServer := &pbdata.DataServer{
		Ip:       dataserver.Ip,
		Port:     dataserver.Port,
		Free:     dataserver.Free,
		Capacity: dataserver.Capacity,

		LastHeartbeatTs: timestamppb.New(dataserver.LastHeartBeatTs),

		Status: status,
	}

	value, err := proto.Marshal(pbDataServer)
	if err != nil {
		log.Warn("failed to encode dataserver to pb, dataserver=%v", dataserver)
		return err
	}

	ec := kv.GetEtcdClient()
	_, err = ec.Put(context.Background(), DataServerKey(dataserver.Addr()), string(value))
	if err != nil {
		log.Warn("failed to save dataserver to etcd, dataserver=%v", dataserver)
		return err
	}

	return nil
}

func kvDeleteDataServer(addr string) error {
	ec := kv.GetEtcdClient()
	_, err := ec.Delete(context.Background(), DataServerKey(addr))
	if err != nil {
		log.Warn("failed to delete dataserver from kv")
		return err
	}

	return nil
}

// ==================== storage dao =========================================

func kvGetStorage(storageID StorageID) (*Storage, error) {
	ec := kv.GetEtcdClient()
	resp, err := ec.Get(context.Background(), StorageKey(storageID))
	if err != nil {
		log.Warn("failed get storage from etcd, storageID=%d", storageID)
		return nil, err
	}

	if resp.Count != 1 {
		return nil, fmt.Errorf("expected only one storage , found %d", resp.Count)
	}

	pbStorage := &pbdata.Storage{}
	for _, kv := range resp.Kvs {
		err := proto.Unmarshal(kv.Value, pbStorage)
		if err != nil {
			log.Warn("failed to decode storage")

			return nil, err
		}

		break
	}
	return &Storage{
		ID:           StorageID(pbStorage.Id),
		Name:         pbStorage.Name,
		IsDeleted:    pbStorage.IsDeleted,
		DeleteTs:     pbStorage.DeletedTs.AsTime(),
		CreateTs:     pbStorage.CreateTs.AsTime(),
		RecycleTs:    pbStorage.RecycleTs.AsTime(),
		LastShardISN: ShardISN(pbStorage.LastShardIndex),
	}, nil
}

func kvGetStorageByName(name string) (*Storage, error) {
	ec := kv.GetEtcdClient()
	resp, err := ec.Get(context.Background(), StorageByNameKey(name))
	if err != nil {
		log.Warn("failed get storage id by name, err: %v", err)
		return nil, err
	}

	if resp.Count == 0 {
		return nil, nil
	}
	if resp.Count != 1 {
		return nil, fmt.Errorf("storage by name count is not equals 1, count: %v", resp.Count)
	}
	storageIDStr := resp.Kvs[0].Value
	storageID, err := strconv.ParseUint(string(storageIDStr), 10, 32)
	if err != nil {
		log.Warn("failed to parse storage id, err: %v", err)
		return nil, err
	}

	return kvGetStorage(StorageID(storageID))
}

func kvHasStorage(storageID StorageID) (bool, error) {
	ec := kv.GetEtcdClient()
	resp, err := ec.Get(context.Background(), StorageKey(storageID))
	if err != nil {
		return false, err
	}

	if resp.Count != 0 {
		return true, nil
	}

	return false, nil
}

func kvPutStorage(storage *Storage) error {
	pbStorage := &pbdata.Storage{
		Id:             uint64(storage.ID),
		Name:           storage.Name,
		IsDeleted:      storage.IsDeleted,
		DeletedTs:      timestamppb.New(storage.DeleteTs),
		CreateTs:       timestamppb.New(storage.CreateTs),
		RecycleTs:      timestamppb.New(storage.RecycleTs),
		LastShardIndex: uint32(storage.LastShardISN),
	}

	value, err := proto.Marshal(pbStorage)
	if err != nil {
		log.Warn("failed to encode storage to pb, storage=%v", storage)
		return err
	}

	ec := kv.GetEtcdClient()
	_, err = ec.Put(context.Background(), StorageKey(storage.ID), string(value))
	if err != nil {
		log.Warn("failed to save storage to etcd, storage=%v", storage)
		return err
	}

	storageIDStr := strconv.FormatUint(uint64(storage.ID), 10)
	_, err = ec.Put(context.Background(), StorageByNameKey(storage.Name), storageIDStr)
	if err != nil {
		log.Warn("failed to save storage name, err: %v", err)
		return err
	}

	return nil
}

func kvDeleteStorage(storageID StorageID) error {
	keys := []string{
		StorageKey(storageID),
		ShardInStoragePrefixKey(storageID),
	}

	var ops []client.Op
	for _, key := range keys {
		ops = append(ops, client.OpDelete(key, client.WithPrefix()))
	}

	ec := kv.GetEtcdClient()
	_, err := ec.Txn(context.Background()).If().Then(ops...).Commit()
	if err != nil {
		log.Warn("failed to delete storage from etcd, storage=%d", storageID)
		return err
	}

	return nil
}

// ==================== deleted storage dao ===================================

func kvGetDeletedStorage(limit int) ([]*Storage, error) {
	ec := kv.GetEtcdClient()

	resp, err := ec.Get(context.TODO(),
		KvPrefixMarkDeletedStorageID,
		client.WithPrefix(),
		client.WithLimit(int64(limit)),
	)
	if err != nil {
		log.Warn("failed to get deleted storage, err: %v", err)
		return nil, err
	}

	var ret []*Storage
	for _, kv := range resp.Kvs {
		var sID uint64
		_, err := fmt.Sscanf(string(kv.Key), KvPrefixMarkDeletedStorageID+"/%d", &sID)
		if err != nil {
			log.Warn("failed to parse deleted storage key")
			continue
		}

		s, err := kvGetStorage(StorageID(sID))
		if err != nil {
			log.Warn("failed to get storage, storage id: %d, err: %v", sID, err)
			continue
		}
		ret = append(ret, s)
	}

	return ret, nil
}

func kvPutDeletedStorageID(sID StorageID) error {
	key := DeletedStorageKey(sID)

	ec := kv.GetEtcdClient()
	_, err := ec.Put(context.TODO(), key, "")
	if err != nil {
		return err
	}

	return nil
}

func kvDelDeletedStorageID(sID StorageID) error {
	key := DeletedStorageKey(sID)

	ec := kv.GetEtcdClient()
	_, err := ec.Delete(context.TODO(), key)
	if err != nil {
		return err
	}

	return nil
}

// ===========================================================================

// FIXME: consider support specify count
func kvGetShardIDsInDataServer(addr string) ([]ShardID, error) {
	ec := kv.GetEtcdClient()

	resp, err := ec.Get(context.Background(), ShardInDataServerPrefixKey(addr), client.WithPrefix())
	if err != nil {
		return nil, err
	}

	var shardIDs []ShardID
	for _, kv := range resp.Kvs {
		key := string(kv.Key)

		var id uint64
		keyTemplate := fmt.Sprintf("%s%s/0x%%016x", KvPrefixShardsInDataServer, addr)
		_, _ = fmt.Sscanf(keyTemplate, key, &id)

		shardIDs = append(shardIDs, ShardID(id))
	}

	return shardIDs, nil
}

func kvIsShardInDataServer(addr string, shardID ShardID) (bool, error) {
	ec := kv.GetEtcdClient()
	resp, err := ec.Get(context.Background(), ShardInDataServerKey(addr, shardID), client.WithPrefix())
	if err != nil {
		return false, err
	}

	if resp.Count != 0 {
		return true, nil
	}

	return false, nil
}

func kvGetShardsInStorage(storageID StorageID) ([]ShardID, error) {
	ec := kv.GetEtcdClient()

	resp, err := ec.Get(context.Background(), ShardInStoragePrefixKey(storageID), client.WithPrefix())
	if err != nil {
		return nil, err
	}

	var shardIDs []ShardID
	for _, kv := range resp.Kvs {
		key := string(kv.Key)

		var id uint32
		keyTemplate := fmt.Sprintf("%s0x%08x/0x%%08x", KvPrefixShardsInStorage, storageID)
		_, _ = fmt.Sscanf(keyTemplate, key, &id)

		shardIDs = append(shardIDs, GenerateShardID(storageID, ShardISN(id)))
	}
	return shardIDs, nil
}

func kvGetShardIDByKey(storageID StorageID, key []byte) (ShardID, error) {
	ec := kv.GetEtcdClient()

	resp, err := ec.Get(context.Background(), ShardRangeInStorageKey(storageID, key), client.WithPrefix())
	if err != nil {
		return 0, err
	}

	if resp.Count == 0 {
		return 0, errors.New("no such key")
	}

	kv := resp.Kvs[0]

	var shardID ShardID
	_, _ = fmt.Sscanf("0x%016x", string(kv.Value), &shardID)

	return shardID, nil
}

func kvPutShardIDByKey(storageID StorageID, key []byte, shardID ShardID) error {
	value := fmt.Sprintf("0x%016x", shardID)

	ec := kv.GetEtcdClient()

	_, err := ec.Put(context.Background(), ShardRangeInStorageKey(storageID, key), value)
	if err != nil {
		return err
	}

	return nil
}

func kvRmoveShardRangeByKey(storageID StorageID, key []byte) error {
	ec := kv.GetEtcdClient()
	_, err := ec.Delete(context.Background(), string(key))
	return err
}

func kvGetShardIDs(storageID StorageID) ([]ShardID, error) {
	ec := kv.GetEtcdClient()

	ret := []ShardID{}

	resp, err := ec.Get(context.Background(), ShardRangeInStorageKey(storageID, []byte{}), client.WithPrefix())
	if err != nil {
		return ret, err
	}

	if resp.Count == 0 {
		return ret, errors.New("no such key")
	}

	for _, kv := range resp.Kvs {
		var shardID ShardID
		_, _ = fmt.Sscanf("0x%016x", string(kv.Value), &shardID)
		ret = append(ret, shardID)
	}

	return ret, nil
}

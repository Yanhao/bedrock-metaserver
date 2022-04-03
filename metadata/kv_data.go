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

func getShardFromKv(shardID ShardID) (*Shard, error) {
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
	shard.ID = ShardID(pbShard.Id)
	for _, rep := range pbShard.Replicates {
		shard.Replicates[rep] = struct{}{}
	}
	shard.ReplicaUpdateTs = pbShard.ReplicaUpdateTs.AsTime()

	return shard, nil

}

func putShardToKv(shard *Shard) error {
	pbShard := &pbdata.Shard{
		Id:              uint64(shard.ID),
		ReplicaUpdateTs: timestamppb.New(shard.ReplicaUpdateTs),
	}
	for _, rep := range pbShard.Replicates {
		shard.Replicates[rep] = struct{}{}
	}

	value, err := proto.Marshal(pbShard)
	if err != nil {
		log.Warn("failed to encode shard to pb, shard=%v", *shard)
		return err
	}

	keys := []string{ShardKey(shard.ID), ShardInStorageKey(shard.SID, shard.ID)}
	values := []string{string(value), ""}

	for addr := range shard.Replicates {
		keys = append(keys, ShardInDataServerKey(addr, shard.ID))
		values = append(values, "")
	}

	var ops []client.Op
	for i := range keys {
		ops = append(ops, client.OpPut(keys[i], values[i]))
	}

	ec := kv.GetEtcdClient()
	_, err = ec.Txn(context.Background()).If().Then(ops...).Commit()
	if err != nil {
		log.Warn("failed to store shard to etcd, shard=%v", *shard)
		return err

	}
	return nil
}

func deleteShardFromKv(shard *Shard) error {
	keys := []string{ShardKey(shard.ID)}

	for addr := range shard.Replicates {
		keys = append(keys, ShardInDataServerKey(addr, shard.ID))
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

func getDataServerFromKv(addr string) (*DataServer, error) {
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

	host, _ := strconv.ParseInt(hostStr, 10, 32)
	port, _ := strconv.ParseInt(portStr, 10, 32)

	dataServer := &DataServer{
		Ip:              uint32(host),
		Port:            uint32(port),
		Free:            pbDataServer.GetFree(),
		Capacity:        pbDataServer.GetCapacity(),
		status:          LiveStatusActive,
		LastHeartBeatTs: time.Time{},
	}

	return dataServer, nil
}

func putDataServerToKv(dataserver *DataServer) error {
	pbDataServer := &pbdata.DataServer{
		Ip:   dataserver.Ip,
		Port: dataserver.Port,
	}

	value, err := proto.Marshal(pbDataServer)
	if err != nil {
		log.Warn("failed to encode dataserver to pb, dataserver=%v", dataserver)
		return err
	}

	ec := kv.GetEtcdClient()
	_, err = ec.Put(context.Background(), DataServerKey(dataserver.Addr()), string(value))
	if err != nil {
		log.Warn("failed to save dataserver to etcd, dataserver=%v", *dataserver)
		return err
	}

	return nil
}

func deleteDataServerFromKv(addr string) error {
	ec := kv.GetEtcdClient()
	_, err := ec.Delete(context.Background(), DataServerKey(addr))
	if err != nil {
		log.Warn("failed to delete dataserver from kv")
		return err
	}

	return nil
}

func GetShardsInDataServerInKv(addr string) ([]ShardID, error) {
	ec := kv.GetEtcdClient()

	resp, err := ec.Get(context.Background(), ShardInDataServerPrefixKey(addr), client.WithPrefix())
	if err != nil {
		return nil, err
	}

	var shardIDs []ShardID
	for _, kv := range resp.Kvs {
		key := string(kv.Key)

		var id uint64
		keyTemplate := fmt.Sprintf("%s%s/%%d", KvPrefixShardsInDataServer, addr)
		_, _ = fmt.Sscanf(keyTemplate, key, &id)

		shardIDs = append(shardIDs, ShardID(id))
	}
	return shardIDs, nil
}

func getShardsInStorageInKv(storageID StorageID) ([]ShardID, error) {
	ec := kv.GetEtcdClient()

	resp, err := ec.Get(context.Background(), ShardInStoragePrefixKey(storageID), client.WithPrefix())
	if err != nil {
		return nil, err
	}

	var shardIDs []ShardID
	for _, kv := range resp.Kvs {
		key := string(kv.Key)

		var id uint64
		keyTemplate := fmt.Sprintf("%s%d/%%d", KvPrefixShardsInDataServer, storageID)
		_, _ = fmt.Sscanf(keyTemplate, key, &id)

		shardIDs = append(shardIDs, ShardID(id))
	}
	return shardIDs, nil
}

func isShardInDataServerInKv(addr string, shardID ShardID) (bool, error) {
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

func hasDataServerInKv(addr string) (bool, error) {
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

func hasStorageInKv(storageID StorageID) (bool, error) {
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

func putStorageToKv(storage *Storage) error {
	pbStorage := &pbdata.Storage{
		Id:        uint64(storage.ID),
		IsDeleted: storage.IsDeleted,
		DeletedTs: timestamppb.New(storage.DeleteTs),
		CreateTs:  timestamppb.New(storage.createTs),
	}

	value, err := proto.Marshal(pbStorage)
	if err != nil {
		log.Warn("failed to encode storage to pb, storage=%v", *storage)
		return err
	}

	ec := kv.GetEtcdClient()
	_, err = ec.Put(context.Background(), StorageKey(storage.ID), string(value))
	if err != nil {
		log.Warn("failed to save storage to etcd, storage=%v", *storage)
		return err
	}

	return nil
}

func getStorageFromKv(storageID StorageID) (*Storage, error) {
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
		ID:        StorageID(pbStorage.Id),
		IsDeleted: pbStorage.IsDeleted,
		DeleteTs:  pbStorage.DeletedTs.AsTime(),
		createTs:  pbStorage.CreateTs.AsTime(),
	}, nil
}

func deleteStorageFromKv(storageID StorageID) error {
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

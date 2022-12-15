package dao

import (
	"context"
	"fmt"
	"strconv"
	"strings"

	client "go.etcd.io/etcd/client/v3"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/timestamppb"

	"sr.ht/moyanhao/bedrock-metaserver/dal/dto"
	"sr.ht/moyanhao/bedrock-metaserver/kvengine"
	"sr.ht/moyanhao/bedrock-metaserver/model"
	"sr.ht/moyanhao/bedrock-metaserver/utils/log"
)

const (
	KvPrefixStorage              = "/storages/"
	KvPrefixStorageByName        = "/storages_by_name/"
	KvPrefixMarkDeletedStorageID = "/deleted_storages/"
)

func storageKey(storageID model.StorageID) string {
	return fmt.Sprintf("%s0x%08x", KvPrefixStorage, storageID)
}

func storageByNameKey(name string) string {
	return fmt.Sprintf("%s%s", KvPrefixStorageByName, strings.TrimSpace(name))
}

func deletedStorageKey(storageID model.StorageID) string {
	return fmt.Sprintf("%s0x%08x", KvPrefixMarkDeletedStorageID, storageID)
}

func KvGetStorage(storageID model.StorageID) (*model.Storage, error) {
	ec := kvengine.GetEtcdClient()
	resp, err := ec.Get(context.Background(), storageKey(storageID))
	if err != nil {
		log.Warn("failed get storage from etcd, storageID=%d", storageID)
		return nil, err
	}

	if resp.Count != 1 {
		return nil, fmt.Errorf("expected only one storage , found %d", resp.Count)
	}

	pbStorage := &dto.Storage{}
	for _, kv := range resp.Kvs {
		err := proto.Unmarshal(kv.Value, pbStorage)
		if err != nil {
			log.Warn("failed to decode storage")

			return nil, err
		}

		break
	}
	return &model.Storage{
		ID:           model.StorageID(pbStorage.Id),
		Name:         pbStorage.Name,
		IsDeleted:    pbStorage.IsDeleted,
		DeleteTs:     pbStorage.DeletedTs.AsTime(),
		CreateTs:     pbStorage.CreateTs.AsTime(),
		RecycleTs:    pbStorage.RecycleTs.AsTime(),
		LastShardISN: model.ShardISN(pbStorage.LastShardIndex),
	}, nil
}

func KvGetStorageByName(name string) (*model.Storage, error) {
	ec := kvengine.GetEtcdClient()
	resp, err := ec.Get(context.Background(), storageByNameKey(name))
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

	return KvGetStorage(model.StorageID(storageID))
}

func KvPutStorage(storage *model.Storage) error {
	pbStorage := &dto.Storage{
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

	ec := kvengine.GetEtcdClient()
	_, err = ec.Put(context.Background(), storageKey(storage.ID), string(value))
	if err != nil {
		log.Warn("failed to save storage to etcd, storage=%v", storage)
		return err
	}

	storageIDStr := strconv.FormatUint(uint64(storage.ID), 10)
	_, err = ec.Put(context.Background(), storageByNameKey(storage.Name), storageIDStr)
	if err != nil {
		log.Warn("failed to save storage name, err: %v", err)
		return err
	}

	return nil
}

func KvDeleteStorage(storageID model.StorageID) error {
	keys := []string{
		storageKey(storageID),
		shardInStoragePrefixKey(storageID),
	}

	var ops []client.Op
	for _, key := range keys {
		ops = append(ops, client.OpDelete(key, client.WithPrefix()))
	}

	ec := kvengine.GetEtcdClient()
	_, err := ec.Txn(context.Background()).If().Then(ops...).Commit()
	if err != nil {
		log.Warn("failed to delete storage from etcd, storage=%d", storageID)
		return err
	}

	return nil
}

func KvPutDeletedStorageID(sID model.StorageID) error {
	key := deletedStorageKey(sID)

	ec := kvengine.GetEtcdClient()
	_, err := ec.Put(context.TODO(), key, "")
	if err != nil {
		return err
	}

	return nil
}

func KvDelDeletedStorageID(sID model.StorageID) error {
	key := deletedStorageKey(sID)

	ec := kvengine.GetEtcdClient()
	_, err := ec.Delete(context.TODO(), key)
	if err != nil {
		return err
	}

	return nil
}
package dal

import (
	"context"
	"errors"
	"fmt"
	"strconv"
	"strings"

	log "github.com/sirupsen/logrus"
	client "go.etcd.io/etcd/client/v3"

	"sr.ht/moyanhao/bedrock-metaserver/kv_engine"
	"sr.ht/moyanhao/bedrock-metaserver/model"
)

const (
	KvPrefixStorage              = "/storages/"
	KvPrefixStorageByName        = "/storages_by_name/"
	KvPrefixMarkDeletedStorageID = "/deleted_storages/"
	KvLastStorageIDkey           = "/last_storage_id"
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
	resp, err := kv_engine.GetEtcdClient().Get(context.Background(), storageKey(storageID))
	if err != nil {
		log.Warnf("failed get storage from etcd, storageID=%d", storageID)
		return nil, err
	}

	if resp.Count != 1 {
		return nil, fmt.Errorf("expected only one storage , found %d", resp.Count)
	}

	var storage model.Storage
	for _, kv := range resp.Kvs {

		err := storage.UnmarshalJSON(kv.Value)
		if err != nil {
			log.Warn("failed to decode storage")

			return nil, err
		}

		break
	}
	return &storage, nil
}

func KvGetStorageByName(name string) (*model.Storage, error) {
	resp, err := kv_engine.GetEtcdClient().Get(context.Background(), storageByNameKey(name))
	if err != nil {
		log.Warnf("failed get storage id by name, err: %v", err)
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
		log.Warnf("failed to parse storage id, err: %v", err)
		return nil, err
	}

	return KvGetStorage(model.StorageID(storageID))
}

func KvPutStorage(storage *model.Storage) error {
	value, err := storage.MarshalJSON()
	if err != nil {
		log.Warnf("failed to encode storage to pb, storage=%v", storage)
		return err
	}

	_, err = kv_engine.GetEtcdClient().Put(context.Background(), storageKey(storage.ID), string(value))
	if err != nil {
		log.Warnf("failed to save storage to etcd, storage=%v", storage)
		return err
	}

	storageIDStr := strconv.FormatUint(uint64(storage.ID), 10)
	_, err = kv_engine.GetEtcdClient().Put(context.Background(), storageByNameKey(storage.Name), storageIDStr)
	if err != nil {
		log.Warnf("failed to save storage name, err: %v", err)
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

	_, err := kv_engine.GetEtcdClient().Txn(context.Background()).If().Then(ops...).Commit()
	if err != nil {
		log.Warnf("failed to delete storage from etcd, storage=%d", storageID)
		return err
	}

	return nil
}

func KvPutDeletedStorageID(sID model.StorageID) error {
	key := deletedStorageKey(sID)

	_, err := kv_engine.GetEtcdClient().Put(context.TODO(), key, "")
	if err != nil {
		return err
	}

	return nil
}

func KvDelDeletedStorageID(sID model.StorageID) error {
	key := deletedStorageKey(sID)

	_, err := kv_engine.GetEtcdClient().Delete(context.TODO(), key)
	if err != nil {
		return err
	}

	return nil
}

func KvGetLastStorageId() (uint64, error) {
	resp, err := kv_engine.GetEtcdClient().Get(context.Background(), KvLastStorageIDkey)
	if err != nil {
		log.Errorf("failed load last storage id from etcd, err: %v", err)
		return 0, err
	}

	if len(resp.Kvs) != 1 {
		return 0, errors.New("wrong kv size")
	}

	sID, err := strconv.ParseUint(string(resp.Kvs[0].Value), 10, 64)
	if err != nil {
		return 0, err
	}

	return sID, nil
}

func KvPutLastStorageId(sID uint64) error {
	_, err := kv_engine.GetEtcdClient().Put(context.Background(), KvLastStorageIDkey, strconv.FormatUint(sID, 10))
	return err
}

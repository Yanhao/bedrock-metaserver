package dal

import (
	"context"
	"fmt"
	"strconv"
	"strings"

	log "github.com/sirupsen/logrus"
	client "go.etcd.io/etcd/client/v3"

	"sr.ht/moyanhao/bedrock-metaserver/errors"
	"sr.ht/moyanhao/bedrock-metaserver/meta_store"
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

func shardInStoragePrefixKey(storageID model.StorageID) string {
	return fmt.Sprintf("%s0x%08x", KvPrefixShard, storageID)
}

func KvGetStorage(storageID model.StorageID) (*model.Storage, error) {
	etcdClient, err := meta_store.GetEtcdClient()
	if err != nil {
		return nil, errors.Wrap(err, errors.ErrCodeSystem, "failed to get etcd client")
	}
	resp, err := etcdClient.Get(context.Background(), storageKey(storageID))
	if err != nil {
		log.Warnf("failed get storage from etcd, storageID=%d", storageID)
		return nil, errors.Wrap(err, errors.ErrCodeDatabase, "failed to get storage")
	}

	if resp.Count != 1 {
		log.Errorf("expected only one storage, found %d, storageID=%d", resp.Count, storageID)
		return nil, errors.New(errors.ErrCodeDatabase, "multiple storage entries found")
	}

	var storage model.Storage
	for _, kv := range resp.Kvs {
		err := storage.UnmarshalJSON(kv.Value)
		if err != nil {
			log.Errorf("failed to decode storage, storageID=%d, err=%v", storageID, err)
			return nil, errors.Wrap(err, errors.ErrCodeInternal, "failed to decode storage")
		}
		break
	}
	return &storage, nil
}

func KvGetStorageByName(name string) (*model.Storage, error) {
	etcdClient, err := meta_store.GetEtcdClient()
	if err != nil {
		return nil, errors.Wrap(err, errors.ErrCodeSystem, "failed to get etcd client")
	}
	resp, err := etcdClient.Get(context.Background(), storageByNameKey(name))
	if err != nil {
		log.Warnf("failed get storage id by name, err: %v", err)
		return nil, errors.Wrap(err, errors.ErrCodeDatabase, "failed to get storage by name")
	}

	if resp.Count == 0 {
		return nil, nil
	}
	if resp.Count != 1 {
		log.Errorf("storage by name count is not equals 1, count: %v", resp.Count)
		return nil, errors.New(errors.ErrCodeDatabase, "multiple storage entries found for name")
	}
	storageIDStr := resp.Kvs[0].Value
	storageID, err := strconv.ParseUint(string(storageIDStr), 10, 32)
	if err != nil {
		log.Errorf("failed to parse storage id, err: %v", err)
		return nil, errors.Wrap(err, errors.ErrCodeInvalidArgument, "invalid storage ID format")
	}

	return KvGetStorage(model.StorageID(storageID))
}

func KvPutStorage(storage *model.Storage) error {
	value, err := storage.MarshalJSON()
	if err != nil {
		log.Warnf("failed to encode storage to pb, storage=%v", storage)
		return errors.Wrap(err, errors.ErrCodeInternal, "failed to encode storage")
	}

	etcdClient, err := meta_store.GetEtcdClient()
	if err != nil {
		return errors.Wrap(err, errors.ErrCodeSystem, "failed to get etcd client")
	}
	_, err = etcdClient.Put(context.Background(), storageKey(storage.ID), string(value))
	if err != nil {
		log.Warnf("failed to save storage to etcd, storage=%v", storage)
		return errors.Wrap(err, errors.ErrCodeDatabase, "failed to save storage")
	}

	storageIDStr := strconv.FormatUint(uint64(storage.ID), 10)
	_, err = etcdClient.Put(context.Background(), storageByNameKey(storage.Name), storageIDStr)
	if err != nil {
		log.Warnf("failed to save storage name, err: %v", err)
		return errors.Wrap(err, errors.ErrCodeDatabase, "failed to save storage name")
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

	etcdClient, err := meta_store.GetEtcdClient()
	if err != nil {
		return errors.Wrap(err, errors.ErrCodeSystem, "failed to get etcd client")
	}
	_, err = etcdClient.Txn(context.Background()).If().Then(ops...).Commit()
	if err != nil {
		log.Warnf("failed to delete storage from etcd, storage=%d", storageID)
		return errors.Wrap(err, errors.ErrCodeDatabase, "failed to delete storage")
	}

	return nil
}

func KvPutDeletedStorageID(sID model.StorageID) error {
	key := deletedStorageKey(sID)

	etcdClient, err := meta_store.GetEtcdClient()
	if err != nil {
		return errors.Wrap(err, errors.ErrCodeSystem, "failed to get etcd client")
	}
	_, err = etcdClient.Put(context.TODO(), key, "")
	if err != nil {
		log.Errorf("failed to mark storage as deleted, storageID=%d, err=%v", sID, err)
		return errors.Wrap(err, errors.ErrCodeDatabase, "failed to mark storage as deleted")
	}

	return nil
}

func KvDelDeletedStorageID(sID model.StorageID) error {
	key := deletedStorageKey(sID)

	etcdClient, err := meta_store.GetEtcdClient()
	if err != nil {
		return errors.Wrap(err, errors.ErrCodeSystem, "failed to get etcd client")
	}
	_, err = etcdClient.Delete(context.TODO(), key)
	if err != nil {
		log.Errorf("failed to unmark storage as deleted, storageID=%d, err=%v", sID, err)
		return errors.Wrap(err, errors.ErrCodeDatabase, "failed to unmark storage as deleted")
	}

	return nil
}

func KvGetLastStorageId() (uint64, error) {
	etcdClient, err := meta_store.GetEtcdClient()
	if err != nil {
		return 0, errors.Wrap(err, errors.ErrCodeSystem, "failed to get etcd client")
	}
	resp, err := etcdClient.Get(context.Background(), KvLastStorageIDkey)
	if err != nil {
		log.Errorf("failed load last storage id from etcd, err: %v", err)
		return 0, errors.Wrap(err, errors.ErrCodeDatabase, "failed to get last storage ID")
	}

	if len(resp.Kvs) == 0 {
		return 0, nil
	}

	if len(resp.Kvs) != 1 {
		log.Errorf("expected only one last storage ID entry, found %d", len(resp.Kvs))
		return 0, errors.New(errors.ErrCodeDatabase, "invalid storage ID entry count")
	}

	sID, err := strconv.ParseUint(string(resp.Kvs[0].Value), 10, 64)
	if err != nil {
		log.Errorf("failed to parse last storage ID, err: %v", err)
		return 0, errors.Wrap(err, errors.ErrCodeInvalidArgument, "invalid storage ID format")
	}

	return sID, nil
}

func KvPutLastStorageId(sID uint64) error {
	etcdClient, err := meta_store.GetEtcdClient()
	if err != nil {
		return errors.Wrap(err, errors.ErrCodeSystem, "failed to get etcd client")
	}
	_, err = etcdClient.Put(context.Background(), KvLastStorageIDkey, strconv.FormatUint(sID, 10))
	if err != nil {
		log.Errorf("failed to save last storage ID, err: %v", err)
		return errors.Wrap(err, errors.ErrCodeDatabase, "failed to save last storage ID")
	}
	return nil
}

package metadata

import (
	"context"
	"strconv"
	"time"

	cache "github.com/hashicorp/golang-lru"

	"sr.ht/moyanhao/bedrock-metaserver/common/log"
	"sr.ht/moyanhao/bedrock-metaserver/kv"
)

type StorageID uint64

type Storage struct {
	ID        StorageID
	IsDeleted bool
	DeleteTs  time.Time
	createTs  time.Time
}

func NewStorage() *Storage {
	return &Storage{}
}

// var StorageList map[StorageID]*Storage
var StorageCache *cache.Cache

func InitStorageCache() {
	StorageCache, _ = cache.New(10240)
}

const (
	KvLastStorageIDkey = "/last_storage_id"
)

var lastStorageID uint64

func LoadLastStroageId() error {
	ec := kv.GetEtcdClient()
	resp, err := ec.Get(context.Background(), KvLastStorageIDkey)
	if err != nil {
		log.Error("failed load last storage id from etcd, err: %v", err)
		return err
	}

	for _, kv := range resp.Kvs {

		sID, err := strconv.ParseUint(string(kv.Value), 10, 64)
		if err != nil {
			return err
		}
		lastStorageID = sID
		break
	}

	return nil
}

func SaveLastStorageId() error {
	ec := kv.GetEtcdClient()
	_, err := ec.Put(context.Background(), KvLastShardIDkey, strconv.FormatUint(lastStorageID, 10))
	if err != nil {
		return err
	}

	return nil
}

func StorageCreate() (*Storage, error) {
	id := lastStorageID + 1
	lastStorageID++

	storage := &Storage{
		ID:        StorageID(id),
		createTs:  time.Now(),
		DeleteTs:  time.Time{},
		IsDeleted: false,
	}

	err := PutStorage(storage)
	if err != nil {
		return nil, err
	}

	err = SaveLastStorageId()
	if err != nil {
		return nil, err
	}

	return storage, nil
}

func StorageDelete(storageID StorageID) error {
	var storage *Storage
	value, ok := StorageCache.Get(storageID)
	if !ok {
		var err error
		storage, err = GetStorage(storageID)
		if err != nil {
			return err
		}
		_ = StorageCache.Add(storageID, storage)

	} else {
		storage, _ = value.(*Storage)
	}
	if storage.IsDeleted {
		return nil
	}

	storage.IsDeleted = true
	storage.DeleteTs = time.Now()

	return PutStorage(storage)
}

func StorageInfo() {
}

func StorageRealDelete(storageID StorageID) error {
	shardIDs, err := GetShardsInStorage(storageID)
	if err != nil {
		return err
	}

	sm := GetShardManager()

	for _, shardID := range shardIDs {
		err := sm.DeleteShard(shardID)
		if err != nil {
			return err
		}
	}

	err = DeleteStorage(storageID)
	if err != nil {
		return err
	}

	return nil
}

func StorageUndelete(storageID StorageID) error {
	var storage *Storage
	value, ok := StorageCache.Get(storageID)
	if !ok {
		var err error
		storage, err = GetStorage(storageID)
		if err != nil {
			return err
		}
		_ = StorageCache.Add(storageID, storage)

	} else {
		storage, _ = value.(*Storage)
	}

	if !storage.IsDeleted {
		return nil
	}
	storage.IsDeleted = false
	storage.DeleteTs = time.Time{}

	return PutStorage(storage)
}

func StorageRename() {
}

package metadata

import (
	"context"
	"strconv"
	"sync"
	"time"

	cache "github.com/hashicorp/golang-lru"

	"sr.ht/moyanhao/bedrock-metaserver/common/log"
	"sr.ht/moyanhao/bedrock-metaserver/kv"
)

type StorageID uint32

type Storage struct {
	ID        StorageID
	Name      string
	IsDeleted bool
	DeleteTs  time.Time
	createTs  time.Time

	LastShardIndex uint32
}

func NewStorage() *Storage {
	return &Storage{}
}

// var StorageList map[StorageID]*Storage
var StorageCache *cache.Cache

func InitStorageCache() {
	StorageCache, _ = cache.New(10240)
}

type StorageManager struct {
	storageCache *cache.Cache
}

func NewStorageManager() *StorageManager {
	c, err := cache.New(10240)
	if err != nil {
		panic("create cache failed")

	}
	return &StorageManager{
		storageCache: c,
	}
}

var (
	storageManager     *StorageManager
	storageManagerOnce sync.Once
)

func GetStorageManager() *StorageManager {
	storageManagerOnce.Do(func() {
		storageManager = NewStorageManager()
	})

	return storageManager
}

func (sm *StorageManager) GetStorage(id StorageID) (*Storage, error) {
	st, err := getStorageFromKv(id)
	if err != nil {
		return nil, err
	}

	sm.storageCache.Add(id, st)
	return st, nil
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
	_, err := ec.Put(context.Background(), KvLastStorageIDkey, strconv.FormatUint(lastStorageID, 10))
	if err != nil {
		return err
	}

	return nil
}

func CreateNewStorage() (*Storage, error) {
	id := lastStorageID + 1
	lastStorageID++

	storage := &Storage{
		ID:        StorageID(id),
		createTs:  time.Now(),
		DeleteTs:  time.Time{},
		IsDeleted: false,
	}

	err := putStorageToKv(storage)
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
		storage, err = getStorageFromKv(storageID)
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

	return putStorageToKv(storage)
}

func StorageInfo() {
}

func StorageRealDelete(storageID StorageID) error {
	shardIDs, err := getShardsInStorageInKv(storageID)
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

	err = deleteStorageFromKv(storageID)
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
		storage, err = getStorageFromKv(storageID)
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

	return putStorageToKv(storage)
}

func StorageRename(storageID StorageID, name string) error {
	return nil
}

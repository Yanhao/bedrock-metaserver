package metadata

import (
	"context"
	"fmt"
	"strconv"
	"sync"
	"time"

	cache "github.com/hashicorp/golang-lru"
	"go.uber.org/atomic"

	"sr.ht/moyanhao/bedrock-metaserver/common/log"
	"sr.ht/moyanhao/bedrock-metaserver/kv"
)

type StorageID uint32

type Storage struct {
	ID        StorageID
	Name      string
	IsDeleted bool
	DeleteTs  time.Time
	RecycleTs time.Time
	CreateTs  time.Time
	Owner     string

	LastShardIndex uint32
}

func (s *Storage) String() string {
	return fmt.Sprintf("%+v", s)
}

func NewStorage() *Storage {
	return &Storage{}
}

const (
	KvLastStorageIDkey = "/last_storage_id"
)

var lastStorageID atomic.Uint64

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
		lastStorageID.Store(sID)
		break
	}

	return nil
}

func SaveLastStorageId() error {
	sID := lastStorageID.Load()
	ec := kv.GetEtcdClient()
	_, err := ec.Put(context.Background(), KvLastStorageIDkey, strconv.FormatUint(sID, 10))
	if err != nil {
		return err
	}

	return nil
}

func CreateNewStorage() (*Storage, error) {
	id := lastStorageID.Inc()

	storage := &Storage{
		ID:        StorageID(id),
		CreateTs:  time.Now(),
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

func (sm *StorageManager) SaveStorage(st *Storage) error {
	err := putStorageToKv(st)
	if err != nil {
		return err
	}

	sm.storageCache.Remove(st.ID)

	return nil
}

func (sm *StorageManager) ClearCache() {
	sm.storageCache.Purge()
}

func (sm *StorageManager) StorageDelete(storageID StorageID, recycleAfter time.Duration) error {
	var storage *Storage
	value, ok := sm.storageCache.Get(storageID)
	if !ok {
		var err error
		storage, err = getStorageFromKv(storageID)
		if err != nil {
			return err
		}
		_ = sm.storageCache.Add(storageID, storage)

	} else {
		storage, _ = value.(*Storage)
	}
	if storage.IsDeleted {
		return nil
	}

	storage.IsDeleted = true
	storage.DeleteTs = time.Now()
	storage.RecycleTs = time.Now().Add(recycleAfter)

	err := putStorageToKv(storage)
	if err != nil {
		return err
	}

	return putDeletedStorageID(storageID)
}

func (sm *StorageManager) StorageRealDelete(storageID StorageID) error {
	shardIDs, err := getShardsInStorageInKv(storageID)
	if err != nil {
		return err
	}

	shm := GetShardManager()

	for _, shardID := range shardIDs {
		err := shm.ShardDelete(shardID)
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

func (sm *StorageManager) StorageUndelete(storageID StorageID) error {
	var storage *Storage
	value, ok := sm.storageCache.Get(storageID)
	if !ok {
		var err error
		storage, err = getStorageFromKv(storageID)
		if err != nil {
			return err
		}
		_ = sm.storageCache.Add(storageID, storage)
	} else {
		storage, _ = value.(*Storage)
	}

	if !storage.IsDeleted {
		return nil
	}
	storage.IsDeleted = false
	storage.DeleteTs = time.Time{}

	err := putStorageToKv(storage)
	if err != nil {
		return err
	}

	return delDeletedStorageID(storageID)
}

func (sm *StorageManager) StorageRename(storageID StorageID, name string) error {
	var st *Storage
	value, ok := sm.storageCache.Get(storageID)
	if !ok {
		var err error
		st, err = getStorageFromKv(storageID)
		if err != nil {
			return err
		}
		_ = sm.storageCache.Add(storageID, st)
	} else {
		st, _ = value.(*Storage)
	}

	st.Name = name
	err := putStorageToKv(st)
	if err != nil {
		return err
	}

	return nil
}

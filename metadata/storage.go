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

	lock sync.RWMutex
}

func (s *Storage) FetchAddLastIndex() uint32 {
	s.lock.Lock()
	defer s.lock.Unlock()

	s.LastShardIndex++
	return s.LastShardIndex
}

func (s *Storage) MarkDelete(recycleAfter time.Duration) error {
	s.lock.Lock()
	defer s.lock.Unlock()

	if s.IsDeleted {
		return nil
	}

	s.IsDeleted = true
	s.DeleteTs = time.Now()
	s.RecycleTs = time.Now().Add(recycleAfter)

	return nil
}

func (s *Storage) Undelete() error {
	s.lock.Lock()
	defer s.lock.Unlock()

	if !s.IsDeleted {
		return nil
	}

	storage.IsDeleted = false
	storage.DeleteTs = time.Time{}

	return nil

}

func (s *Storage) Rename(newName string) error {
	s.lock.Lock()
	defer s.lock.Unlock()

	s.Name = newName

	return nil
}

func (s *Storage) Info() string {
	return fmt.Sprintf("%+v", s)
}

func NewStorage() *Storage {
	return &Storage{}
}

const (
	KvLastStorageIDkey = "/last_storage_id"
)

var lastStorageID atomic.Uint64

func LoadLastStorageId() error {
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

func CreateNewStorage() (StorageID, error) {
	id := lastStorageID.Inc()

	storage := &Storage{
		ID:        StorageID(id),
		CreateTs:  time.Now(),
		DeleteTs:  time.Time{},
		IsDeleted: false,
	}

	err := SaveLastStorageId()
	if err != nil {
		return 0, err
	}

	err = putStorageToKv(storage)
	if err != nil {
		return 0, err
	}

	return StorageID(id), nil
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
	v, ok := sm.storageCache.Get(id)
	if ok {
		return v.(*Storage), nil
	}

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
	storage, err := sm.GetStorage(storageID)
	if err != nil {
		return err
	}

	err = storage.MarkDelete(recycleAfter)
	if err != nil {
		return err
	}

	err = putStorageToKv(storage)
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
	storage, err := sm.GetStorage(storageID)
	if err != nil {
		return err
	}

	err = storage.Undelete()
	if err != nil {
		return err
	}

	err = putStorageToKv(storage)
	if err != nil {
		return err
	}

	return delDeletedStorageID(storageID)
}

func (sm *StorageManager) StorageRename(storageID StorageID, name string) error {
	storage, err := sm.GetStorage(storageID)
	if err != nil {
		return err
	}

	err = storage.Rename(name)
	if err != nil {
		return err
	}

	return nil
}

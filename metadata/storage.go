package metadata

import (
	"context"
	"fmt"
	"strconv"
	"sync"
	"time"

	cache "github.com/hashicorp/golang-lru"
	"github.com/jinzhu/copier"
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

	LastShardISN ShardISN

	lock sync.RWMutex `copier:"-"`
}

func (s *Storage) FetchAddLastISN() ShardISN {
	s.lock.Lock()
	defer s.lock.Unlock()

	s.LastShardISN++
	return s.LastShardISN
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

	s.IsDeleted = false
	s.DeleteTs = time.Time{}

	return nil
}

func (s *Storage) Rename(newName string) error {
	s.lock.Lock()
	defer s.lock.Unlock()

	s.Name = newName

	return nil
}

func (s *Storage) Info() string {
	s.lock.RLock()
	defer s.lock.RUnlock()

	return fmt.Sprintf(
		"Storage{ ID: %08x, Name: %s, IsDeleted: %v, DeleteTs: %v, RecycleTs: %v, CreateTs: %v, Owner: %s }",
		s.ID,
		s.Name,
		s.IsDeleted,
		s.DeleteTs,
		s.RecycleTs,
		s.CreateTs,
		s.Owner,
	)
}

func (s *Storage) Copy() *Storage {
	s.lock.RLock()
	defer s.lock.RUnlock()

	var ret Storage
	copier.Copy(&ret, s)
	s.lock = sync.RWMutex{}

	return &ret
}

const (
	KvLastStorageIDkey = "/last_storage_id"
)

type StorageManager struct {
	lastStorageID atomic.Uint64
	storageCache  *cache.Cache
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

func (sm *StorageManager) LoadLastStorageId() error {
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
		sm.lastStorageID.Store(sID)
		break
	}

	return nil
}

func (sm *StorageManager) GetStorage(id StorageID) (*Storage, error) {
	v, ok := sm.storageCache.Get(id)
	if ok {
		return v.(*Storage), nil
	}

	st, err := kvGetStorage(id)
	if err != nil {
		return nil, err
	}

	sm.storageCache.Add(id, st)
	return st, nil
}

func (sm *StorageManager) GetStorageCopy(id StorageID) (*Storage, error) {
	ret, err := sm.GetStorage(id)
	if err != nil {
		return nil, err
	}

	return ret.Copy(), nil
}

func (sm *StorageManager) GetStorageByName(name string) (*Storage, error) {
	return kvGetStorageByName(name)
}

func (sm *StorageManager) FetchAddStorageLastISN(id StorageID) (ShardISN, error) {
	st, err := sm.GetStorage(id)
	if err != nil {
		return 0, err
	}

	ret := st.FetchAddLastISN()

	err = sm.PutStorage(st)
	if err != nil {
		// FIXME: restore LastISN
		return 0, err
	}

	return ret, nil
}

func (sm *StorageManager) SaveLastStorageId() error {
	sID := sm.lastStorageID.Load()
	ec := kv.GetEtcdClient()
	_, err := ec.Put(context.Background(), KvLastStorageIDkey, strconv.FormatUint(sID, 10))
	if err != nil {
		return err
	}

	return nil
}

func (sm *StorageManager) CreateNewStorage() (*Storage, error) {
	id := sm.lastStorageID.Inc()

	storage := &Storage{
		ID:        StorageID(id),
		CreateTs:  time.Now(),
		DeleteTs:  time.Time{},
		IsDeleted: false,
	}

	err := sm.SaveLastStorageId()
	if err != nil {
		return nil, err
	}

	err = kvPutStorage(storage)
	if err != nil {
		return nil, err
	}

	return storage.Copy(), nil
}

func (sm *StorageManager) PutStorage(st *Storage) error {
	err := kvPutStorage(st)
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

	err = kvPutStorage(storage)
	if err != nil {
		return err
	}

	return kvPutDeletedStorageID(storageID)
}

func (sm *StorageManager) StorageRealDelete(storageID StorageID) error {
	shardIDs, err := kvGetShardsInStorage(storageID)
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

	err = kvDeleteStorage(storageID)
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

	err = kvPutStorage(storage)
	if err != nil {
		return err
	}

	return kvDelDeletedStorageID(storageID)
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

	return sm.PutStorage(storage)
}

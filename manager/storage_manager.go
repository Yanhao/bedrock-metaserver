package manager

import (
	"context"
	"errors"
	"strconv"
	"sync"
	"time"

	log "github.com/sirupsen/logrus"
	"go.uber.org/atomic"

	"sr.ht/moyanhao/bedrock-metaserver/dal"
	"sr.ht/moyanhao/bedrock-metaserver/kv_engine"
	"sr.ht/moyanhao/bedrock-metaserver/model"
)

const (
	KvLastStorageIDkey = "/last_storage_id"
)

var (
	ErrNoSuchStorage = errors.New("no such storage")
)

type StorageManager struct {
	lastStorageID atomic.Uint64

	storageCache   map[model.StorageID]*model.Storage
	storageNameMap map[string]model.StorageID
	storageLock    sync.RWMutex
}

func NewStorageManager() *StorageManager {
	// c, err := cache.New(10240)
	// if err != nil {
	// 	panic("create storage cache failed")

	// }
	return &StorageManager{
		storageCache:   make(map[model.StorageID]*model.Storage),
		storageNameMap: make(map[string]model.StorageID),
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

func (sm *StorageManager) putStorage(st *model.Storage) error {
	err := dal.KvPutStorage(st)
	if err != nil {
		return err
	}

	s := st.Copy()

	sm.storageLock.Lock()
	defer sm.storageLock.Unlock()

	sm.storageCache[s.ID] = s
	sm.storageNameMap[s.Name] = s.ID

	return nil
}

func (sm *StorageManager) ClearCache() {
	sm.storageLock.Lock()
	defer sm.storageLock.Unlock()

	sm.storageCache = make(map[model.StorageID]*model.Storage)
	sm.storageNameMap = make(map[string]model.StorageID)
}

func (sm *StorageManager) LoadLastStorageId() error {
	ec := kv_engine.GetEtcdClient()
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

func (sm *StorageManager) SaveLastStorageId() error {
	sID := sm.lastStorageID.Load()
	ec := kv_engine.GetEtcdClient()

	_, err := ec.Put(context.Background(), KvLastStorageIDkey, strconv.FormatUint(sID, 10))
	if err != nil {
		return err
	}

	return nil
}

func (sm *StorageManager) GetStorage(id model.StorageID) (*model.Storage, error) {
	sm.storageLock.Lock()
	defer sm.storageLock.Unlock()

	v, ok := sm.storageCache[id]
	if ok {
		return v.Copy(), nil
	}

	st, err := dal.KvGetStorage(id)
	if err != nil {
		return nil, err
	}

	s := st.Copy()
	sm.storageCache[s.ID] = s
	sm.storageNameMap[s.Name] = s.ID

	return st, nil
}

func (sm *StorageManager) GetStorageByName(name string) (*model.Storage, error) {
	sm.storageLock.Lock()
	defer sm.storageLock.Unlock()

	id, ok := sm.storageNameMap[name]
	if ok {
		v, ok := sm.storageCache[id]
		if !ok {
			// FIXME: should not happen
			return nil, ErrNoSuchStorage
		}

		return v.Copy(), nil
	}

	st, err := dal.KvGetStorageByName(name)
	if err != nil {
		return nil, err
	}
	if st == nil {
		return nil, nil
	}

	s := st.Copy()
	sm.storageCache[s.ID] = s
	sm.storageNameMap[s.Name] = s.ID

	return st, nil
}

func (sm *StorageManager) FetchAddStorageLastISN(id model.StorageID) (model.ShardISN, error) {
	st, err := sm.GetStorage(id)
	if err != nil {
		return 0, err
	}

	ret := st.LastShardISN
	st.LastShardISN += 1

	err = sm.putStorage(st)
	if err != nil {
		// FIXME: restore LastISN
		return 0, err
	}

	log.Info("shard isn: 0x%08x", ret)
	return ret, nil
}

func (sm *StorageManager) CreateNewStorage(name string) (*model.Storage, error) {
	id := sm.lastStorageID.Inc()
	log.Info("new storage id, %v", id)

	err := sm.SaveLastStorageId()
	if err != nil {
		return nil, err
	}

	storage := &model.Storage{
		ID:        model.StorageID(id),
		Name:      name,
		CreateTs:  time.Now(),
		DeleteTs:  time.Time{},
		IsDeleted: false,
	}

	s := storage.Copy()
	if err := sm.putStorage(s); err != nil {
		return nil, err
	}

	return storage, nil
}

func (sm *StorageManager) StorageDelete(storageID model.StorageID, recycleAfter time.Duration) error {
	storage, err := sm.GetStorage(storageID)
	if err != nil {
		return err
	}
	if storage.IsDeleted {
		return nil
	}

	storage.IsDeleted = true
	storage.DeleteTs = time.Now()
	storage.RecycleTs = time.Now().Add(recycleAfter)

	if err := sm.putStorage(storage); err != nil {
		return err
	}

	return dal.KvPutDeletedStorageID(storageID)
}

func (sm *StorageManager) StorageUndelete(storageID model.StorageID) error {
	storage, err := sm.GetStorage(storageID)
	if err != nil {
		return err
	}

	if !storage.IsDeleted {
		return nil
	}

	storage.IsDeleted = false
	storage.DeleteTs = time.Time{}

	if err := sm.putStorage(storage); err != nil {
		return err
	}

	return dal.KvDelDeletedStorageID(storageID)
}

func (sm *StorageManager) StorageRealDelete(storageID model.StorageID) error {
	shardIDs, err := dal.KvGetShardsInStorage(storageID)
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

	err = dal.KvDeleteStorage(storageID)
	if err != nil {
		return err
	}

	return nil
}

func (sm *StorageManager) StorageRename(storageID model.StorageID, name string) error {
	storage, err := sm.GetStorage(storageID)
	if err != nil {
		return err
	}

	storage.Name = name

	return sm.putStorage(storage)
}

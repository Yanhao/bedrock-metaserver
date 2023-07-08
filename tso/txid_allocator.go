package tso

import (
	"context"
	"encoding/json"
	"fmt"
	"sync"
	"time"

	log "github.com/sirupsen/logrus"

	"sr.ht/moyanhao/bedrock-metaserver/kv_engine"
)

const (
	TXID_KEY           = "/tx_id"
	PRE_ALLOCATE_RANGE = 0xFFFF
)

type tsoValue struct {
	TxId uint64 `json:"tx_id"`
}

type TxIDsAllocator struct {
	timestamp       uint64
	preAllcateRange uint64
	nowPosition     uint64

	lock sync.Mutex
}

var (
	txIDsAllocator     *TxIDsAllocator
	txIDsAllocatorOnce sync.Once
)

func GetTxIDAllocator() *TxIDsAllocator {
	txIDsAllocatorOnce.Do(func() {
		txIDsAllocator = NewTxIDsAllocator()
	})

	return txIDsAllocator
}

func NewTxIDsAllocator() *TxIDsAllocator {
	ec := kv_engine.GetEtcdClient()
	resp, err := ec.KV.Get(context.Background(), TXID_KEY)
	if err != nil || resp.Count != 1 {
		panic(fmt.Sprintf("get /txid key failed, err: %v", err))
	}

	var tsov tsoValue
	err = json.Unmarshal(resp.Kvs[0].Value, &tsov)
	if err != nil {
		panic(fmt.Sprintf("unmashal tosValue failed, err: %v", err))
	}

	return &TxIDsAllocator{
		timestamp:       tsov.TxId,
		preAllcateRange: PRE_ALLOCATE_RANGE,
		nowPosition:     0,
	}
}

func (t *TxIDsAllocator) Allocate(count uint32) ([]uint64, error) {
	t.lock.Lock()
	defer t.lock.Unlock()

	var ret []uint64
	for i := count; i > 0; i-- {
		v, err := t.AllocateOne(false)
		if err != nil {
			return nil, err
		}

		ret = append(ret, v)
	}

	return ret, nil
}

func (t *TxIDsAllocator) AllocateOne(withLock bool) (uint64, error) {
	if withLock {
		t.lock.Lock()
		defer t.lock.Unlock()
	}

	if t.nowPosition < t.preAllcateRange {
		t.nowPosition += 1

		return t.timestamp + t.nowPosition, nil
	}

	t.timestamp = uint64(time.Now().UnixNano()) & 0xFFFF_FFFF
	t.nowPosition = 0

	tsov := tsoValue{
		TxId: t.timestamp,
	}

	data, err := json.Marshal(tsov)
	if err != nil {
		log.Errorf("failed to marshal tosValue data, err: %v", err)
		return 0, err
	}

	ec := kv_engine.GetEtcdClient()
	_, err = ec.Put(context.TODO(), TXID_KEY, string(data))
	if err != nil {
		log.Errorf("failed to put tso value, err: %v", err)
		return 0, err
	}

	return t.AllocateOne(false)
}

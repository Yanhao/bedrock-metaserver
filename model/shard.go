package model

import (
	"encoding/json"
	"fmt"
	"math/big"
	"runtime/debug"
	"time"

	"github.com/jinzhu/copier"
	log "github.com/sirupsen/logrus"
)

type (
	ShardISN uint32
	ShardID  uint64
)

type Shard struct {
	ISN             ShardISN            `json:"isn"` // internal serial number
	SID             StorageID           `json:"sid"`
	Replicates      map[string]struct{} `json:"replicates"`
	ReplicaUpdateTs time.Time           `json:"replica_update_ts"`
	IsDeleted       bool                `json:"is_deleted"`
	DeleteTs        time.Time           `json:"delete_ts"`
	CreateTs        time.Time           `json:"create_ts"`
	Leader          string              `json:"leader"`
	LeaderChangeTs  time.Time           `json:"leader_change_ts"`
	RangeKeyStart   []byte              `json:"range_key_start"`
	RangeKeyEnd     []byte              `json:"range_key_end"`
}

func GenerateShardID(storageID StorageID, shardISN ShardISN) ShardID {
	shardID := (uint64(storageID) << 32) | (uint64(shardISN))

	return ShardID(shardID)
}

func (s *Shard) MarshalJSON() ([]byte, error) {
	type Alias Shard
	return json.Marshal(&struct {
		ReplicaUpdateTs int64 `json:"replica_update_ts"`
		DeleteTs        int64 `json:"delete_ts"`
		CreateTs        int64 `json:"create_ts"`
		LeaderChangeTs  int64 `json:"leader_change_ts"`
		*Alias
	}{
		ReplicaUpdateTs: s.ReplicaUpdateTs.Unix(),
		DeleteTs:        s.DeleteTs.Unix(),
		CreateTs:        s.CreateTs.Unix(),
		LeaderChangeTs:  s.LeaderChangeTs.Unix(),
		Alias:           (*Alias)(s),
	})
}

func (s *Shard) UnmarshalJSON(data []byte) error {
	log.Infof("json: %s", string(data))
	type Alias Shard
	aux := &struct {
		ReplicaUpdateTs int64 `json:"replica_update_ts"`
		DeleteTs        int64 `json:"delete_ts"`
		CreateTs        int64 `json:"create_ts"`
		LeaderChangeTs  int64 `json:"leader_change_ts"`
		*Alias
	}{
		Alias: (*Alias)(s),
	}

	if err := json.Unmarshal(data, &aux); err != nil {
		return err
	}

	s.ReplicaUpdateTs = time.Unix(aux.ReplicaUpdateTs, 0)
	s.DeleteTs = time.Unix(aux.DeleteTs, 0)
	s.CreateTs = time.Unix(aux.CreateTs, 0)
	s.LeaderChangeTs = time.Unix(aux.LeaderChangeTs, 0)

	log.Infof("data: %#v", s)

	return nil
}

func (sd *Shard) ID() ShardID {
	return GenerateShardID(sd.SID, sd.ISN)
}

func (sd *Shard) Copy() *Shard {
	var ret Shard
	if err := copier.Copy(&ret, sd); err != nil {
		log.Errorf("err: %v stack:%s", err, string(debug.Stack()))
		panic(fmt.Sprintf("copy shard struct failed, err: %v", err))
	}

	return &ret
}

func (sd *Shard) ChangeLeader(addr string) {
	if sd.Leader != addr {
		sd.Leader = addr
		sd.LeaderChangeTs = time.Now()
	}
}

func (sd *Shard) ValueSize() uint64 {
	return 0
}

func (sd *Shard) SplitShardRangeKey() []byte {
	min := big.NewInt(0).SetBytes(sd.RangeKeyStart)
	max := big.NewInt(0).SetBytes(sd.RangeKeyEnd)

	mid := big.NewInt(0).Add(min, max)
	mid = mid.Div(mid, big.NewInt(2))

	return mid.Bytes()
}

package model

import (
	"encoding/json"
	"math/big"
	"time"

	"github.com/jinzhu/copier"
)

type (
	ShardISN uint32
	ShardID  uint64
)

type Shard struct {
	ISN             ShardISN // internal serial number
	SID             StorageID
	Replicates      map[string]struct{}
	ReplicaUpdateTs time.Time
	IsDeleted       bool
	DeleteTs        time.Time
	CreateTs        time.Time
	Leader          string
	LeaderChangeTs  time.Time
	RangeKeyMax     []byte
	RangeKeyMin     []byte
}

func GenerateShardID(storageID StorageID, shardISN ShardISN) ShardID {
	shardID := (uint64(storageID) << 32) | (uint64(shardISN))

	return ShardID(shardID)
}

func (s *Shard) MarshalJSON() ([]byte, error) {
	type Alias Shard
	return json.Marshal(&struct {
		ReplicaUpdateTs int64 `json:"replicaUpdateTs"`
		DeleteTs        int64 `json:"deleteTs"`
		CreateTs        int64 `json:"createTs"`
		LeaderChangeTs  int64 `json:"leaderChangeTs"`
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
	type Alias Shard
	aux := &struct {
		ReplicaUpdateTs int64 `json:"replicaUpdateTs"`
		DeleteTs        int64 `json:"deleteTs"`
		CreateTs        int64 `json:"createTs"`
		LeaderChangeTs  int64 `json:"leaderChangeTs"`
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

	return nil
}

func (sd *Shard) ID() ShardID {
	return GenerateShardID(sd.SID, sd.ISN)
}

func (sd *Shard) Copy() *Shard {
	var ret Shard
	if err := copier.Copy(&ret, sd); err != nil {
		panic(err)
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
	min := big.NewInt(0)
	max := big.NewInt(0)

	max.SetBytes(sd.RangeKeyMax)
	min.SetBytes(sd.RangeKeyMin)

	tmp := big.NewInt(0)
	tmp = tmp.Sub(max, min)
	tmp = tmp.Div(tmp, big.NewInt(2))

	min.Add(min, tmp)

	return min.Bytes()
}

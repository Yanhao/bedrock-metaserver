package model

import (
	"math/big"
	"time"

	"github.com/jinzhu/copier"
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

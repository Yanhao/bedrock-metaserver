package metadata

import (
	cache "github.com/hashicorp/golang-lru"
)

type Replicate interface {
}

type ShardID uint64

type Shard struct {
	ID         ShardID
	Replicates []*Replicate
}

func (sd *Shard) Info() {
}

func (sd *Shard) Transfer() {
}

func (sd *Shard) Create() {
}

func (sd *Shard) Delete() {
}

func (sd *Shard) MarkDelete() {
}

func (sd *Shard) MarkUndelete() {
}

func (sd *Shard) Repair() {
}

type ShardManager struct {
	shardsCache *cache.Cache
}

func NewShardManager() *ShardManager {
	c, err := cache.New(102400)
	if err != nil {
		panic("create cache failed")
	}

	return &ShardManager{
		shardsCache: c,
	}
}

func (sm *ShardManager) ClearCache() {
	sm.shardsCache.Purge()
}

func (sm *ShardManager) GetShard() {
}

func (sm *ShardManager) PutShard() {
}

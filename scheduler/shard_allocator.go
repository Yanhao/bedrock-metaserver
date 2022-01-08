package scheduler

import (
	"sync"

	"sr.ht/moyanhao/bedrock-metaserver/metadata"
)

type ShardAllocator struct {
}

func NewShardAllocator() *ShardAllocator {
	return &ShardAllocator{}
}

var (
	shardAllocator    *ShardAllocator
	shardAllcatorOnce sync.Once
)

func GetShardAllocator() *ShardAllocator {
	shardAllcatorOnce.Do(func() {
		shardAllocator = NewShardAllocator()
	})

	return shardAllocator
}

func (sa *ShardAllocator) AllocatorNewStorage() []*metadata.Shard {
	panic("todo")
}

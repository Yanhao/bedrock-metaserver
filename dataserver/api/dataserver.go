package api

type DsApi interface {
	DeleteShard(shardID uint64) error
	CreateShard(shardID uint64) error
	TransferShard(shardID uint64, toAddr string) error
	TransferShardLeader(shardID uint64, newLeader string) error
	RepairShard(shardID uint64, leader string) error
}

func NewDataServerApi() *DsApi {
	panic("todo")
}

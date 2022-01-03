package metadata

func GetShard(shardID ShardID) (*Shard, error) {
	panic("todo")
}

func PutShard(shard *Shard) error {
	return nil
}

func GetDataServer(addr string) (*DataServer, error) {
	panic("todo")
}

func PutDataServer(dataserver *DataServer) error {
	panic("todo")
}

func GetShardsInDataServer(addr string) ([]ShardID, error) {
	panic("todo")
}

func GetShardsInStorage(storageID StorageID) ([]ShardID, error) {
	panic("todo")
}

func IsShardInDataServer(shardID ShardID) (bool, error) {
	panic("todo")
}

func HasDataServer(addr string) (bool, error) {
	panic("todo")
}

func HasStorage(storageID StorageID) (bool, error) {
	panic("todo")
}

package metadata

import "fmt"

const (
	KvPrefixDataServer         = "/dataservers/"
	KvPrefixStorage            = "/storages/"
	KvPrefixShard              = "/shards/"
	KvPrefixShardsInDataServer = "/shards/dataservers/"
	KvPrefixShardsInStorage    = "/shards/storages/"
)

// const (
// 	KvShardsInDataServerTpl = KvPrefixShardsInDataServer + "%s/"
// 	KvShardsInStorageTpl    = KvPrefixShardsInStorage + "%d/"
// )

func ShardKey(shardID ShardID) string {
	return fmt.Sprintf("%s%d", KvPrefixShard, shardID)
}

func StorageKey(storageID StorageID) string {
	return fmt.Sprintf("%s%d", KvPrefixStorage, storageID)
}

func DataServerKey(addr string) string {
	return fmt.Sprintf("%s%s", KvPrefixDataServer, addr)
}

func DataServerPrefixKey(addr string) string {
	return fmt.Sprintf("%s%s/", KvPrefixShardsInDataServer, addr)
}

func StoragePrefixKey(strageID StorageID) string {
	return fmt.Sprintf("%s%d/", KvPrefixShardsInStorage, strageID)
}

func ShardInDataServerKey(addr string, shardID ShardID) string {
	return fmt.Sprintf("%s%d", DataServerPrefixKey(addr), shardID)
}

func ShardInStorageKey(storageID StorageID, shardID ShardID) string {
	return fmt.Sprintf("%s%d", StoragePrefixKey(storageID), shardID)
}

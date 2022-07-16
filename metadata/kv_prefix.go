package metadata

import (
	"fmt"
	"strings"
)

const (
	KvPrefixIdc        = "/idcs/"
	KvPrefixDataServer = "/dataservers/"
	KvPrefixStorage    = "/storages/"
	KvPrefixShard      = "/shards/"

	KvPrefixDataServerInIdc    = "/dataservers_in_idc/"
	KvPrefixShardsInDataServer = "/shards_in_dataserver/"
	KvPrefixShardsInStorage    = "/shards_in_storage/"

	KvPrefixMarkDeletedStorageID = "/deleted_storages/"
)

// const (
// 	KvShardsInDataServerTpl = KvPrefixShardsInDataServer + "%s/"
// 	KvShardsInStorageTpl    = KvPrefixShardsInStorage + "%d/"
// )

func IdcKey(idc string) string {
	return fmt.Sprintf("%s%s", KvPrefixIdc, strings.TrimSpace(idc))
}

func StorageKey(storageID StorageID) string {
	return fmt.Sprintf("%s0x%08x", KvPrefixStorage, storageID)
}

func DeletedStorageKey(storageID StorageID) string {
	return fmt.Sprintf("%s0x%08x", KvPrefixMarkDeletedStorageID, storageID)
}

func DataServerKey(addr string) string {
	return fmt.Sprintf("%s%s", KvPrefixDataServer, strings.TrimSpace(addr))
}

func ShardKey(shardID ShardID) string {
	return fmt.Sprintf("%s0x%08x", KvPrefixShard, shardID)
}

// ---------------------------------------------------------------------

func DataServerInIdcPrefixKey(idc string) string {
	return fmt.Sprintf("%s%s/", KvPrefixDataServerInIdc, idc)
}

func ShardInDataServerPrefixKey(addr string) string {
	return fmt.Sprintf("%s%s/", KvPrefixShardsInDataServer, addr)
}

func ShardInStoragePrefixKey(storageID StorageID) string {
	return fmt.Sprintf("%s%d/", KvPrefixShardsInStorage, storageID)
}

// ---------------------------------------------------------------------

func DataServerInIdcKey(idc, addr string) string {
	return fmt.Sprintf("%s%s", DataServerInIdcPrefixKey(idc), addr)
}

func ShardInDataServerKey(addr string, shardID ShardID) string {
	return fmt.Sprintf("%s%d", ShardInDataServerPrefixKey(addr), shardID)
}

func ShardInStorageKey(storageID StorageID, shardID ShardID) string {
	return fmt.Sprintf("%s%d", ShardInStoragePrefixKey(storageID), shardID)
}

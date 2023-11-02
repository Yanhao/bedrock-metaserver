package main

import (
	_ "errors"

	"sr.ht/moyanhao/bedrock-metaserver/clients/metaserver"
)

func HeartBeatParamCheck(req *metaserver.HeartBeatRequest) error {
	return nil
}

func ScanStorageShardsParamCheck(req *metaserver.ScanShardRangeRequest) error {
	return nil
}

func InfoParamCheck(req *metaserver.InfoRequest) error {
	return nil
}

func CreateStorageParamCheck(req *metaserver.CreateStorageRequest) error {
	return nil
}

func DeleteStorageParamCheck(req *metaserver.DeleteStorageRequest) error {
	return nil
}

func UndeleteStorageParamCheck(req *metaserver.UndeleteStorageRequest) error {
	return nil
}

func RenameStorageParamCheck(req *metaserver.RenameStorageRequest) error {
	return nil
}

func ResizeStorageParamCheck(req *metaserver.ResizeStorageRequest) error {
	return nil
}

func GetStoragesParamCheck(req *metaserver.StorageInfoRequest) error {
	return nil
}

func AddDataServerParamCheck(req *metaserver.AddDataServerRequest) error {
	return nil
}

func RemoveDataServerParamCheck(req *metaserver.RemoveDataServerRequest) error {
	return nil
}

func ListDataServerParamCheck(req *metaserver.ListDataServerRequest) error {
	return nil
}

func UpdateDataServerParamCheck(req *metaserver.UpdateDataServerRequest) error {
	return nil
}

func ShardInfoParamCheck(req *metaserver.ShardInfoRequest) error {
	return nil
}

func CreateShardParamCheck(req *metaserver.CreateShardRequest) error {
	return nil
}

func RemoveShardParamCheck(req *metaserver.RemoveShardRequest) error {
	return nil
}

func AllocateTxIDsParamCheck(req *metaserver.AllocateTxidsRequest) error {
	return nil
}

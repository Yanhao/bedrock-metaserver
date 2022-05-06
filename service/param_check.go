package service

import (
	_ "errors"

	"sr.ht/moyanhao/bedrock-metaserver/proto"
)

func HeartBeatParamCheck(req *proto.HeartBeatRequest) error {
	return nil
}

func GetShardRoutesParamCheck(req *proto.GetShardRoutesRequest) error {
	return nil
}

func CreateStorageParamCheck(req *proto.CreateStorageRequest) error {
	return nil
}

func DeleteStorageParamCheck(req *proto.DeleteStorageRequest) error {
	return nil
}

func RenameStorageParamCheck(req *proto.RenameStorageRequest) error {
	return nil
}

func ResizeStorageParamCheck(req *proto.ResizeStorageRequest) error {
	return nil
}

func GetStoragesParamCheck(req *proto.GetStoragesRequest) error {
	return nil
}

func AddDataServerParamCheck(req *proto.AddDataServerRequest) error {
	return nil
}

func RemoveDataServerParamCheck(req *proto.RemoveDataServerRequest) error {
	return nil
}

func ListDataServerParamCheck(req *proto.ListDataServerRequest) error {
	return nil
}

func UpdateDataServerParamCheck(req *proto.UpdateDataServerRequest) error {
	return nil
}

func ShardInfoParamCheck(req *proto.ShardInfoRequest) error {
	return nil
}

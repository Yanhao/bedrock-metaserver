package service

import (
	_ "errors"
)

func HeartBeatParamCheck(req *HeartBeatRequest) error {
	return nil
}

func GetShardRoutesParamCheck(req *GetShardRoutesRequest) error {
	return nil
}

func CreateStorageParamCheck(req *CreateStorageRequest) error {
	return nil
}

func DeleteStorageParamCheck(req *DeleteStorageRequest) error {
	return nil
}

func UndeleteStorageParamCheck(req *UndeleteStorageRequest) error {
	return nil
}

func RenameStorageParamCheck(req *RenameStorageRequest) error {
	return nil
}

func ResizeStorageParamCheck(req *ResizeStorageRequest) error {
	return nil
}

func GetStoragesParamCheck(req *GetStoragesRequest) error {
	return nil
}

func AddDataServerParamCheck(req *AddDataServerRequest) error {
	return nil
}

func RemoveDataServerParamCheck(req *RemoveDataServerRequest) error {
	return nil
}

func ListDataServerParamCheck(req *ListDataServerRequest) error {
	return nil
}

func UpdateDataServerParamCheck(req *UpdateDataServerRequest) error {
	return nil
}

func ShardInfoParamCheck(req *ShardInfoRequest) error {
	return nil
}

func CreateShardParamCheck(req *CreateShardRequest) error {
	return nil
}

func RemoveShardParamCheck(req *RemoveShardRequest) error {
	return nil
}

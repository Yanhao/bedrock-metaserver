package dataserver

import (
	"context"
	"errors"

	grpc "google.golang.org/grpc"
)

type DsApi interface {
	DeleteShard(shardID uint64, storageID uint64) error
	CreateShard(shardID uint64, storageID uint64) error

	// TransferShard(shardID uint64, toAddr string) error
	// PullShardData(shardID uint64, leader string) error
	TransferShardLeader(shardID uint64, replicates []string) error

	// RepairShard(shardID uint64, leader string) error

	Close()
}

type DataServerApi struct {
	addr     string
	client   DataServiceClient
	grpcConn *grpc.ClientConn
}

func NewDataServerApi(addr string) (DsApi, error) {
	conn, err := grpc.Dial(addr)
	if err != nil {
		return nil, err
	}

	c := NewDataServiceClient(conn)

	ret := &DataServerApi{
		addr:     addr,
		client:   c,
		grpcConn: conn,
	}

	return ret, nil
}

func (ds *DataServerApi) Close() {
	_ = ds.grpcConn.Close()
}

func (ds *DataServerApi) CreateShard(shardID uint64, storageID uint64) error {
	req := &CreateShardRequest{
		ShardId:   shardID,
		StorageId: storageID,
	}

	resp, err := ds.client.CreateShard(context.TODO(), req)
	if err != nil || resp == nil {
		return errors.New("")
	}

	return nil
}

func (ds *DataServerApi) DeleteShard(shardID uint64, storageID uint64) error {
	req := &DeleteShardRequest{
		ShardId: shardID,
	}

	resp, err := ds.client.DeleteShard(context.TODO(), req)
	if err != nil || resp == nil {
		return errors.New("")
	}

	return nil
}

func (ds *DataServerApi) TransferShardLeader(shardID uint64, replicates []string) error {
	req := &TransferShardLeaderRequest{
		ShardId:    shardID,
		Replicates: replicates,
	}

	resp, err := ds.client.TransferShardLeader(context.TODO(), req)
	if err != nil || resp == nil {
		return errors.New("")
	}

	return nil
}

// func (ds *DataServerApi) PullShardData(shardID uint64, leader string) error {
// 	req := &PullShardDataRequest{}

// 	resp, err := ds.client.PullShardData(context.TODO(), req)
// 	if err != nil || resp == nil {

// 	}

// 	return nil
// }

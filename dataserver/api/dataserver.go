package api

import (
	"context"

	grpc "google.golang.org/grpc"
)

type DsApi interface {
	DeleteShard(shardID uint64) error
	CreateShard(shardID uint64) error

	// TransferShard(shardID uint64, toAddr string) error
	PullShardData(shardID uint64, leader string) error
	TransferShardLeader(shardID uint64, newLeader string) error

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

func (ds *DataServerApi) CreateShard(shardID uint64) error {
	req := &CreateShardRequest{}

	resp, err := ds.client.CreateShard(context.TODO(), req)
	if err != nil || resp == nil {

	}

	return nil
}

func (ds *DataServerApi) DeleteShard(shardID uint64) error {
	req := &DeleteShardRequest{}

	resp, err := ds.client.DeleteShard(context.TODO(), req)
	if err != nil || resp == nil {

	}

	return nil
}

func (ds *DataServerApi) TransferShardLeader(shardID uint64, newLeader string) error {
	req := &TransferShardLeaderRequest{}

	resp, err := ds.client.TransferShardLeader(context.TODO(), req)
	if err != nil || resp == nil {

	}

	return nil
}

func (ds *DataServerApi) PullShardData(shardID uint64, leader string) error {
	req := &PullShardDataRequest{}

	resp, err := ds.client.PullShardData(context.TODO(), req)
	if err != nil || resp == nil {

	}

	return nil
}

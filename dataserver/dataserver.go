package dataserver

import (
	"context"
	"errors"

	log "github.com/sirupsen/logrus"
	grpc "google.golang.org/grpc"
	"google.golang.org/protobuf/types/known/timestamppb"
)

type DsApi interface {
	DeleteShard(shardID uint64) error
	CreateShard(shardID uint64) error

	// TransferShard(shardID uint64, toAddr string) error
	// PullShardData(shardID uint64, leader string) error
	TransferShardLeader(shardID uint64, replicates []string) error

	SplitShard(shardID uint64) error
	MergeShard(aShardID uint64, bShardID uint64) error
	MigrateShard(shardID uint64, targetShardID uint64, targetAddress string) error

	// RepairShard(shardID uint64, leader string) error

	Close()
}

type DataServerApi struct {
	addr     string
	client   DataServiceClient
	grpcConn *grpc.ClientConn
}

func NewDataServerApi(addr string) (DsApi, error) {
	conn, err := grpc.Dial(addr, grpc.WithInsecure())
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
	now := timestamppb.Now()

	req := &CreateShardRequest{
		ShardId:         shardID,
		CreateTs:        now,
		Leader:          "",
		LeaderChangeTs:  now,
		ReplicaUpdateTs: now,
		Replicates:      []string{},
	}

	resp, err := ds.client.CreateShard(context.TODO(), req)
	if err != nil || resp == nil {
		log.Errorf("failed to create shard, err: %v", err)
		return err
	}

	return nil
}

func (ds *DataServerApi) DeleteShard(shardID uint64) error {
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
		ShardId:        shardID,
		Replicates:     replicates,
		LeaderChangeTs: timestamppb.Now(),
	}

	resp, err := ds.client.TransferShardLeader(context.TODO(), req)
	if err != nil || resp == nil {
		return err
	}

	return nil
}

func (ds *DataServerApi) SplitShard(shardID uint64) error {
	req := &SplitShardRequest{
		ShardId: shardID,
	}

	resp, err := ds.client.SplitShard(context.TODO(), req)
	if err != nil || resp == nil {
		return err
	}

	return nil
}

func (ds *DataServerApi) MergeShard(aShardID uint64, bShardID uint64) error {
	req := &MergeShardRequest{
		ShardIdA: aShardID,
		ShardIdB: bShardID,
	}

	resp, err := ds.client.MergeShard(context.TODO(), req)
	if err != nil || resp == nil {
		return err
	}

	return nil
}

func (ds *DataServerApi) MigrateShard(shardID uint64, targetShardID uint64, targetAddress string) error {
	req := &MigrateShardRequest{
		ShardIdFrom:   shardID,
		ShardIdTo:     targetShardID,
		TargetAddress: targetAddress,
		Entries:       []*MigrateShardRequest_Entry{},
	}

	cli, err := ds.client.MigrateShard(context.TODO())
	if err != nil {
		return err
	}

	err = cli.Send(req)
	if err != nil {
		return err
	}

	return nil
}

package service

import (
	"context"
	"time"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	emptypb "google.golang.org/protobuf/types/known/emptypb"

	"sr.ht/moyanhao/bedrock-metaserver/common/log"
	"sr.ht/moyanhao/bedrock-metaserver/messages"
	"sr.ht/moyanhao/bedrock-metaserver/metadata"
)

type MetaService struct {
	messages.UnimplementedMetaServiceServer
}

func (m *MetaService) HeartBeat(ctx context.Context, req *messages.HeartBeatRequest) (resp *emptypb.Empty, err error) {
	server, ok := metadata.DataServers[req.Addr]
	if !ok {
		log.Warn("no such server in record: %s", req.Addr)
		return nil, status.Errorf(codes.NotFound, "no such server: %s", req.Addr)
	}

	server.MarkActive(true)

	return &emptypb.Empty{}, nil
}

func getUpdatedRoute(shardID metadata.ShardID, ts time.Time) (*messages.RouteRecord, error) {
	sm := metadata.GetShardManager()

	shard, err := sm.GetShard(shardID)
	if err != nil {
		log.Error("get shard failed, shardID=%d", shard)
		return nil, status.Errorf(codes.Internal, "get shard failed")
	}

	if shard.ReplicaUpdateTs.After(ts) {
		route := &messages.RouteRecord{
			ShardId: uint64(shardID),
		}
		for rep := range shard.Replicates {
			route.Addrs = append(route.Addrs, rep)
		}

		return route, nil
	}
	return nil, nil
}

func (m *MetaService) GetShardRoutes(ctx context.Context, req *messages.GetShardRoutesRequest) (*messages.GetShardRoutesResponse, error) {
	ts := req.GetTimestamp().AsTime()

	resp := &messages.GetShardRoutesResponse{}

	if req.GetShardRange() != nil {
		begin := req.GetShardRange().StartShardId
		end := begin + req.GetShardRange().Offset

		for shardID := begin; shardID <= end; shardID++ {
			route, err := getUpdatedRoute(metadata.ShardID(shardID), ts)
			if err != nil {
				return nil, err
			}

			resp.Routes = append(resp.Routes, route)
		}
	} else if req.GetShardsList() != nil {
		for _, shardID := range req.GetShardsList().GetShardIds() {
			route, err := getUpdatedRoute(metadata.ShardID(shardID), ts)
			if err != nil {
				return nil, err
			}

			resp.Routes = append(resp.Routes, route)
		}
	}

	return resp, nil
}

func (m *MetaService) CreateStorage(ctx context.Context, req *messages.CreateStorageRequest) (*messages.CreateStorageResponse, error) {
	panic("")
}

func (m *MetaService) DeleteStorage(ctx context.Context, req *messages.DeleteStorageRequest) (*messages.DeleteStorageResponse, error) {
	panic("")
}

func (m *MetaService) RenameStorage(ctx context.Context, req *messages.RenameStorageRequest) (*messages.RenameStorageResponse, error) {
	panic("")
}

func (m *MetaService) ResizeStorage(ctx context.Context, req *messages.ResizeStorageRequest) (*messages.ResizeStorageResponse, error) {
	panic("")
}

func (m *MetaService) GetStorages(ctx context.Context, req *messages.GetStoragesRequest) (*messages.GetStoragesResponse, error) {
	panic("")
}

func (m *MetaService) AddDataServer(ctx context.Context, req *messages.AddDataServerRequest) (*messages.AddDataServerResponse, error) {
	panic("")
}

func (m *MetaService) RemoveDataServer(ctx context.Context, req *messages.RemoveDataServerRequest) (*messages.RemoveDataServerResponse, error) {
	panic("")
}

func (m *MetaService) ListDataServer(ctx context.Context, req *messages.ListDataServerRequest) (*messages.ListDataServerResponse, error) {
	panic("")
}

func (m *MetaService) UpdateDataServer(ctx context.Context, req *messages.UpdateDataServerRequest) (*messages.UpdateDataServerResponse, error) {
	panic("")
}

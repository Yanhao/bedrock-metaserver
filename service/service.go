package service

import (
	"context"
	"net"
	"time"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	emptypb "google.golang.org/protobuf/types/known/emptypb"

	"sr.ht/moyanhao/bedrock-metaserver/common/log"
	"sr.ht/moyanhao/bedrock-metaserver/messages"
	"sr.ht/moyanhao/bedrock-metaserver/metadata"
	"sr.ht/moyanhao/bedrock-metaserver/scheduler"
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
	resp := &messages.CreateStorageResponse{}

	storage, err := metadata.CreateNewStorage()
	if err != nil {
		log.Error("create storage failed, err: %v", err)
		return resp, status.Errorf(codes.Internal, "create storage failed")
	}

	resp.Id = uint64(storage.ID)

	return resp, nil
}

func (m *MetaService) DeleteStorage(ctx context.Context, req *messages.DeleteStorageRequest) (*messages.DeleteStorageResponse, error) {
	resp := &messages.DeleteStorageResponse{}

	for _, id := range req.Ids {
		err := metadata.StorageDelete(metadata.StorageID(id))
		if err != nil {
			log.Error("create storage failed, err: %v", err)
			return resp, status.Errorf(codes.Internal, "create storage failed")
		}
	}

	return resp, nil
}

func (m *MetaService) RenameStorage(ctx context.Context, req *messages.RenameStorageRequest) (*messages.RenameStorageResponse, error) {
	resp := &messages.RenameStorageResponse{}

	err := metadata.StorageRename(metadata.StorageID(req.Id), req.NewName)
	if err != nil {
		log.Error("create storage failed, err: %v", err)
		return resp, status.Errorf(codes.Internal, "create storage failed")
	}

	return resp, nil
}

func (m *MetaService) ResizeStorage(ctx context.Context, req *messages.ResizeStorageRequest) (*messages.ResizeStorageResponse, error) {
	panic("impletment me!")
}

func (m *MetaService) GetStorages(ctx context.Context, req *messages.GetStoragesRequest) (*messages.GetStoragesResponse, error) {
	resp := &messages.GetStoragesResponse{}

	for _, id := range req.Ids {
		st, err := metadata.GetStorageManager().GetStorage(metadata.StorageID(id))
		if err != nil {
			return nil, status.Errorf(codes.Internal, "")
		}

		resp.Storages = append(resp.Storages, &messages.Storage{
			Id:   uint64(st.ID),
			Name: st.Name,
		})
	}

	for _, name := range req.Names {
		_ = name
	}

	return resp, nil
}

func (m *MetaService) AddDataServer(ctx context.Context, req *messages.AddDataServerRequest) (*messages.AddDataServerResponse, error) {
	resp := &messages.AddDataServerResponse{}

	ip, port, err := net.SplitHostPort(req.Addr)
	if err != nil {
		return nil, status.Errorf(codes.InvalidArgument, "")
	}
	err = metadata.DataServerAdd(ip, port)
	if err != nil {
		return nil, status.Errorf(codes.Internal, "")
	}

	return resp, nil
}

func (m *MetaService) RemoveDataServer(ctx context.Context, req *messages.RemoveDataServerRequest) (*messages.RemoveDataServerResponse, error) {
	resp := &messages.RemoveDataServerResponse{}
	_, _, err := net.SplitHostPort(req.Addr)
	if err != nil {
		return nil, status.Errorf(codes.InvalidArgument, "")
	}

	err = scheduler.ClearDataserver(req.Addr)
	if err != nil {
		return nil, status.Errorf(codes.Internal, "")
	}
	metadata.DataServerRemove(req.Addr)

	return resp, nil
}

func (m *MetaService) ListDataServer(ctx context.Context, req *messages.ListDataServerRequest) (*messages.ListDataServerResponse, error) {
	resp := &messages.ListDataServerResponse{}

	for _, ds := range metadata.DataServers {
		_ = ds
		resp.DataServers = append(resp.DataServers, &messages.DataServer{})
	}

	return resp, nil
}

func (m *MetaService) UpdateDataServer(ctx context.Context, req *messages.UpdateDataServerRequest) (*messages.UpdateDataServerResponse, error) {
	panic("")
}

func (m *MetaService) ShardInfo(ctx context.Context, req *messages.ShardInfoRequest) (*messages.ShardInfoResponse, error) {
	panic("")
}

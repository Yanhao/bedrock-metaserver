package service

import (
	"context"
	"net"
	"time"

	"github.com/jinzhu/copier"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/types/known/emptypb"
	"google.golang.org/protobuf/types/known/timestamppb"

	"sr.ht/moyanhao/bedrock-metaserver/common/log"
	"sr.ht/moyanhao/bedrock-metaserver/metadata"
	"sr.ht/moyanhao/bedrock-metaserver/proto"
	"sr.ht/moyanhao/bedrock-metaserver/scheduler"
)

type MetaService struct {
	proto.UnimplementedMetaServiceServer
}

func (m *MetaService) HeartBeat(ctx context.Context, req *proto.HeartBeatRequest) (*emptypb.Empty, error) {
	err := HeartBeatParamCheck(req)
	if err != nil {
		log.Warn("HeartBeat: invalid arguments, err: %v", err)
		return nil, status.Errorf(codes.InvalidArgument, err.Error())
	}

	server, err := metadata.GetDataServerByAddr(req.Addr)
	if err != nil {
		log.Warn("no such server in record: %s", req.Addr)
		return nil, status.Errorf(codes.NotFound, "no such server: %s", req.Addr)
	}

	server.HeartBeat()

	log.Debug("receive heartbeat from %s", req.Addr)

	return &emptypb.Empty{}, nil
}

func getUpdatedRoute(shardID metadata.ShardID, ts time.Time) (*proto.RouteRecord, error) {
	sm := metadata.GetShardManager()

	shard, err := sm.GetShard(shardID)
	if err != nil {
		log.Error("get shard failed, shardID=%d", shard)
		return nil, status.Errorf(codes.Internal, "get shard failed")
	}

	if shard.ReplicaUpdateTs.After(ts) {
		route := &proto.RouteRecord{
			ShardId: uint64(shardID),
		}
		for rep := range shard.Replicates {
			route.Addrs = append(route.Addrs, rep)
		}

		return route, nil
	}

	return nil, nil
}

func (m *MetaService) GetShardRoutes(ctx context.Context, req *proto.GetShardRoutesRequest) (*proto.GetShardRoutesResponse, error) {
	err := GetShardRoutesParamCheck(req)
	if err != nil {
		log.Warn("GetShardRoutes: invalid arguments, err: %v", err)
		return nil, status.Errorf(codes.InvalidArgument, err.Error())
	}

	resp := &proto.GetShardRoutesResponse{}

	ts := req.GetTimestamp().AsTime()
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

func (m *MetaService) CreateStorage(ctx context.Context, req *proto.CreateStorageRequest) (*proto.CreateStorageResponse, error) {
	err := CreateStorageParamCheck(req)
	if err != nil {
		log.Warn("CreateStorage: invalid arguments, err: %v", err)
		return nil, status.Errorf(codes.InvalidArgument, err.Error())
	}

	resp := &proto.CreateStorageResponse{}

	storage, err := scheduler.GetShardAllocator().AllocatorNewStorage()
	if err != nil {
		log.Error("create storage failed, err: %v", err)
		return resp, status.Errorf(codes.Internal, "create storage failed")
	}

	resp.Id = uint64(storage.ID)

	return resp, nil
}

func (m *MetaService) DeleteStorage(ctx context.Context, req *proto.DeleteStorageRequest) (*proto.DeleteStorageResponse, error) {
	err := DeleteStorageParamCheck(req)
	if err != nil {
		log.Warn("DeleteStorage: invalid arguments, err: %v", err)
		return nil, status.Errorf(codes.InvalidArgument, err.Error())
	}

	resp := &proto.DeleteStorageResponse{}

	var recycleTime time.Duration
	if req.RealDelete {
		recycleTime = time.Minute * 10
	} else {
		if req.RecycleAfter < 30 {
			recycleTime = time.Minute * 30
		} else {
			recycleTime = time.Minute * time.Duration(req.RecycleAfter)
		}
	}

	err = metadata.GetStorageManager().StorageDelete(metadata.StorageID(req.Id), recycleTime)
	if err != nil {
		log.Error("create storage failed, err: %v", err)
		return resp, status.Errorf(codes.Internal, "create storage failed")
	}

	return resp, nil
}

func (m *MetaService) RenameStorage(ctx context.Context, req *proto.RenameStorageRequest) (*proto.RenameStorageResponse, error) {
	err := RenameStorageParamCheck(req)
	if err != nil {
		log.Warn("RenameStorage: invalid arguments, err: %v", err)
		return nil, status.Errorf(codes.InvalidArgument, err.Error())
	}

	resp := &proto.RenameStorageResponse{}

	err = metadata.GetStorageManager().StorageRename(metadata.StorageID(req.Id), req.NewName)
	if err != nil {
		log.Error("create storage failed, err: %v", err)
		return resp, status.Errorf(codes.Internal, "create storage failed")
	}

	return resp, nil
}

func (m *MetaService) ResizeStorage(ctx context.Context, req *proto.ResizeStorageRequest) (*proto.ResizeStorageResponse, error) {
	err := ResizeStorageParamCheck(req)
	if err != nil {
		log.Warn("ResizeStorage: invalid arguments, err: %v", err)
		return nil, status.Errorf(codes.InvalidArgument, err.Error())
	}

	resp := &proto.ResizeStorageResponse{}

	st, err := metadata.GetStorageManager().GetStorage(metadata.StorageID(req.Id))
	if err != nil {
		return nil, status.Errorf(codes.InvalidArgument, "")
	}

	if st.LastShardIndex >= uint32(req.NewShardCount) {
		return nil, status.Errorf(codes.InvalidArgument, "")
	}

	expandCount := req.NewShardCount - uint64(st.LastShardIndex)
	err = scheduler.GetShardAllocator().ExpandStorage(st, uint32(expandCount))
	if err != nil {
		return nil, status.Errorf(codes.Internal, "")
	}

	st.LastShardIndex = uint32(req.NewShardCount)

	var updatedSt metadata.Storage
	err = copier.CopyWithOption(&updatedSt, st, copier.Option{IgnoreEmpty: true, DeepCopy: true})
	if err != nil {
		return nil, status.Errorf(codes.Internal, "")
	}

	err = metadata.GetStorageManager().SaveStorage(&updatedSt)
	if err != nil {
		return nil, status.Errorf(codes.Internal, "")
	}

	return resp, nil
}

func (m *MetaService) GetStorages(ctx context.Context, req *proto.GetStoragesRequest) (*proto.GetStoragesResponse, error) {
	err := GetStoragesParamCheck(req)
	if err != nil {
		log.Warn("GetStorages: invalid arguments, err: %v", err)
		return nil, status.Errorf(codes.InvalidArgument, err.Error())
	}

	resp := &proto.GetStoragesResponse{}

	for _, id := range req.Ids {
		st, err := metadata.GetStorageManager().GetStorage(metadata.StorageID(id))
		if err != nil {
			return nil, status.Errorf(codes.Internal, "")
		}

		resp.Storages = append(resp.Storages, &proto.Storage{
			Id:        uint64(st.ID),
			Name:      st.Name,
			CreateTs:  timestamppb.New(st.CreateTs),
			DeletedTs: timestamppb.New(st.DeleteTs),
			Owner:     st.Owner,
		})
	}

	for _, name := range req.Names {
		_ = name
	}

	return resp, nil
}

func (m *MetaService) AddDataServer(ctx context.Context, req *proto.AddDataServerRequest) (*proto.AddDataServerResponse, error) {
	err := AddDataServerParamCheck(req)
	if err != nil {
		log.Warn("AddDataServer: invalid arguments, err: %v", err)
		return nil, status.Errorf(codes.InvalidArgument, err.Error())
	}

	resp := &proto.AddDataServerResponse{}

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

func (m *MetaService) RemoveDataServer(ctx context.Context, req *proto.RemoveDataServerRequest) (*proto.RemoveDataServerResponse, error) {
	err := RemoveDataServerParamCheck(req)
	if err != nil {
		log.Warn("RemoveDataServer: invalid arguments, err: %v", err)
		return nil, status.Errorf(codes.InvalidArgument, err.Error())
	}

	resp := &proto.RemoveDataServerResponse{}
	_, _, err = net.SplitHostPort(req.Addr)
	if err != nil {
		return nil, status.Errorf(codes.InvalidArgument, "")
	}

	if !metadata.IsDataServerExists(req.Addr) {
		return nil, status.Errorf(codes.NotFound, "")
	}

	go func() {
		err = scheduler.ClearDataserver(req.Addr)
		if err != nil {
			return
		}
		err = metadata.DataServerRemove(req.Addr)
		if err != nil {
			return
		}
	}()

	return resp, nil
}

func (m *MetaService) ListDataServer(ctx context.Context, req *proto.ListDataServerRequest) (*proto.ListDataServerResponse, error) {
	err := ListDataServerParamCheck(req)
	if err != nil {
		log.Warn("ListDataServer: invalid arguments, err: %v", err)
		return nil, status.Errorf(codes.InvalidArgument, err.Error())
	}

	resp := &proto.ListDataServerResponse{}

	dss := metadata.DataServersClone()

	for _, ds := range dss {
		_ = ds
		resp.DataServers = append(resp.DataServers,
			&proto.DataServer{
				Ip:              ds.Ip,
				Port:            ds.Port,
				Capacity:        ds.Capacity,
				Free:            ds.Free,
				LastHeartbeatTs: timestamppb.New(ds.LastHeartBeatTs),
				Status: func(s metadata.LiveStatus) string {
					switch s {
					case metadata.LiveStatusActive:
						return "active"
					case metadata.LiveStatusOffline:
						return "offline"
					case metadata.LiveStatusInactive:
						return "inactive"
					default:
						return "unknown"
					}
				}(ds.Status),
			})
	}

	return resp, nil
}

func (m *MetaService) UpdateDataServer(ctx context.Context, req *proto.UpdateDataServerRequest) (*proto.UpdateDataServerResponse, error) {
	err := UpdateDataServerParamCheck(req)
	if err != nil {
		log.Warn("UpdateDataServer: invalid arguments, err: %v", err)
		return nil, status.Errorf(codes.InvalidArgument, err.Error())
	}

	resp := &proto.UpdateDataServerResponse{}

	return resp, nil
}

func (m *MetaService) ShardInfo(ctx context.Context, req *proto.ShardInfoRequest) (*proto.ShardInfoResponse, error) {
	if err := ShardInfoParamCheck(req); err != nil {
		log.Warn("ShardInfo: invalid arguments, err: %v", err)
		return nil, status.Errorf(codes.InvalidArgument, err.Error())
	}

	resp := &proto.ShardInfoResponse{}

	sm := metadata.GetShardManager()
	if sm == nil {
		return resp, status.Errorf(codes.Internal, "")
	}

	shard, err := sm.GetShard(metadata.ShardID(req.GetId()))
	if err != nil {
		return resp, status.Errorf(codes.Internal, "")
	}

	resp = &proto.ShardInfoResponse{
		Shard: &proto.Shard{
			Id:              uint64(shard.ID),
			StorageId:       uint64(shard.SID),
			ReplicaUpdateTs: timestamppb.New(shard.ReplicaUpdateTs),
			Replicates: func(repSet map[string]struct{}) []string {
				var ret []string
				for rep := range repSet {
					ret = append(ret, rep)
				}

				return ret
			}(shard.Replicates),
			IsDeleted:      shard.IsDeleted,
			DeletedTs:      timestamppb.New(shard.DeleteTs),
			Leader:         shard.Leader,
			LeaderChangeTs: timestamppb.New(shard.LeaderChangeTs),
		},
	}

	return resp, nil
}

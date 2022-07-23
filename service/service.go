package service

import (
	"context"
	"net"
	"time"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/types/known/emptypb"
	"google.golang.org/protobuf/types/known/timestamppb"

	"sr.ht/moyanhao/bedrock-metaserver/common/log"
	"sr.ht/moyanhao/bedrock-metaserver/kv"
	"sr.ht/moyanhao/bedrock-metaserver/metadata"
	"sr.ht/moyanhao/bedrock-metaserver/scheduler"
)

type MetaService struct {
	UnimplementedMetaServiceServer
}

func (m *MetaService) HeartBeat(ctx context.Context, req *HeartBeatRequest) (*emptypb.Empty, error) {
	err := HeartBeatParamCheck(req)
	if err != nil {
		log.Warn("HeartBeat: invalid arguments, err: %v", err)
		return nil, status.Errorf(codes.InvalidArgument, err.Error())
	}

	if !kv.IsMetaServerLeader() {
		leader := kv.GetMetaServerLeader()
		mscli, _ := GetMetaServerConns().GetClient(leader)
		return mscli.HeartBeat(ctx, req)
	}

	dm := metadata.GetDataServerManager()

	server, err := dm.GetDataServer(req.Addr)
	if err != nil {
		log.Warn("no such server in record: %s", req.Addr)
		return nil, status.Errorf(codes.NotFound, "no such server: %s", req.Addr)
	}

	server.HeartBeat()

	log.Info("receive heartbeat from %s", req.Addr)

	return &emptypb.Empty{}, nil
}

func getUpdatedRoute(shardID metadata.ShardID, ts time.Time) (*RouteRecord, error) {
	sm := metadata.GetShardManager()

	shard, err := sm.GetShard(shardID)
	if err != nil {
		log.Error("get shard failed, shardID=%d", shard)
		return nil, status.Errorf(codes.Internal, "get shard failed")
	}

	if shard.ReplicaUpdateTs.After(ts) {
		route := &RouteRecord{
			ShardId: uint64(shardID),
		}
		for rep := range shard.Replicates {
			route.Addrs = append(route.Addrs, rep)
		}

		return route, nil
	}

	return nil, nil
}

func (m *MetaService) GetShardRoutes(ctx context.Context, req *GetShardRoutesRequest) (*GetShardRoutesResponse, error) {
	err := GetShardRoutesParamCheck(req)
	if err != nil {
		log.Warn("GetShardRoutes: invalid arguments, err: %v", err)
		return nil, status.Errorf(codes.InvalidArgument, err.Error())
	}

	if !kv.IsMetaServerLeader() {
		leader := kv.GetMetaServerLeader()
		mscli, _ := GetMetaServerConns().GetClient(leader)
		return mscli.GetShardRoutes(ctx, req)
	}

	resp := &GetShardRoutesResponse{}

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

func (m *MetaService) CreateStorage(ctx context.Context, req *CreateStorageRequest) (*CreateStorageResponse, error) {
	err := CreateStorageParamCheck(req)
	if err != nil {
		log.Warn("CreateStorage: invalid arguments, err: %v", err)
		return nil, status.Errorf(codes.InvalidArgument, err.Error())
	}

	if !kv.IsMetaServerLeader() {
		leader := kv.GetMetaServerLeader()
		mscli, _ := GetMetaServerConns().GetClient(leader)
		return mscli.CreateStorage(ctx, req)
	}

	resp := &CreateStorageResponse{}

	storage, err := scheduler.GetShardAllocator().AllocatorNewStorage()
	if err != nil {
		log.Error("create storage failed, err: %v", err)
		return resp, status.Errorf(codes.Internal, "create storage failed")
	}

	resp.Id = uint64(storage.ID)

	return resp, nil
}

func (m *MetaService) DeleteStorage(ctx context.Context, req *DeleteStorageRequest) (*DeleteStorageResponse, error) {
	err := DeleteStorageParamCheck(req)
	if err != nil {
		log.Warn("DeleteStorage: invalid arguments, err: %v", err)
		return nil, status.Errorf(codes.InvalidArgument, err.Error())
	}

	if !kv.IsMetaServerLeader() {
		leader := kv.GetMetaServerLeader()
		mscli, _ := GetMetaServerConns().GetClient(leader)
		return mscli.DeleteStorage(ctx, req)
	}

	resp := &DeleteStorageResponse{}

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
		log.Error("delete storage failed, err: %v", err)
		return resp, status.Errorf(codes.Internal, "delete storage failed")
	}

	return resp, nil
}

func (m *MetaService) UndeleteStorage(ctx context.Context, req *UndeleteStorageRequest) (*UndeleteStorageResponse, error) {
	err := UndeleteStorageParamCheck(req)
	if err != nil {
		log.Warn("UndeleteStorage: invalid arguments, err: %v", err)
		return nil, status.Errorf(codes.InvalidArgument, err.Error())
	}

	if !kv.IsMetaServerLeader() {
		leader := kv.GetMetaServerLeader()
		mscli, _ := GetMetaServerConns().GetClient(leader)
		return mscli.UndeleteStorage(ctx, req)
	}

	resp := &UndeleteStorageResponse{}

	err = metadata.GetStorageManager().StorageUndelete(metadata.StorageID(req.Id))
	if err != nil {
		log.Error("undelete storage failed, err: %v", err)
		return resp, status.Errorf(codes.Internal, "undelete storage failed")
	}

	return resp, nil
}

func (m *MetaService) RenameStorage(ctx context.Context, req *RenameStorageRequest) (*RenameStorageResponse, error) {
	err := RenameStorageParamCheck(req)
	if err != nil {
		log.Warn("RenameStorage: invalid arguments, err: %v", err)
		return nil, status.Errorf(codes.InvalidArgument, err.Error())
	}

	if !kv.IsMetaServerLeader() {
		leader := kv.GetMetaServerLeader()
		mscli, _ := GetMetaServerConns().GetClient(leader)
		return mscli.RenameStorage(ctx, req)
	}

	resp := &RenameStorageResponse{}

	err = metadata.GetStorageManager().StorageRename(metadata.StorageID(req.Id), req.NewName)
	if err != nil {
		log.Error("create storage failed, err: %v", err)
		return resp, status.Errorf(codes.Internal, "create storage failed")
	}

	return resp, nil
}

func (m *MetaService) ResizeStorage(ctx context.Context, req *ResizeStorageRequest) (*ResizeStorageResponse, error) {
	err := ResizeStorageParamCheck(req)
	if err != nil {
		log.Warn("ResizeStorage: invalid arguments, err: %v", err)
		return nil, status.Errorf(codes.InvalidArgument, err.Error())
	}

	if !kv.IsMetaServerLeader() {
		leader := kv.GetMetaServerLeader()
		mscli, _ := GetMetaServerConns().GetClient(leader)
		return mscli.ResizeStorage(ctx, req)
	}

	resp := &ResizeStorageResponse{}

	st, err := metadata.GetStorageManager().GetStorage(metadata.StorageID(req.Id))
	if err != nil {
		return nil, status.Errorf(codes.InvalidArgument, "")
	}

	if uint32(st.LastShardISN) >= uint32(req.NewShardCount) {
		return nil, status.Errorf(codes.InvalidArgument, "")
	}

	expandCount := req.NewShardCount - uint64(st.LastShardISN)
	err = scheduler.GetShardAllocator().ExpandStorage(st.ID, uint32(expandCount))
	if err != nil {
		return nil, status.Errorf(codes.Internal, "")
	}

	return resp, nil
}

func (m *MetaService) GetStorages(ctx context.Context, req *GetStoragesRequest) (*GetStoragesResponse, error) {
	err := GetStoragesParamCheck(req)
	if err != nil {
		log.Warn("GetStorages: invalid arguments, err: %v", err)
		return nil, status.Errorf(codes.InvalidArgument, err.Error())
	}

	if !kv.IsMetaServerLeader() {
		leader := kv.GetMetaServerLeader()
		mscli, _ := GetMetaServerConns().GetClient(leader)
		return mscli.GetStorages(ctx, req)
	}

	resp := &GetStoragesResponse{}

	for _, id := range req.Ids {
		st, err := metadata.GetStorageManager().GetStorage(metadata.StorageID(id))
		if err != nil {
			return nil, status.Errorf(codes.Internal, "")
		}

		resp.Storages = append(resp.Storages, &Storage{
			Id:        uint32(st.ID),
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

func (m *MetaService) AddDataServer(ctx context.Context, req *AddDataServerRequest) (*AddDataServerResponse, error) {
	log.Info("serve AddDataServer: req %#v", req)
	err := AddDataServerParamCheck(req)
	if err != nil {
		log.Warn("AddDataServer: invalid arguments, err: %v", err)
		return nil, status.Errorf(codes.InvalidArgument, err.Error())
	}

	if !kv.IsMetaServerLeader() {
		leader := kv.GetMetaServerLeader()
		mscli, _ := GetMetaServerConns().GetClient(leader)
		return mscli.AddDataServer(ctx, req)
	}

	resp := &AddDataServerResponse{}

	ip, port, err := net.SplitHostPort(req.Addr)
	if err != nil {
		return nil, status.Errorf(codes.InvalidArgument, "")
	}
	log.Info("start add dataserver %v to cluster", req.Addr)

	dm := metadata.GetDataServerManager()
	err = dm.AddDataServer(ip, port)
	if err != nil {
		return nil, status.Errorf(codes.Internal, "%v", err)
	}
	log.Info("add dataserver %v to cluster successfully", req.Addr)

	return resp, nil
}

func (m *MetaService) RemoveDataServer(ctx context.Context, req *RemoveDataServerRequest) (*RemoveDataServerResponse, error) {
	err := RemoveDataServerParamCheck(req)
	if err != nil {
		log.Warn("RemoveDataServer: invalid arguments, err: %v", err)
		return nil, status.Errorf(codes.InvalidArgument, err.Error())
	}

	if !kv.IsMetaServerLeader() {
		leader := kv.GetMetaServerLeader()
		mscli, _ := GetMetaServerConns().GetClient(leader)
		return mscli.RemoveDataServer(ctx, req)
	}

	resp := &RemoveDataServerResponse{}
	_, _, err = net.SplitHostPort(req.Addr)
	if err != nil {
		return nil, status.Errorf(codes.InvalidArgument, "")
	}

	dm := metadata.GetDataServerManager()
	if !dm.IsDataServerExists(req.Addr) {
		return nil, status.Errorf(codes.NotFound, "")
	}

	go func() {
		err = scheduler.ClearDataserver(req.Addr)
		if err != nil {
			return
		}
		err = dm.RemoveDataServer(req.Addr)
		if err != nil {
			return
		}
	}()

	return resp, nil
}

func (m *MetaService) ListDataServer(ctx context.Context, req *ListDataServerRequest) (*ListDataServerResponse, error) {
	err := ListDataServerParamCheck(req)
	if err != nil {
		log.Warn("ListDataServer: invalid arguments, err: %v", err)
		return nil, status.Errorf(codes.InvalidArgument, err.Error())
	}

	if !kv.IsMetaServerLeader() {
		leader := kv.GetMetaServerLeader()
		mscli, _ := GetMetaServerConns().GetClient(leader)
		return mscli.ListDataServer(ctx, req)
	}

	resp := &ListDataServerResponse{}

	dm := metadata.GetDataServerManager()
	dss := dm.DataServersClone()
	log.Info("copied dataservers: %#v", dss)

	for _, ds := range dss {
		resp.DataServers = append(resp.DataServers,
			&DataServer{
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

func (m *MetaService) UpdateDataServer(ctx context.Context, req *UpdateDataServerRequest) (*UpdateDataServerResponse, error) {
	err := UpdateDataServerParamCheck(req)
	if err != nil {
		log.Warn("UpdateDataServer: invalid arguments, err: %v", err)
		return nil, status.Errorf(codes.InvalidArgument, err.Error())
	}

	if !kv.IsMetaServerLeader() {
		leader := kv.GetMetaServerLeader()
		mscli, _ := GetMetaServerConns().GetClient(leader)
		return mscli.UpdateDataServer(ctx, req)
	}

	resp := &UpdateDataServerResponse{}

	return resp, nil
}

func (m *MetaService) ShardInfo(ctx context.Context, req *ShardInfoRequest) (*ShardInfoResponse, error) {
	if err := ShardInfoParamCheck(req); err != nil {
		log.Warn("ShardInfo: invalid arguments, err: %v", err)
		return nil, status.Errorf(codes.InvalidArgument, err.Error())
	}

	if !kv.IsMetaServerLeader() {
		leader := kv.GetMetaServerLeader()
		mscli, _ := GetMetaServerConns().GetClient(leader)
		return mscli.ShardInfo(ctx, req)
	}

	resp := &ShardInfoResponse{}

	sm := metadata.GetShardManager()
	if sm == nil {
		return resp, status.Errorf(codes.Internal, "")
	}

	shard, err := sm.GetShardCopy(metadata.ShardID(req.GetId()))
	if err != nil {
		return resp, status.Errorf(codes.Internal, "")
	}

	resp = &ShardInfoResponse{
		Shard: &Shard{
			Isn:             uint32(shard.ISN),
			StorageId:       uint32(shard.SID),
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

func (m *MetaService) CreateShard(ctx context.Context, req *CreateShardRequest) (*CreateShardResponse, error) {
	if err := CreateShardParamCheck(req); err != nil {
		log.Warn("CreateShard: invalid arguments, err: %v", err)
		return nil, status.Errorf(codes.InvalidArgument, err.Error())
	}

	if !kv.IsMetaServerLeader() {
		leader := kv.GetMetaServerLeader()
		mscli, _ := GetMetaServerConns().GetClient(leader)
		return mscli.CreateShard(ctx, req)
	}

	resp := &CreateShardResponse{}

	stm := metadata.GetStorageManager()
	st, err := stm.GetStorage(metadata.StorageID(req.StorageId))
	if err != nil || st == nil {
		log.Warn("no such storage, storage id: %v", req.StorageId)
		return nil, status.Errorf(codes.FailedPrecondition, "no such storage, id=%v", req.StorageId)
	}

	shardID := metadata.GenerateShardID(metadata.StorageID(req.StorageId), metadata.ShardISN(req.ShardIsn))
	shm := metadata.GetShardManager()
	_, err = shm.GetShard(shardID)
	if err == nil {
		log.Warn("shard already exists, shard id: %v", shardID)
		return nil, status.Errorf(codes.AlreadyExists, "shard already exists, id=%v", shardID)
	}
	if err != metadata.ErrNoSuchShard {
		log.Warn("failed to get shard, shard id: %v, err: %v", shardID, err)
		return nil, status.Errorf(codes.Internal, "failed to get shard, err: %v", err)
	}

	shard, err := shm.CreateNewShardByIDs(metadata.StorageID(req.StorageId), metadata.ShardISN(req.ShardIsn))
	if err != nil {
		log.Warn("failed to create new shard by id, err: %v", err)
		return nil, status.Errorf(codes.Internal, "failed to create shard, err: %v", err)
	}

	sa := scheduler.GetShardAllocator()
	// replicates, err := sa.AllocateShardReplicates(shard.ID, 3)
	_, err = sa.AllocateShardReplicates(shard.ID(), 3)
	if err != nil {
		log.Warn("failed to allocate shard replicates, err: %v", err)
		return nil, status.Errorf(codes.Internal, "failed to allocate replicates, err: %v", err)
	}

	return resp, nil
}

func (m *MetaService) RemoveShard(ctx context.Context, req *RemoveShardRequest) (*RemoveShardResponse, error) {
	if err := RemoveShardParamCheck(req); err != nil {
		log.Warn("CreateShard: invalid arguments, err: %v", err)
		return nil, status.Errorf(codes.InvalidArgument, err.Error())
	}

	if !kv.IsMetaServerLeader() {
		leader := kv.GetMetaServerLeader()
		mscli, _ := GetMetaServerConns().GetClient(leader)
		return mscli.RemoveShard(ctx, req)
	}

	resp := &RemoveShardResponse{}

	shardID := metadata.GenerateShardID(metadata.StorageID(req.StorageId), metadata.ShardISN(req.ShardIsn))
	shm := metadata.GetShardManager()
	err := shm.MarkDelete(shardID)
	if err != nil {
		log.Warn("failed to mark delete for shard, err: %v", err)
		return nil, status.Errorf(codes.Internal, "failed to mark delete for shard, err: %v", err)
	}

	return resp, nil
}

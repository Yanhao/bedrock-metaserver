package service

import (
	"context"
	"net"
	"time"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/types/known/emptypb"
	"google.golang.org/protobuf/types/known/timestamppb"

	"sr.ht/moyanhao/bedrock-metaserver/kvengine"
	"sr.ht/moyanhao/bedrock-metaserver/manager"
	"sr.ht/moyanhao/bedrock-metaserver/model"
	"sr.ht/moyanhao/bedrock-metaserver/scheduler"
	"sr.ht/moyanhao/bedrock-metaserver/tso"
	"sr.ht/moyanhao/bedrock-metaserver/utils/log"
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

	if !kvengine.GetLeaderShip().IsMetaServerLeader() {
		leader := kvengine.GetLeaderShip().GetMetaServerLeader()
		mscli, _ := GetMetaServerConns().GetClient(leader)
		return mscli.HeartBeat(ctx, req)
	}

	dm := manager.GetDataServerManager()
	if err := dm.MarkActive(req.GetAddr(), true); err != nil {
		return nil, status.Errorf(codes.Internal, "inernal error: %v", err)
	}

	log.Info("receive heartbeat from %s", req.Addr)

	return &emptypb.Empty{}, nil
}

func getUpdatedRoute(shardID model.ShardID, ts time.Time) (*RouteRecord, error) {
	sm := manager.GetShardManager()
	shard, err := sm.GetShard(shardID)
	if err != nil {
		log.Error("get shard failed, shardID: %d, err: %v", shardID, err)
		return nil, status.Errorf(codes.Internal, "get shard failed")
	}

	if shard.ReplicaUpdateTs.After(ts) {
		route := &RouteRecord{
			ShardId:    uint64(shardID),
			LeaderAddr: shard.Leader,
		}
		for rep := range shard.Replicates {
			route.Addrs = append(route.Addrs, rep)
		}

		log.Info("route: %v", route)

		return route, nil
	}

	return nil, nil
}

func (m *MetaService) GetShardRoutes(ctx context.Context, req *GetShardRoutesRequest) (*GetShardRoutesResponse, error) {
	log.Info("req: %v", req.GetShardRange())
	err := GetShardRoutesParamCheck(req)
	if err != nil {
		log.Warn("GetShardRoutes: invalid arguments, err: %v", err)
		return nil, status.Errorf(codes.InvalidArgument, err.Error())
	}

	if !kvengine.GetLeaderShip().IsMetaServerLeader() {
		leader := kvengine.GetLeaderShip().GetMetaServerLeader()
		mscli, _ := GetMetaServerConns().GetClient(leader)
		return mscli.GetShardRoutes(ctx, req)
	}

	resp := &GetShardRoutesResponse{}

	ts := req.GetTimestamp().AsTime()
	if req.GetShardRange() != nil {
		begin := req.GetShardRange().StartShardId
		end := begin + req.GetShardRange().Offset

		log.Info("req: shard_id: 0x%016x, end: %v", begin, end)

		for shardID := begin; shardID <= end; shardID++ {
			route, err := getUpdatedRoute(model.ShardID(shardID), ts)
			if err != nil {
				return nil, err
			}

			resp.Routes = append(resp.Routes, route)
		}
	} else if req.GetShardsList() != nil {
		for _, shardID := range req.GetShardsList().GetShardIds() {
			route, err := getUpdatedRoute(model.ShardID(shardID), ts)
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

	if !kvengine.GetLeaderShip().IsMetaServerLeader() {
		leader := kvengine.GetLeaderShip().GetMetaServerLeader()
		mscli, _ := GetMetaServerConns().GetClient(leader)
		return mscli.CreateStorage(ctx, req)
	}

	resp := &CreateStorageResponse{}

	sm := manager.GetStorageManager()
	st, err := sm.GetStorageByName(req.Name)
	if err != nil {
		log.Error("check storage by name failed, err: %v", err)
		return resp, status.Errorf(codes.Internal, "check storage by name failed")
	}

	if st != nil {
		log.Error("shard name %v already exists", req.Name)
		return resp, status.Errorf(codes.AlreadyExists, "shard name %v already exists", req.Name)
	}

	storage, err := scheduler.GetShardAllocator().AllocatorNewStorage(req.Name, req.InitialRangeCount)
	if err != nil {
		log.Error("create storage failed, err: %v", err)
		return resp, status.Errorf(codes.Internal, "create storage failed")
	}

	resp.Id = uint64(storage.ID)

	log.Info("successfully create storage, id: %v", resp.Id)

	return resp, nil
}

func (m *MetaService) DeleteStorage(ctx context.Context, req *DeleteStorageRequest) (*DeleteStorageResponse, error) {
	err := DeleteStorageParamCheck(req)
	if err != nil {
		log.Warn("DeleteStorage: invalid arguments, err: %v", err)
		return nil, status.Errorf(codes.InvalidArgument, err.Error())
	}

	if !kvengine.GetLeaderShip().IsMetaServerLeader() {
		leader := kvengine.GetLeaderShip().GetMetaServerLeader()
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

	err = manager.GetStorageManager().StorageDelete(model.StorageID(req.Id), recycleTime)
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

	if !kvengine.GetLeaderShip().IsMetaServerLeader() {
		leader := kvengine.GetLeaderShip().GetMetaServerLeader()
		mscli, _ := GetMetaServerConns().GetClient(leader)
		return mscli.UndeleteStorage(ctx, req)
	}

	resp := &UndeleteStorageResponse{}

	err = manager.GetStorageManager().StorageUndelete(model.StorageID(req.Id))
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

	if !kvengine.GetLeaderShip().IsMetaServerLeader() {
		leader := kvengine.GetLeaderShip().GetMetaServerLeader()
		mscli, _ := GetMetaServerConns().GetClient(leader)
		return mscli.RenameStorage(ctx, req)
	}

	resp := &RenameStorageResponse{}

	err = manager.GetStorageManager().StorageRename(model.StorageID(req.Id), req.NewName)
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

	if !kvengine.GetLeaderShip().IsMetaServerLeader() {
		leader := kvengine.GetLeaderShip().GetMetaServerLeader()
		mscli, _ := GetMetaServerConns().GetClient(leader)
		return mscli.ResizeStorage(ctx, req)
	}

	resp := &ResizeStorageResponse{}

	st, err := manager.GetStorageManager().GetStorage(model.StorageID(req.Id))
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
	log.Info("GetStorages: req: %v", req)
	err := GetStoragesParamCheck(req)
	if err != nil {
		log.Warn("GetStorages: invalid arguments, err: %v", err)
		return nil, status.Errorf(codes.InvalidArgument, err.Error())
	}

	if !kvengine.GetLeaderShip().IsMetaServerLeader() {
		leader := kvengine.GetLeaderShip().GetMetaServerLeader()
		mscli, _ := GetMetaServerConns().GetClient(leader)
		return mscli.GetStorages(ctx, req)
	}

	resp := &GetStoragesResponse{}

	for _, id := range req.Ids {
		st, err := manager.GetStorageManager().GetStorage(model.StorageID(id))
		if err != nil {
			return nil, status.Errorf(codes.Internal, "%v", err)
		}

		log.Info("storage: %+v", st)

		resp.Storages = append(resp.Storages, &Storage{
			Id:           uint32(st.ID),
			Name:         st.Name,
			CreateTs:     timestamppb.New(st.CreateTs),
			DeletedTs:    timestamppb.New(st.DeleteTs),
			Owner:        st.Owner,
			LastShardIsn: uint32(st.LastShardISN),
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

	if !kvengine.GetLeaderShip().IsMetaServerLeader() {
		leader := kvengine.GetLeaderShip().GetMetaServerLeader()
		mscli, _ := GetMetaServerConns().GetClient(leader)
		return mscli.AddDataServer(ctx, req)
	}

	resp := &AddDataServerResponse{}

	ip, port, err := net.SplitHostPort(req.Addr)
	if err != nil {
		return nil, status.Errorf(codes.InvalidArgument, "")
	}
	log.Info("start add dataserver %v to cluster", req.Addr)

	dm := manager.GetDataServerManager()
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

	if !kvengine.GetLeaderShip().IsMetaServerLeader() {
		leader := kvengine.GetLeaderShip().GetMetaServerLeader()
		mscli, _ := GetMetaServerConns().GetClient(leader)
		return mscli.RemoveDataServer(ctx, req)
	}

	resp := &RemoveDataServerResponse{}
	_, _, err = net.SplitHostPort(req.Addr)
	if err != nil {
		return nil, status.Errorf(codes.InvalidArgument, "")
	}

	dm := manager.GetDataServerManager()
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

	if !kvengine.GetLeaderShip().IsMetaServerLeader() {
		leader := kvengine.GetLeaderShip().GetMetaServerLeader()
		mscli, _ := GetMetaServerConns().GetClient(leader)
		return mscli.ListDataServer(ctx, req)
	}

	resp := &ListDataServerResponse{}

	dm := manager.GetDataServerManager()
	dss := dm.DataServersCopy()
	log.Info("copied dataservers: %#v", dss)

	for _, ds := range dss {
		resp.DataServers = append(resp.DataServers,
			&DataServer{
				Ip:              ds.Ip,
				Port:            ds.Port,
				Capacity:        ds.Capacity,
				Free:            ds.Free,
				LastHeartbeatTs: timestamppb.New(ds.LastHeartBeatTs),
				Status: func(s model.LiveStatus) string {
					switch s {
					case model.LiveStatusActive:
						return "active"
					case model.LiveStatusOffline:
						return "offline"
					case model.LiveStatusInactive:
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

	if !kvengine.GetLeaderShip().IsMetaServerLeader() {
		leader := kvengine.GetLeaderShip().GetMetaServerLeader()
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

	if !kvengine.GetLeaderShip().IsMetaServerLeader() {
		leader := kvengine.GetLeaderShip().GetMetaServerLeader()
		mscli, _ := GetMetaServerConns().GetClient(leader)
		return mscli.ShardInfo(ctx, req)
	}

	resp := &ShardInfoResponse{}

	sm := manager.GetShardManager()
	if sm == nil {
		return resp, status.Errorf(codes.Internal, "")
	}

	shard, err := sm.GetShard(model.ShardID(req.GetId()))
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

	if !kvengine.GetLeaderShip().IsMetaServerLeader() {
		leader := kvengine.GetLeaderShip().GetMetaServerLeader()
		mscli, _ := GetMetaServerConns().GetClient(leader)
		return mscli.CreateShard(ctx, req)
	}

	resp := &CreateShardResponse{}

	stm := manager.GetStorageManager()
	st, err := stm.GetStorage(model.StorageID(req.StorageId))
	if err != nil || st == nil {
		log.Warn("no such storage, storage id: %v", req.StorageId)
		return nil, status.Errorf(codes.FailedPrecondition, "no such storage, id=%v", req.StorageId)
	}

	shardID := model.GenerateShardID(model.StorageID(req.StorageId), model.ShardISN(req.ShardIsn))
	shm := manager.GetShardManager()
	_, err = shm.GetShard(shardID)
	if err == nil {
		log.Warn("shard already exists, shard id: %v", shardID)
		return nil, status.Errorf(codes.AlreadyExists, "shard already exists, id=%v", shardID)
	}

	shard, err := shm.CreateNewShardByIDs(model.StorageID(req.StorageId), model.ShardISN(req.ShardIsn))
	if err != nil {
		log.Warn("failed to create new shard by id, err: %v", err)
		return nil, status.Errorf(codes.Internal, "failed to create shard, err: %v", err)
	}

	sa := scheduler.GetShardAllocator()
	// replicates, err := sa.AllocateShardReplicates(shard.ID, 3)
	_, err = sa.AllocateShardReplicates(shard.ID(), scheduler.DefaultReplicatesCount)
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

	if !kvengine.GetLeaderShip().IsMetaServerLeader() {
		leader := kvengine.GetLeaderShip().GetMetaServerLeader()
		mscli, _ := GetMetaServerConns().GetClient(leader)
		return mscli.RemoveShard(ctx, req)
	}

	resp := &RemoveShardResponse{}

	shardID := model.GenerateShardID(model.StorageID(req.StorageId), model.ShardISN(req.ShardIsn))
	shm := manager.GetShardManager()
	err := shm.MarkDelete(shardID)
	if err != nil {
		log.Warn("failed to mark delete for shard, err: %v", err)
		return nil, status.Errorf(codes.Internal, "failed to mark delete for shard, err: %v", err)
	}

	return resp, nil
}

func (m *MetaService) GetShardIDByKey(ctx context.Context, req *GetShardIDByKeyRequest) (*GetShardIDByKeyResponse, error) {
	if err := GetShardIDByKeyParamCheck(req); err != nil {
		log.Warn("GetShardIDByKey: invalid arguments, err: %v", err)
		return nil, status.Errorf(codes.InvalidArgument, err.Error())
	}

	if !kvengine.GetLeaderShip().IsMetaServerLeader() {
		leader := kvengine.GetLeaderShip().GetMetaServerLeader()
		mscli, _ := GetMetaServerConns().GetClient(leader)
		return mscli.GetShardIDByKey(ctx, req)
	}

	resp := &GetShardIDByKeyResponse{}

	sm := manager.GetShardManager()
	shardID, err := sm.GetShardIDByKey(model.StorageID(req.StorageId), req.Key)
	if err != nil {
		return nil, status.Errorf(codes.Internal, "failed to get storage, err: %v", err)
	}

	resp.ShardId = uint64(shardID)

	return resp, nil
}

func (m *MetaService) SyncShardInDataServer(reqStream MetaService_SyncShardInDataServerServer) error {
	for {
		req, err := reqStream.Recv()
		if err != nil {
			return status.Error(codes.Internal, err.Error())
		}

		if req == nil {
			break
		}

		syncTs := req.GetSyncTs().GetSeconds()
		shards := req.GetShards()
		for _, shard := range shards {
			err := manager.GetShardManager().UpdateShardInDataServer(req.GetDataserverAddr(), model.ShardID(shard.GetShardId()), syncTs)
			if err != nil {
				return err
			}
		}

		if req.GetIsLastPiece() {
			dsm := manager.GetDataServerManager()
			err = dsm.UpdateSyncTs(req.GetDataserverAddr(), syncTs)
			if err != nil {
				return err
			}
		}
	}

	resp := SyncShardInDataServerResponse{}
	reqStream.SendAndClose(&resp)

	return nil
}

func (m *MetaService) AllocateTxIDs(ctx context.Context, req *AllocateTxIDsRequest) (*AllocateTxIDsResponse, error) {
	if err := AllocateTxIDsParamCheck(req); err != nil {
		log.Warn("AllocateTxID: invalid arguments, err: %v", err)
		return nil, status.Errorf(codes.InvalidArgument, err.Error())
	}

	if !kvengine.GetLeaderShip().IsMetaServerLeader() {
		leader := kvengine.GetLeaderShip().GetMetaServerLeader()
		mscli, _ := GetMetaServerConns().GetClient(leader)
		return mscli.AllocateTxIDs(ctx, req)
	}

	resp := &AllocateTxIDsResponse{}

	txIDs, err := tso.GetTxIDAllocator().Allocate(req.Count)
	if err != nil {
		return nil, status.Errorf(codes.Internal, err.Error())
	}

	resp.TxIds = txIDs

	return resp, nil
}

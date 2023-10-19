package main

import (
	"bytes"
	"context"
	"io"
	"net"
	"time"

	log "github.com/sirupsen/logrus"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/types/known/emptypb"
	"google.golang.org/protobuf/types/known/timestamppb"

	"sr.ht/moyanhao/bedrock-metaserver/clients/metaserver"
	"sr.ht/moyanhao/bedrock-metaserver/manager"
	"sr.ht/moyanhao/bedrock-metaserver/model"
	"sr.ht/moyanhao/bedrock-metaserver/role"
	"sr.ht/moyanhao/bedrock-metaserver/scheduler"
	"sr.ht/moyanhao/bedrock-metaserver/tso"
)

type MetaService struct {
	metaserver.UnimplementedMetaServiceServer
}

func (m *MetaService) HeartBeat(ctx context.Context, req *metaserver.HeartBeatRequest) (*emptypb.Empty, error) {
	err := HeartBeatParamCheck(req)
	if err != nil {
		log.Warnf("HeartBeat: invalid arguments, err: %v", err)
		return nil, status.Errorf(codes.InvalidArgument, err.Error())
	}

	if !role.GetLeaderShip().IsMetaServerLeader() {
		leader := role.GetLeaderShip().GetMetaServerLeader()
		mscli, _ := metaserver.GetMetaServerConns().GetClient(leader)
		return mscli.HeartBeat(ctx, req)
	}

	dm := manager.GetDataServerManager()
	if err := dm.MarkActive(req.GetAddr(), true); err != nil {
		return nil, status.Errorf(codes.Internal, "inernal error: %v", err)
	}

	log.Infof("receive heartbeat from %s", req.Addr)

	return &emptypb.Empty{}, nil
}

func getUpdatedRoute(shardID model.ShardID, ts time.Time) (*metaserver.ShardRoute, error) {
	log.Infof("getUpdateRoute, shardID: %v, ts: %v", shardID, ts)
	sm := manager.GetShardManager()
	shard, err := sm.GetShard(shardID)
	if err != nil {
		log.Errorf("get shard failed, shardID: %d, err: %v", shardID, err)
		return nil, status.Errorf(codes.Internal, "get shard failed")
	}
	log.Infof("shard: %#v", shard)

	if shard.ReplicaUpdateTs.After(ts) {
		route := &metaserver.ShardRoute{
			ShardId:    uint64(shardID),
			LeaderAddr: shard.Leader,
		}
		for rep := range shard.Replicates {
			route.Addrs = append(route.Addrs, rep)
		}

		log.Infof("route: %v", route)

		return route, nil
	}

	log.Infof("getUpdateRoute returns nil, nil")
	return nil, nil
}

func (m *MetaService) GetShardRoute(ctx context.Context, req *metaserver.GetShardRouteRequest) (*metaserver.GetShardRouteResponse, error) {
	log.Infof("GetShardRoutes request: %v", req)

	err := GetShardRoutesParamCheck(req)
	if err != nil {
		log.Warnf("GetShardRoutes: invalid arguments, err: %v", err)
		return nil, status.Errorf(codes.InvalidArgument, err.Error())
	}

	if !role.GetLeaderShip().IsMetaServerLeader() {
		leader := role.GetLeaderShip().GetMetaServerLeader()
		mscli, _ := metaserver.GetMetaServerConns().GetClient(leader)
		return mscli.GetShardRoute(ctx, req)
	}

	resp := &metaserver.GetShardRouteResponse{}

	ts := req.GetTimestamp().AsTime()
	for _, shardID := range req.GetShardIds() {
		route, err := getUpdatedRoute(model.ShardID(shardID), ts)
		if err != nil {
			return nil, err
		}

		resp.Routes = append(resp.Routes, route)
	}

	return resp, nil
}

func (m *MetaService) ScanShardRange(ctx context.Context, req *metaserver.ScanShardRangeRequest) (*metaserver.ScanShardRangeResponse, error) {
	log.Infof("ScanShardRange request: %v", req)

	err := ScanStorageShardsParamCheck(req)
	if err != nil {
		log.Warnf("ScanShardRange: invalid arguments, err: %v", err)
		return nil, status.Errorf(codes.InvalidArgument, err.Error())
	}

	if !role.GetLeaderShip().IsMetaServerLeader() {
		leader := role.GetLeaderShip().GetMetaServerLeader()
		mscli, _ := metaserver.GetMetaServerConns().GetClient(leader)
		return mscli.ScanShardRange(ctx, req)
	}

	resp := &metaserver.ScanShardRangeResponse{}

	shardsAndRange, err := manager.GetStorageManager().ScanShardRange(model.StorageID(req.StorageId), req.RangeStart)
	if err != nil {
		return resp, status.Errorf(codes.Internal, "get storage shards failed, err: %v", err)
	}


	for _, s := range shardsAndRange {
		resp.Ranges = append(resp.Ranges, &metaserver.ScanShardRangeResponse_ShardRange{
			ShardId:    uint64(s.ShardID),
			RangeStart: s.RangeStart,
			RangeEnd:   s.RangeEnd,
		})
	}
	log.Infof("resp: %v", resp)

	if  len(resp.Ranges) == 0 || bytes.Equal(resp.Ranges[len(resp.Ranges)-1].RangeEnd, scheduler.MaxKey) {
		resp.IsEnd = true
	}

	return resp, nil
}

func (m *MetaService) Info(ctx context.Context, req *metaserver.InfoRequest) (*metaserver.InfoResponse, error) {
	log.Infof("Info request")

	err := InfoParamCheck(req)
	if err != nil {
		log.Warnf("Info: invalid arguments, err: %v", err)
		return nil, status.Errorf(codes.InvalidArgument, err.Error())
	}

	resp := &metaserver.InfoResponse{}

	resp.LeaderAddr = role.GetLeaderShip().GetMetaServerLeader()

	return resp, nil
}

func (m *MetaService) CreateStorage(ctx context.Context, req *metaserver.CreateStorageRequest) (*metaserver.CreateStorageResponse, error) {
	err := CreateStorageParamCheck(req)
	if err != nil {
		log.Warnf("CreateStorage: invalid arguments, err: %v", err)
		return nil, status.Errorf(codes.InvalidArgument, err.Error())
	}

	if !role.GetLeaderShip().IsMetaServerLeader() {
		leader := role.GetLeaderShip().GetMetaServerLeader()
		mscli, _ := metaserver.GetMetaServerConns().GetClient(leader)
		return mscli.CreateStorage(ctx, req)
	}

	resp := &metaserver.CreateStorageResponse{}

	st, err := manager.GetStorageManager().GetStorageByName(req.Name)
	if err != nil {
		log.Errorf("check storage by name failed, err: %v", err)
		return resp, status.Errorf(codes.Internal, "check storage by name failed")
	}

	if st != nil {
		log.Errorf("shard name %v already exists", req.Name)
		return resp, status.Errorf(codes.AlreadyExists, "shard name %v already exists", req.Name)
	}

	storage, err := scheduler.GetShardAllocator().AllocateNewStorage(req.Name, req.InitialRangeCount)
	if err != nil {
		log.Errorf("create storage failed, err: %v", err)
		return resp, status.Errorf(codes.Internal, "create storage failed")
	}

	resp.Id = uint64(storage.ID)

	log.Infof("successfully create storage, id: %v", resp.Id)

	return resp, nil
}

func (m *MetaService) DeleteStorage(ctx context.Context, req *metaserver.DeleteStorageRequest) (*metaserver.DeleteStorageResponse, error) {
	err := DeleteStorageParamCheck(req)
	if err != nil {
		log.Warnf("DeleteStorage: invalid arguments, err: %v", err)
		return nil, status.Errorf(codes.InvalidArgument, err.Error())
	}

	if !role.GetLeaderShip().IsMetaServerLeader() {
		leader := role.GetLeaderShip().GetMetaServerLeader()
		mscli, _ := metaserver.GetMetaServerConns().GetClient(leader)
		return mscli.DeleteStorage(ctx, req)
	}

	resp := &metaserver.DeleteStorageResponse{}

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
		log.Errorf("delete storage failed, err: %v", err)
		return resp, status.Errorf(codes.Internal, "delete storage failed")
	}

	return resp, nil
}

func (m *MetaService) UndeleteStorage(ctx context.Context, req *metaserver.UndeleteStorageRequest) (*metaserver.UndeleteStorageResponse, error) {
	err := UndeleteStorageParamCheck(req)
	if err != nil {
		log.Warnf("UndeleteStorage: invalid arguments, err: %v", err)
		return nil, status.Errorf(codes.InvalidArgument, err.Error())
	}

	if !role.GetLeaderShip().IsMetaServerLeader() {
		leader := role.GetLeaderShip().GetMetaServerLeader()
		mscli, _ := metaserver.GetMetaServerConns().GetClient(leader)
		return mscli.UndeleteStorage(ctx, req)
	}

	resp := &metaserver.UndeleteStorageResponse{}

	err = manager.GetStorageManager().StorageUndelete(model.StorageID(req.Id))
	if err != nil {
		log.Errorf("undelete storage failed, err: %v", err)
		return resp, status.Errorf(codes.Internal, "undelete storage failed")
	}

	return resp, nil
}

func (m *MetaService) RenameStorage(ctx context.Context, req *metaserver.RenameStorageRequest) (*metaserver.RenameStorageResponse, error) {
	err := RenameStorageParamCheck(req)
	if err != nil {
		log.Warnf("RenameStorage: invalid arguments, err: %v", err)
		return nil, status.Errorf(codes.InvalidArgument, err.Error())
	}

	if !role.GetLeaderShip().IsMetaServerLeader() {
		leader := role.GetLeaderShip().GetMetaServerLeader()
		mscli, _ := metaserver.GetMetaServerConns().GetClient(leader)
		return mscli.RenameStorage(ctx, req)
	}

	resp := &metaserver.RenameStorageResponse{}

	err = manager.GetStorageManager().StorageRename(model.StorageID(req.Id), req.NewName)
	if err != nil {
		log.Errorf("create storage failed, err: %v", err)
		return resp, status.Errorf(codes.Internal, "create storage failed")
	}

	return resp, nil
}

func (m *MetaService) ResizeStorage(ctx context.Context, req *metaserver.ResizeStorageRequest) (*metaserver.ResizeStorageResponse, error) {
	err := ResizeStorageParamCheck(req)
	if err != nil {
		log.Warnf("ResizeStorage: invalid arguments, err: %v", err)
		return nil, status.Errorf(codes.InvalidArgument, err.Error())
	}

	if !role.GetLeaderShip().IsMetaServerLeader() {
		leader := role.GetLeaderShip().GetMetaServerLeader()
		mscli, _ := metaserver.GetMetaServerConns().GetClient(leader)
		return mscli.ResizeStorage(ctx, req)
	}

	resp := &metaserver.ResizeStorageResponse{}

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

func (m *MetaService) StorageInfo(ctx context.Context, req *metaserver.StorageInfoRequest) (*metaserver.StorageInfoResponse, error) {
	log.Infof("GetStorages: req: %v", req)
	err := GetStoragesParamCheck(req)
	if err != nil {
		log.Warnf("GetStorages: invalid arguments, err: %v", err)
		return nil, status.Errorf(codes.InvalidArgument, err.Error())
	}

	if !role.GetLeaderShip().IsMetaServerLeader() {
		leader := role.GetLeaderShip().GetMetaServerLeader()
		mscli, _ := metaserver.GetMetaServerConns().GetClient(leader)
		return mscli.StorageInfo(ctx, req)
	}

	resp := &metaserver.StorageInfoResponse{}

	for _, id := range req.Ids {
		st, err := manager.GetStorageManager().GetStorage(model.StorageID(id))
		if err != nil {
			return nil, status.Errorf(codes.Internal, "%v", err)
		}

		log.Infof("storage: %+v", st)

		resp.Storages = append(resp.Storages, &metaserver.Storage{
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

func (m *MetaService) AddDataServer(ctx context.Context, req *metaserver.AddDataServerRequest) (*metaserver.AddDataServerResponse, error) {
	log.Infof("serve AddDataServer: req %#v", req)
	err := AddDataServerParamCheck(req)
	if err != nil {
		log.Warnf("AddDataServer: invalid arguments, err: %v", err)
		return nil, status.Errorf(codes.InvalidArgument, err.Error())
	}

	if !role.GetLeaderShip().IsMetaServerLeader() {
		leader := role.GetLeaderShip().GetMetaServerLeader()
		mscli, _ := metaserver.GetMetaServerConns().GetClient(leader)
		return mscli.AddDataServer(ctx, req)
	}

	resp := &metaserver.AddDataServerResponse{}

	ip, port, err := net.SplitHostPort(req.Addr)
	if err != nil {
		return nil, status.Errorf(codes.InvalidArgument, "")
	}
	log.Infof("start add dataserver %v to cluster", req.Addr)

	dm := manager.GetDataServerManager()
	err = dm.AddDataServer(ip, port)
	if err != nil {
		return nil, status.Errorf(codes.Internal, "%v", err)
	}
	log.Infof("add dataserver %v to cluster successfully", req.Addr)

	return resp, nil
}

func (m *MetaService) RemoveDataServer(ctx context.Context, req *metaserver.RemoveDataServerRequest) (*metaserver.RemoveDataServerResponse, error) {
	err := RemoveDataServerParamCheck(req)
	if err != nil {
		log.Warnf("RemoveDataServer: invalid arguments, err: %v", err)
		return nil, status.Errorf(codes.InvalidArgument, err.Error())
	}

	if !role.GetLeaderShip().IsMetaServerLeader() {
		leader := role.GetLeaderShip().GetMetaServerLeader()
		mscli, _ := metaserver.GetMetaServerConns().GetClient(leader)
		return mscli.RemoveDataServer(ctx, req)
	}

	resp := &metaserver.RemoveDataServerResponse{}
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

func (m *MetaService) ListDataServer(ctx context.Context, req *metaserver.ListDataServerRequest) (*metaserver.ListDataServerResponse, error) {
	err := ListDataServerParamCheck(req)
	if err != nil {
		log.Warnf("ListDataServer: invalid arguments, err: %v", err)
		return nil, status.Errorf(codes.InvalidArgument, err.Error())
	}

	if !role.GetLeaderShip().IsMetaServerLeader() {
		leader := role.GetLeaderShip().GetMetaServerLeader()
		mscli, _ := metaserver.GetMetaServerConns().GetClient(leader)
		return mscli.ListDataServer(ctx, req)
	}

	resp := &metaserver.ListDataServerResponse{}

	dm := manager.GetDataServerManager()
	dss := dm.GetDataServersCopy()
	log.Infof("copied dataservers: %#v", dss)

	for _, ds := range dss {
		resp.DataServers = append(resp.DataServers,
			&metaserver.DataServer{
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

func (m *MetaService) UpdateDataServer(ctx context.Context, req *metaserver.UpdateDataServerRequest) (*metaserver.UpdateDataServerResponse, error) {
	err := UpdateDataServerParamCheck(req)
	if err != nil {
		log.Warnf("UpdateDataServer: invalid arguments, err: %v", err)
		return nil, status.Errorf(codes.InvalidArgument, err.Error())
	}

	if !role.GetLeaderShip().IsMetaServerLeader() {
		leader := role.GetLeaderShip().GetMetaServerLeader()
		mscli, _ := metaserver.GetMetaServerConns().GetClient(leader)
		return mscli.UpdateDataServer(ctx, req)
	}

	resp := &metaserver.UpdateDataServerResponse{}

	return resp, nil
}

func (m *MetaService) ShardInfo(ctx context.Context, req *metaserver.ShardInfoRequest) (*metaserver.ShardInfoResponse, error) {
	if err := ShardInfoParamCheck(req); err != nil {
		log.Warnf("ShardInfo: invalid arguments, err: %v", err)
		return nil, status.Errorf(codes.InvalidArgument, err.Error())
	}

	if !role.GetLeaderShip().IsMetaServerLeader() {
		leader := role.GetLeaderShip().GetMetaServerLeader()
		mscli, _ := metaserver.GetMetaServerConns().GetClient(leader)
		return mscli.ShardInfo(ctx, req)
	}

	resp := &metaserver.ShardInfoResponse{}

	sm := manager.GetShardManager()
	if sm == nil {
		return resp, status.Errorf(codes.Internal, "")
	}

	shard, err := sm.GetShard(model.ShardID(req.GetId()))
	if err != nil {
		return resp, status.Errorf(codes.Internal, "")
	}

	resp = &metaserver.ShardInfoResponse{
		Shard: &metaserver.Shard{
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

func (m *MetaService) CreateShard(ctx context.Context, req *metaserver.CreateShardRequest) (*metaserver.CreateShardResponse, error) {
	if err := CreateShardParamCheck(req); err != nil {
		log.Warnf("CreateShard: invalid arguments, err: %v", err)
		return nil, status.Errorf(codes.InvalidArgument, err.Error())
	}

	if !role.GetLeaderShip().IsMetaServerLeader() {
		leader := role.GetLeaderShip().GetMetaServerLeader()
		mscli, _ := metaserver.GetMetaServerConns().GetClient(leader)
		return mscli.CreateShard(ctx, req)
	}

	resp := &metaserver.CreateShardResponse{}

	stm := manager.GetStorageManager()
	st, err := stm.GetStorage(model.StorageID(req.StorageId))
	if err != nil || st == nil {
		log.Warnf("no such storage, storage id: %v", req.StorageId)
		return nil, status.Errorf(codes.FailedPrecondition, "no such storage, id=%v", req.StorageId)
	}

	shardID := model.GenerateShardID(model.StorageID(req.StorageId), model.ShardISN(req.ShardIsn))
	shm := manager.GetShardManager()
	_, err = shm.GetShard(shardID)
	if err == nil {
		log.Warnf("shard already exists, shard id: %v", shardID)
		return nil, status.Errorf(codes.AlreadyExists, "shard already exists, id=%v", shardID)
	}

	shard, err := shm.CreateNewShardByIDs(model.StorageID(req.StorageId), model.ShardISN(req.ShardIsn))
	if err != nil {
		log.Warnf("failed to create new shard by id, err: %v", err)
		return nil, status.Errorf(codes.Internal, "failed to create shard, err: %v", err)
	}

	sa := scheduler.GetShardAllocator()
	// replicates, err := sa.AllocateShardReplicates(shard.ID, 3)
	_, err = sa.AllocateShardReplicates(shard.ID(), scheduler.DefaultReplicatesCount, req.RangeStart, req.RangeEnd)
	if err != nil {
		log.Warnf("failed to allocate shard replicates, err: %v", err)
		return nil, status.Errorf(codes.Internal, "failed to allocate replicates, err: %v", err)
	}

	return resp, nil
}

func (m *MetaService) RemoveShard(ctx context.Context, req *metaserver.RemoveShardRequest) (*metaserver.RemoveShardResponse, error) {
	if err := RemoveShardParamCheck(req); err != nil {
		log.Warnf("CreateShard: invalid arguments, err: %v", err)
		return nil, status.Errorf(codes.InvalidArgument, err.Error())
	}

	if !role.GetLeaderShip().IsMetaServerLeader() {
		leader := role.GetLeaderShip().GetMetaServerLeader()
		mscli, _ := metaserver.GetMetaServerConns().GetClient(leader)
		return mscli.RemoveShard(ctx, req)
	}

	resp := &metaserver.RemoveShardResponse{}

	shardID := model.GenerateShardID(model.StorageID(req.StorageId), model.ShardISN(req.ShardIsn))
	shm := manager.GetShardManager()
	err := shm.MarkDelete(shardID)
	if err != nil {
		log.Warnf("failed to mark delete for shard, err: %v", err)
		return nil, status.Errorf(codes.Internal, "failed to mark delete for shard, err: %v", err)
	}

	return resp, nil
}

func (m *MetaService) SyncShardInDataServer(reqStream metaserver.MetaService_SyncShardInDataServerServer) error {
	log.Info("sync shard in dataserver ...")

	if !role.GetLeaderShip().IsMetaServerLeader() {
		leader := role.GetLeaderShip().GetMetaServerLeader()
		mscli, _ := metaserver.GetMetaServerConns().GetClient(leader)

		targetStream, err := mscli.SyncShardInDataServer(context.Background())
		if err != nil {
			log.Errorf("Failed to create stream to target node: %v", err)
			return err
		}
		defer targetStream.CloseSend()
		log.Infof("create sync shard request stream to %v", leader)

		for {
			req, err := reqStream.Recv()
			if err == io.EOF {
				break
			}
			if err != nil {
				log.Errorf("sync shard in dataserver, stream receive failed, err: %v", err)
				return status.Error(codes.Internal, err.Error())
			}

			err = targetStream.Send(req)
			if err != nil {
				log.Errorf("Failed to send message to target node: %v", err)
				return err
			}
		}

		resp, err := targetStream.CloseAndRecv()
		if err != nil {
			log.Errorf("Failed to receive response from target node: %v", err)
			return err
		}

		return reqStream.SendAndClose(resp)
	}

	for {
		req, err := reqStream.Recv()
		if err == io.EOF {
			break
		}

		if err != nil {
			log.Errorf("sync shard in dataserver, stream receive failed, err: %v", err)
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
				log.Errorf("update shard in dataserver failed, err: %v", err)
				return status.Errorf(codes.Internal, "%v", err)
			}
		}

		if req.GetIsLastPiece() {
			dsm := manager.GetDataServerManager()
			dsAddr := req.GetDataserverAddr()
			err = dsm.UpdateSyncTs(dsAddr, syncTs)
			if err != nil {
				log.Errorf("update shard sync timestamp failed, dataserver address: %v, err: %v", dsAddr, err)
				return status.Errorf(codes.Internal, "%v", err)
			}
		}
	}

	resp := metaserver.SyncShardInDataServerResponse{}
	reqStream.SendAndClose(&resp)

	return nil
}

func (m *MetaService) AllocateTxids(ctx context.Context, req *metaserver.AllocateTxidsRequest) (*metaserver.AllocateTxidsResponse, error) {
	if err := AllocateTxIDsParamCheck(req); err != nil {
		log.Warnf("AllocateTxID: invalid arguments, err: %v", err)
		return nil, status.Errorf(codes.InvalidArgument, err.Error())
	}

	if !role.GetLeaderShip().IsMetaServerLeader() {
		leader := role.GetLeaderShip().GetMetaServerLeader()
		mscli, _ := metaserver.GetMetaServerConns().GetClient(leader)
		return mscli.AllocateTxids(ctx, req)
	}

	resp := &metaserver.AllocateTxidsResponse{}

	txIDs, err := tso.GetTxIDAllocator().Allocate(req.Count)
	if err != nil {
		return nil, status.Errorf(codes.Internal, err.Error())
	}

	resp.Txids = txIDs

	return resp, nil
}

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
	"sr.ht/moyanhao/bedrock-metaserver/errors"
	"sr.ht/moyanhao/bedrock-metaserver/health_checker"
	"sr.ht/moyanhao/bedrock-metaserver/manager"
	"sr.ht/moyanhao/bedrock-metaserver/model"
	"sr.ht/moyanhao/bedrock-metaserver/role"
	"sr.ht/moyanhao/bedrock-metaserver/scheduler"
	"sr.ht/moyanhao/bedrock-metaserver/tso"
)

type MetaService struct {
	metaserver.UnimplementedMetaServiceServer
}

func forwardToLeaderIfNotLeader[T any](ctx context.Context, req any, forwardFunc func(metaserver.MetaServiceClient) (T, error)) (T, error) {
	var zero T
	if !role.GetLeaderShip().IsMetaServerLeader() {
		leader := role.GetLeaderShip().GetMetaServerLeader()
		mscli, err := metaserver.GetMetaServerConns().GetClient(leader)
		if err != nil {
			log.Errorf("failed to get leader client, leader: %v, err: %v", leader, err)
			return zero, errors.Wrap(err, errors.ErrCodeInternal, "failed to get leader client")
		}
		return forwardFunc(mscli)
	}
	return zero, nil
}

func forwardStreamingToLeader(reqStream metaserver.MetaService_SyncShardInDataServerServer, streamFunc func(metaserver.MetaServiceClient) (metaserver.MetaService_SyncShardInDataServerClient, error)) error {
	if !role.GetLeaderShip().IsMetaServerLeader() {
		leader := role.GetLeaderShip().GetMetaServerLeader()
		mscli, err := metaserver.GetMetaServerConns().GetClient(leader)
		if err != nil {
			log.Errorf("failed to get leader client, leader: %v, err: %v", leader, err)
			return errors.Wrap(err, errors.ErrCodeInternal, "failed to get leader client")
		}

		targetStream, err := streamFunc(mscli)
		if err != nil {
			log.Errorf("Failed to create stream to target node: %v", err)
			return errors.Wrap(err, errors.ErrCodeInternal, "failed to create stream to target node")
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
				return errors.Wrap(err, errors.ErrCodeInternal, "failed to receive from sync shard stream")
			}

			err = targetStream.Send(req)
			if err != nil {
				log.Errorf("Failed to send message to target node: %v", err)
				return errors.Wrap(err, errors.ErrCodeInternal, "failed to send message to target node")
			}
		}

		resp, err := targetStream.CloseAndRecv()
		if err != nil {
			log.Errorf("Failed to receive response from target node: %v", err)
			return errors.Wrap(err, errors.ErrCodeInternal, "failed to receive response from target node")
		}

		return reqStream.SendAndClose(resp)
	}
	return nil
}

func (m *MetaService) HeartBeat(ctx context.Context, req *metaserver.HeartBeatRequest) (*emptypb.Empty, error) {
	err := HeartBeatParamCheck(req)
	if err != nil {
		log.Warnf("HeartBeat: invalid arguments, err: %v", err)
		err = errors.Wrap(err, errors.ErrCodeInvalidArgument, "invalid HeartBeat arguments")
		return nil, err
	}

	if resp, err := forwardToLeaderIfNotLeader(ctx, req, func(client metaserver.MetaServiceClient) (*emptypb.Empty, error) {
		return client.HeartBeat(ctx, req)
	}); err != nil {
		return nil, err
	} else if resp != nil {
		return resp, nil
	}

	if err := manager.GetDataServerManager().MarkActive(req.GetAddr(), true); err != nil {
		log.Errorf("failed to mark dataserver active, addr: %s, err: %v", req.GetAddr(), err)
		err = errors.Wrap(err, errors.ErrCodeInternal, "failed to mark dataserver active")
		return nil, err
	}

	if err := manager.GetDataServerManager().UpdateStatus(req); err != nil {
		log.Errorf("failed to update dataserver status, err: %v", err)
		err = errors.Wrap(err, errors.ErrCodeInternal, "failed to update dataserver status")
		return nil, err
	}

	log.Infof("receive heartbeat from %s", req.Addr)

	return &emptypb.Empty{}, nil
}

func (m *MetaService) ScanShardRange(ctx context.Context, req *metaserver.ScanShardRangeRequest) (*metaserver.ScanShardRangeResponse, error) {
	log.Infof("ScanShardRange request: %v", req)

	err := ScanStorageShardsParamCheck(req)
	if err != nil {
		log.Warnf("ScanShardRange: invalid arguments, err: %v", err)
		err = errors.Wrap(err, errors.ErrCodeInvalidArgument, "invalid ScanShardRange arguments")
		return nil, err
	}

	if resp, err := forwardToLeaderIfNotLeader(ctx, req, func(client metaserver.MetaServiceClient) (*metaserver.ScanShardRangeResponse, error) {
		return client.ScanShardRange(ctx, req)
	}); err != nil {
		return nil, err
	} else if resp != nil {
		return resp, nil
	}

	resp := &metaserver.ScanShardRangeResponse{}

	shardsAndRange, err := manager.GetStorageManager().ScanShardRange(model.StorageID(req.StorageId), req.RangeStart)
	if err != nil {
		log.Errorf("get storage shards failed, err: %v", err)
		err = errors.Wrap(err, errors.ErrCodeInternal, "failed to get storage shards")
		return resp, err
	}

	for _, s := range shardsAndRange {
		resp.Ranges = append(resp.Ranges, &metaserver.ScanShardRangeResponse_ShardRange{
			ShardId:    uint64(s.ShardID),
			RangeStart: s.RangeStart,
			RangeEnd:   s.RangeEnd,
			LeaderAddr: s.LeaderAddr,
			Addrs:      s.Addrs,
		})
	}
	log.Infof("resp: %v", resp)

	if len(resp.Ranges) == 0 || bytes.Equal(resp.Ranges[len(resp.Ranges)-1].RangeEnd, scheduler.MaxKey) {
		resp.IsEnd = true
	}

	return resp, nil
}

func (m *MetaService) Info(ctx context.Context, req *metaserver.InfoRequest) (*metaserver.InfoResponse, error) {
	log.Debug("Info request")

	err := InfoParamCheck(req)
	if err != nil {
		log.Warnf("Info: invalid arguments, err: %v", err)
		err = errors.Wrap(err, errors.ErrCodeInvalidArgument, "invalid Info arguments")
		return nil, err
	}

	resp := &metaserver.InfoResponse{}

	resp.LeaderAddr = role.GetLeaderShip().GetMetaServerLeader()

	return resp, nil
}

func (m *MetaService) CreateStorage(ctx context.Context, req *metaserver.CreateStorageRequest) (*metaserver.CreateStorageResponse, error) {
	err := CreateStorageParamCheck(req)
	if err != nil {
		log.Warnf("CreateStorage: invalid arguments, err: %v", err)
		err = errors.Wrap(err, errors.ErrCodeInvalidArgument, "invalid CreateStorage arguments")
		return nil, err
	}

	if resp, err := forwardToLeaderIfNotLeader(ctx, req, func(client metaserver.MetaServiceClient) (*metaserver.CreateStorageResponse, error) {
		return client.CreateStorage(ctx, req)
	}); err != nil {
		return nil, err
	} else if resp != nil {
		return resp, nil
	}

	resp := &metaserver.CreateStorageResponse{}

	st, err := manager.GetStorageManager().GetStorageByName(req.Name)
	if err != nil && !errors.IsNotFound(err) {
		log.Errorf("check storage by name failed, err: %v", err)
		err = errors.Wrap(err, errors.ErrCodeInternal, "failed to check storage by name")
		return resp, err
	}

	if st != nil {
		log.Errorf("storage name %v already exists", req.Name)
		err = errors.New(errors.ErrCodeAlreadyExists, "storage name already exists")
		return resp, err
	}

	storage, err := scheduler.GetShardAllocator().AllocateNewStorage(req.Name, req.InitialRangeCount)
	if err != nil {
		log.Errorf("create storage failed, err: %v", err)
		err = errors.Wrap(err, errors.ErrCodeInternal, "failed to create storage")
		return resp, err
	}

	resp.Id = uint64(storage.ID)

	log.Infof("successfully create storage, id: %v", resp.Id)

	return resp, nil
}

func (m *MetaService) DeleteStorage(ctx context.Context, req *metaserver.DeleteStorageRequest) (*metaserver.DeleteStorageResponse, error) {
	err := DeleteStorageParamCheck(req)
	if err != nil {
		log.Warnf("DeleteStorage: invalid arguments, err: %v", err)
		err = errors.Wrap(err, errors.ErrCodeInvalidArgument, "invalid DeleteStorage arguments")
		return nil, err
	}

	if resp, err := forwardToLeaderIfNotLeader(ctx, req, func(client metaserver.MetaServiceClient) (*metaserver.DeleteStorageResponse, error) {
		return client.DeleteStorage(ctx, req)
	}); err != nil {
		return nil, err
	} else if resp != nil {
		return resp, nil
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
		err = errors.Wrap(err, errors.ErrCodeInternal, "failed to delete storage")
		return resp, err
	}

	return resp, nil
}

func (m *MetaService) UndeleteStorage(ctx context.Context, req *metaserver.UndeleteStorageRequest) (*metaserver.UndeleteStorageResponse, error) {
	err := UndeleteStorageParamCheck(req)
	if err != nil {
		log.Warnf("UndeleteStorage: invalid arguments, err: %v", err)
		err = errors.Wrap(err, errors.ErrCodeInvalidArgument, "invalid UndeleteStorage arguments")
		return nil, err
	}

	if resp, err := forwardToLeaderIfNotLeader(ctx, req, func(client metaserver.MetaServiceClient) (*metaserver.UndeleteStorageResponse, error) {
		return client.UndeleteStorage(ctx, req)
	}); err != nil {
		return nil, err
	} else if resp != nil {
		return resp, nil
	}

	resp := &metaserver.UndeleteStorageResponse{}

	err = manager.GetStorageManager().StorageUndelete(model.StorageID(req.Id))
	if err != nil {
		log.Errorf("undelete storage failed, err: %v", err)
		err = errors.Wrap(err, errors.ErrCodeInternal, "failed to undelete storage")
		return resp, err
	}

	return resp, nil
}

func (m *MetaService) RenameStorage(ctx context.Context, req *metaserver.RenameStorageRequest) (*metaserver.RenameStorageResponse, error) {
	err := RenameStorageParamCheck(req)
	if err != nil {
		log.Warnf("RenameStorage: invalid arguments, err: %v", err)
		err = errors.Wrap(err, errors.ErrCodeInvalidArgument, "invalid RenameStorage arguments")
		return nil, err
	}

	if resp, err := forwardToLeaderIfNotLeader(ctx, req, func(client metaserver.MetaServiceClient) (*metaserver.RenameStorageResponse, error) {
		return client.RenameStorage(ctx, req)
	}); err != nil {
		return nil, err
	} else if resp != nil {
		return resp, nil
	}

	resp := &metaserver.RenameStorageResponse{}

	err = manager.GetStorageManager().StorageRename(model.StorageID(req.Id), req.NewName)
	if err != nil {
		log.Errorf("rename storage failed, err: %v", err)
		err = errors.Wrap(err, errors.ErrCodeInternal, "failed to rename storage")
		return resp, err
	}

	return resp, nil
}

func (m *MetaService) ResizeStorage(ctx context.Context, req *metaserver.ResizeStorageRequest) (*metaserver.ResizeStorageResponse, error) {
	err := ResizeStorageParamCheck(req)
	if err != nil {
		log.Warnf("ResizeStorage: invalid arguments, err: %v", err)
		err = errors.Wrap(err, errors.ErrCodeInvalidArgument, "invalid ResizeStorage arguments")
		return nil, err
	}

	if resp, err := forwardToLeaderIfNotLeader(ctx, req, func(client metaserver.MetaServiceClient) (*metaserver.ResizeStorageResponse, error) {
		return client.ResizeStorage(ctx, req)
	}); err != nil {
		return nil, err
	} else if resp != nil {
		return resp, nil
	}

	resp := &metaserver.ResizeStorageResponse{}

	st, err := manager.GetStorageManager().GetStorage(model.StorageID(req.Id))
	if err != nil {
		log.Warnf("ResizeStorage: failed to get storage, id=%d, err: %v", req.Id, err)
		err = errors.Wrap(err, errors.ErrCodeNotFound, "storage not found")
		return nil, err
	}

	if uint32(st.LastShardISN) >= uint32(req.NewShardCount) {
		log.Warnf("ResizeStorage: new shard count %d is not larger than current %d", req.NewShardCount, st.LastShardISN)
		err = errors.New(errors.ErrCodeInvalidArgument, "new shard count must be larger than current count")
		return nil, err
	}

	expandCount := req.NewShardCount - uint64(st.LastShardISN)
	err = scheduler.GetShardAllocator().ExpandStorage(st.ID, uint32(expandCount))
	if err != nil {
		log.Errorf("ResizeStorage: failed to expand storage, id=%d, err: %v", st.ID, err)
		err = errors.Wrap(err, errors.ErrCodeInternal, "failed to expand storage")
		return nil, err
	}

	return resp, nil
}

func (m *MetaService) StorageInfo(ctx context.Context, req *metaserver.StorageInfoRequest) (*metaserver.StorageInfoResponse, error) {
	log.Infof("GetStorages: req: %v", req)
	err := GetStoragesParamCheck(req)
	if err != nil {
		log.Warnf("GetStorages: invalid arguments, err: %v", err)
		err = errors.Wrap(err, errors.ErrCodeInvalidArgument, "invalid GetStorages arguments")
		return nil, err
	}

	if resp, err := forwardToLeaderIfNotLeader(ctx, req, func(client metaserver.MetaServiceClient) (*metaserver.StorageInfoResponse, error) {
		return client.StorageInfo(ctx, req)
	}); err != nil {
		return nil, err
	} else if resp != nil {
		return resp, nil
	}

	resp := &metaserver.StorageInfoResponse{}

	for _, id := range req.Ids {
		st, err := manager.GetStorageManager().GetStorage(model.StorageID(id))
		if err != nil {
			log.Errorf("failed to get storage, id=%d, err: %v", id, err)
			err = errors.Wrap(err, errors.ErrCodeNotFound, "storage not found")
			return nil, err
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
		st, error := manager.GetStorageManager().GetStorageByName(name)
		if error != nil {
			return nil, status.Errorf(codes.Internal, "get storage by name failed, name: %s, err: %v", name, err)
		}

		resp.Storages = append(resp.Storages, &metaserver.Storage{
			Id:           uint32(st.ID),
			Name:         st.Name,
			CreateTs:     timestamppb.New(st.CreateTs),
			DeletedTs:    timestamppb.New(st.DeleteTs),
			Owner:        st.Owner,
			LastShardIsn: uint32(st.LastShardISN),
		})
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

	if resp, err := forwardToLeaderIfNotLeader(ctx, req, func(client metaserver.MetaServiceClient) (*metaserver.AddDataServerResponse, error) {
		return client.AddDataServer(ctx, req)
	}); err != nil {
		return nil, err
	} else if resp != nil {
		return resp, nil
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

	if resp, err := forwardToLeaderIfNotLeader(ctx, req, func(client metaserver.MetaServiceClient) (*metaserver.RemoveDataServerResponse, error) {
		return client.RemoveDataServer(ctx, req)
	}); err != nil {
		return nil, err
	} else if resp != nil {
		return resp, nil
	}

	resp := &metaserver.RemoveDataServerResponse{}
	_, _, err = net.SplitHostPort(req.Addr)
	if err != nil {
		return nil, status.Errorf(codes.InvalidArgument, "invalid data server address format")
	}

	dm := manager.GetDataServerManager()
	if !dm.IsDataServerExists(req.Addr) {
		return nil, status.Errorf(codes.NotFound, "data server not found")
	}

	go func() {
		err = health_checker.ClearDataserver(req.Addr)
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

	if resp, err := forwardToLeaderIfNotLeader(ctx, req, func(client metaserver.MetaServiceClient) (*metaserver.ListDataServerResponse, error) {
		return client.ListDataServer(ctx, req)
	}); err != nil {
		return nil, err
	} else if resp != nil {
		return resp, nil
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
				Free:            ds.FreeCapacity,
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
		err = errors.Wrap(err, errors.ErrCodeInvalidArgument, "invalid UpdateDataServer arguments")
		return nil, err
	}

	if resp, err := forwardToLeaderIfNotLeader(ctx, req, func(client metaserver.MetaServiceClient) (*metaserver.UpdateDataServerResponse, error) {
		return client.UpdateDataServer(ctx, req)
	}); err != nil {
		return nil, err
	} else if resp != nil {
		return resp, nil
	}

	resp := &metaserver.UpdateDataServerResponse{}

	return resp, nil
}

func (m *MetaService) ShardInfo(ctx context.Context, req *metaserver.ShardInfoRequest) (*metaserver.ShardInfoResponse, error) {
	if err := ShardInfoParamCheck(req); err != nil {
		log.Warnf("ShardInfo: invalid arguments, err: %v", err)
		return nil, status.Errorf(codes.InvalidArgument, err.Error())
	}

	if resp, err := forwardToLeaderIfNotLeader(ctx, req, func(client metaserver.MetaServiceClient) (*metaserver.ShardInfoResponse, error) {
		return client.ShardInfo(ctx, req)
	}); err != nil {
		return nil, err
	} else if resp != nil {
		return resp, nil
	}

	resp := &metaserver.ShardInfoResponse{}

	sm := manager.GetShardManager()
	if sm == nil {
		return resp, errors.New(errors.ErrCodeInternal, "shard manager is nil")
	}

	shard, err := sm.GetShard(model.ShardID(req.GetId()))
	if err != nil {
		log.Errorf("Failed to get shard: %v", err)
		err = errors.Wrap(err, errors.ErrCodeNotFound, "shard not found")
		return nil, err
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
		err = errors.Wrap(err, errors.ErrCodeInvalidArgument, "invalid CreateShard arguments")
		return nil, err
	}

	if resp, err := forwardToLeaderIfNotLeader(ctx, req, func(client metaserver.MetaServiceClient) (*metaserver.CreateShardResponse, error) {
		return client.CreateShard(ctx, req)
	}); err != nil {
		return nil, err
	} else if resp != nil {
		return resp, nil
	}

	resp := &metaserver.CreateShardResponse{}

	stm := manager.GetStorageManager()
	st, err := stm.GetStorage(model.StorageID(req.StorageId))
	if err != nil || st == nil {
		log.Warnf("no such storage, storage id: %v", req.StorageId)
		err = errors.New(errors.ErrCodeNotFound, "storage not found")
		return nil, err
	}

	shardID := model.GenerateShardID(model.StorageID(req.StorageId), model.ShardISN(req.ShardIsn))
	shm := manager.GetShardManager()
	_, err = shm.GetShard(shardID)
	if err == nil {
		log.Warnf("shard already exists, shard id: %v", shardID)
		err = errors.New(errors.ErrCodeAlreadyExists, "shard already exists")
		return nil, err
	}

	shard, err := shm.CreateNewShardByIDs(model.StorageID(req.StorageId), model.ShardISN(req.ShardIsn))
	if err != nil {
		log.Warnf("failed to create new shard by id, err: %v", err)
		err = errors.Wrap(err, errors.ErrCodeInternal, "failed to create shard")
		return nil, err
	}

	sa := scheduler.GetShardAllocator()
	// replicates, err := sa.AllocateShardReplicates(shard.ID, 3)
	_, err = sa.AllocateShardReplicates(shard.ID(), scheduler.DefaultReplicatesCount, req.RangeStart, req.RangeEnd)
	if err != nil {
		log.Warnf("failed to allocate shard replicates, err: %v", err)
		err = errors.Wrap(err, errors.ErrCodeInternal, "failed to allocate shard replicates")
		return nil, err
	}

	return resp, nil
}

func (m *MetaService) RemoveShard(ctx context.Context, req *metaserver.RemoveShardRequest) (*metaserver.RemoveShardResponse, error) {
	if err := RemoveShardParamCheck(req); err != nil {
		log.Warnf("RemoveShard: invalid arguments, err: %v", err)
		err = errors.Wrap(err, errors.ErrCodeInvalidArgument, "invalid RemoveShard arguments")
		return nil, err
	}

	if resp, err := forwardToLeaderIfNotLeader(ctx, req, func(client metaserver.MetaServiceClient) (*metaserver.RemoveShardResponse, error) {
		return client.RemoveShard(ctx, req)
	}); err != nil {
		return nil, err
	} else if resp != nil {
		return resp, nil
	}

	resp := &metaserver.RemoveShardResponse{}

	shardID := model.GenerateShardID(model.StorageID(req.StorageId), model.ShardISN(req.ShardIsn))
	shm := manager.GetShardManager()
	err := shm.MarkDelete(shardID)
	if err != nil {
		log.Warnf("failed to mark delete for shard, err: %v", err)
		err = errors.Wrap(err, errors.ErrCodeInternal, "failed to mark delete for shard")
		return nil, err
	}

	return resp, nil
}

func (m *MetaService) SyncShardInDataServer(reqStream metaserver.MetaService_SyncShardInDataServerServer) error {
	log.Info("sync shard in dataserver ...")

	if err := forwardStreamingToLeader(reqStream, func(client metaserver.MetaServiceClient) (metaserver.MetaService_SyncShardInDataServerClient, error) {
		return client.SyncShardInDataServer(context.Background())
	}); err != nil {
		return err
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

	if resp, err := forwardToLeaderIfNotLeader(ctx, req, func(client metaserver.MetaServiceClient) (*metaserver.AllocateTxidsResponse, error) {
		return client.AllocateTxids(ctx, req)
	}); err != nil {
		return nil, err
	} else if resp != nil {
		return resp, nil
	}

	resp := &metaserver.AllocateTxidsResponse{}

	txIdStart, txIdEnd, err := tso.GetTxIDAllocator().Allocate(req.Count, true)
	if err != nil {
		return nil, status.Errorf(codes.Internal, err.Error())
	}

	resp.TxidRangeStart = txIdStart
	resp.TxidRangeEnd = txIdEnd

	return resp, nil
}

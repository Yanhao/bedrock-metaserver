package dao

import (
	"context"
	"errors"
	"fmt"
	"net"
	"strings"
	"time"

	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/timestamppb"

	"sr.ht/moyanhao/bedrock-metaserver/dal/dto"
	"sr.ht/moyanhao/bedrock-metaserver/kvengine"
	"sr.ht/moyanhao/bedrock-metaserver/model"
	"sr.ht/moyanhao/bedrock-metaserver/utils/log"
)

const (
	KvPrefixIdc             = "/idcs/"
	KvPrefixDataServer      = "/dataservers/"
	KvPrefixDataServerInIdc = "/dataservers_in_idc/"
)

func idcKey(idc string) string {
	return fmt.Sprintf("%s%s", KvPrefixIdc, strings.TrimSpace(idc))
}

func dataServerKey(addr string) string {
	return fmt.Sprintf("%s%s", KvPrefixDataServer, strings.TrimSpace(addr))
}

func dataServerInIdcKey(idc, addr string) string {
	return fmt.Sprintf("%s%s", dataServerInIdcPrefixKey(idc), addr)
}

func dataServerInIdcPrefixKey(idc string) string {
	return fmt.Sprintf("%s%s/", KvPrefixDataServerInIdc, idc)
}

func KvGetDataServer(addr string) (*model.DataServer, error) {
	ec := kvengine.GetEtcdClient()
	resp, err := ec.KV.Get(context.Background(), dataServerKey(addr))
	if err != nil || resp.Count == 0 {
		return nil, ErrNoSuchShard
	}

	if resp.Count != 1 {
		return nil, errors.New("")
	}

	pbDataServer := &dto.DataServer{}
	for _, item := range resp.Kvs {
		err := proto.Unmarshal(item.Value, pbDataServer)
		if err != nil {
			return nil, err
		}
	}

	hostStr, portStr, err := net.SplitHostPort(addr)
	if err != nil {
		log.Warn("parse address failed, err: %v", err)
	}

	dataServer := &model.DataServer{
		Ip:              hostStr,
		Port:            portStr,
		Free:            pbDataServer.GetFree(),
		Capacity:        pbDataServer.GetCapacity(),
		Status:          model.LiveStatusActive,
		LastHeartBeatTs: time.Time{},
		LastSyncTs:      pbDataServer.GetLastSyncTs(),
	}

	return dataServer, nil
}

func KvPutDataServer(dataserver *model.DataServer) error {
	var status dto.DataServer_LiveStatus
	if dataserver.Status == model.LiveStatusActive {
		status = dto.DataServer_ACTIVE
	} else if dataserver.Status == model.LiveStatusInactive {
		status = dto.DataServer_INACTIVE
	} else if dataserver.Status == model.LiveStatusOffline {
		status = dto.DataServer_OFFLINE
	}

	pbDataServer := &dto.DataServer{
		Ip:       dataserver.Ip,
		Port:     dataserver.Port,
		Free:     dataserver.Free,
		Capacity: dataserver.Capacity,

		LastHeartbeatTs: timestamppb.New(dataserver.LastHeartBeatTs),

		Status:     status,
		LastSyncTs: dataserver.LastSyncTs,
	}

	value, err := proto.Marshal(pbDataServer)
	if err != nil {
		log.Warn("failed to encode dataserver to pb, dataserver=%v", dataserver)
		return err
	}

	ec := kvengine.GetEtcdClient()
	_, err = ec.Put(context.Background(), dataServerKey(dataserver.Addr()), string(value))
	if err != nil {
		log.Warn("failed to save dataserver to etcd, dataserver=%v", dataserver)
		return err
	}

	return nil
}

func KvDeleteDataServer(addr string) error {
	ec := kvengine.GetEtcdClient()
	_, err := ec.Delete(context.Background(), dataServerKey(addr))
	if err != nil {
		log.Warn("failed to delete dataserver from kv")
		return err
	}

	return nil
}

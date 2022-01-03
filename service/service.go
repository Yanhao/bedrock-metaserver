package service

import (
	"context"
	"net"
	"strconv"

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

	ipStr := strconv.FormatUint(uint64(req.Addr.Ip), 32)
	portStr := strconv.FormatUint(uint64(req.Addr.Port), 32)

	addr := net.JoinHostPort(ipStr, portStr)
	server, ok := metadata.DataServers[addr]
	if !ok {
		log.Warn("no such server in record: %s", addr)
		return nil, status.Errorf(codes.NotFound, "no such server: %s", addr)
	}

	server.MarkActive(true)

	return &emptypb.Empty{}, nil
}

func (m *MetaService) GetShardRoutes(ctx context.Context, req *messages.GetShardRoutesRequest) (*messages.GetShardRoutesResponse, error) {
	panic("")
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

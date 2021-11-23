package service

import (
	"context"
	"sr.ht/moyanhao/bedrock-metaserver/messages"
)

type MetaService struct {
	messages.UnimplementedMetaServiceServer
}

func (m *MetaService) HeartBeat(ctx context.Context, req *messages.HeartBeatRequest) (*messages.HeartBeatResponse, error) {
	panic("")
}

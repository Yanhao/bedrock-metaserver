package service

import (
	"context"
	"sr.ht/moyanhao/bedrock-metaserver/messages"
)

type MetaService struct {
	messages.UnimplementedMetaServiceServer
}

func (m *MetaService) Hello(ctx context.Context, req *messages.HelloRequest) (*messages.HelloResponse, error) {
	return &messages.HelloResponse{
		Msg: "hello world",
	}, nil
}
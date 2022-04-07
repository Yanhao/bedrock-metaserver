// Code generated by protoc-gen-go-grpc. DO NOT EDIT.

package api

import (
	context "context"
	grpc "google.golang.org/grpc"
	codes "google.golang.org/grpc/codes"
	status "google.golang.org/grpc/status"
)

// This is a compile-time assertion to ensure that this generated file
// is compatible with the grpc package it is being compiled against.
// Requires gRPC-Go v1.32.0 or later.
const _ = grpc.SupportPackageIsVersion7

// DataServiceClient is the client API for DataService service.
//
// For semantics around ctx use and closing/ending streaming RPCs, please refer to https://pkg.go.dev/google.golang.org/grpc/?tab=doc#ClientConn.NewStream.
type DataServiceClient interface {
	// rpc SplitShard(SplitShardRequest) returns (SplitShardResponse);
	// rpc MergeShard(MergeShardRequest) returns (MergeShardResponse);
	CreateShard(ctx context.Context, in *CreateShardRequest, opts ...grpc.CallOption) (*CreateShardResponse, error)
	DeleteShard(ctx context.Context, in *DeleteShardRequest, opts ...grpc.CallOption) (*DeleteShardResponse, error)
	PullShardData(ctx context.Context, in *PullShardDataRequest, opts ...grpc.CallOption) (*PullShardDataResponse, error)
	// rpc TransferShard(TransferShardRequest) returns (TransferShardResponse);
	// rpc AddShardReplica(AddShardReplicaRequest) returns (AddShardReplicaResponse);
	// rpc DeleteShardReplica(DeleteShardReplicaRequest) returns (DeleteShardReplicaResponse);
	TransferShardLeader(ctx context.Context, in *TransferShardLeaderRequest, opts ...grpc.CallOption) (*TransferShardLeaderResponse, error)
	ShardRead(ctx context.Context, in *ShardReadRequest, opts ...grpc.CallOption) (*ShardReadResponse, error)
	ShardWrite(ctx context.Context, in *ShardWriteRequest, opts ...grpc.CallOption) (*ShardWriteResponse, error)
}

type dataServiceClient struct {
	cc grpc.ClientConnInterface
}

func NewDataServiceClient(cc grpc.ClientConnInterface) DataServiceClient {
	return &dataServiceClient{cc}
}

func (c *dataServiceClient) CreateShard(ctx context.Context, in *CreateShardRequest, opts ...grpc.CallOption) (*CreateShardResponse, error) {
	out := new(CreateShardResponse)
	err := c.cc.Invoke(ctx, "/service_pb.DataService/CreateShard", in, out, opts...)
	if err != nil {
		return nil, err
	}
	return out, nil
}

func (c *dataServiceClient) DeleteShard(ctx context.Context, in *DeleteShardRequest, opts ...grpc.CallOption) (*DeleteShardResponse, error) {
	out := new(DeleteShardResponse)
	err := c.cc.Invoke(ctx, "/service_pb.DataService/DeleteShard", in, out, opts...)
	if err != nil {
		return nil, err
	}
	return out, nil
}

func (c *dataServiceClient) PullShardData(ctx context.Context, in *PullShardDataRequest, opts ...grpc.CallOption) (*PullShardDataResponse, error) {
	out := new(PullShardDataResponse)
	err := c.cc.Invoke(ctx, "/service_pb.DataService/PullShardData", in, out, opts...)
	if err != nil {
		return nil, err
	}
	return out, nil
}

func (c *dataServiceClient) TransferShardLeader(ctx context.Context, in *TransferShardLeaderRequest, opts ...grpc.CallOption) (*TransferShardLeaderResponse, error) {
	out := new(TransferShardLeaderResponse)
	err := c.cc.Invoke(ctx, "/service_pb.DataService/TransferShardLeader", in, out, opts...)
	if err != nil {
		return nil, err
	}
	return out, nil
}

func (c *dataServiceClient) ShardRead(ctx context.Context, in *ShardReadRequest, opts ...grpc.CallOption) (*ShardReadResponse, error) {
	out := new(ShardReadResponse)
	err := c.cc.Invoke(ctx, "/service_pb.DataService/ShardRead", in, out, opts...)
	if err != nil {
		return nil, err
	}
	return out, nil
}

func (c *dataServiceClient) ShardWrite(ctx context.Context, in *ShardWriteRequest, opts ...grpc.CallOption) (*ShardWriteResponse, error) {
	out := new(ShardWriteResponse)
	err := c.cc.Invoke(ctx, "/service_pb.DataService/ShardWrite", in, out, opts...)
	if err != nil {
		return nil, err
	}
	return out, nil
}

// DataServiceServer is the server API for DataService service.
// All implementations must embed UnimplementedDataServiceServer
// for forward compatibility
type DataServiceServer interface {
	// rpc SplitShard(SplitShardRequest) returns (SplitShardResponse);
	// rpc MergeShard(MergeShardRequest) returns (MergeShardResponse);
	CreateShard(context.Context, *CreateShardRequest) (*CreateShardResponse, error)
	DeleteShard(context.Context, *DeleteShardRequest) (*DeleteShardResponse, error)
	PullShardData(context.Context, *PullShardDataRequest) (*PullShardDataResponse, error)
	// rpc TransferShard(TransferShardRequest) returns (TransferShardResponse);
	// rpc AddShardReplica(AddShardReplicaRequest) returns (AddShardReplicaResponse);
	// rpc DeleteShardReplica(DeleteShardReplicaRequest) returns (DeleteShardReplicaResponse);
	TransferShardLeader(context.Context, *TransferShardLeaderRequest) (*TransferShardLeaderResponse, error)
	ShardRead(context.Context, *ShardReadRequest) (*ShardReadResponse, error)
	ShardWrite(context.Context, *ShardWriteRequest) (*ShardWriteResponse, error)
	mustEmbedUnimplementedDataServiceServer()
}

// UnimplementedDataServiceServer must be embedded to have forward compatible implementations.
type UnimplementedDataServiceServer struct {
}

func (UnimplementedDataServiceServer) CreateShard(context.Context, *CreateShardRequest) (*CreateShardResponse, error) {
	return nil, status.Errorf(codes.Unimplemented, "method CreateShard not implemented")
}
func (UnimplementedDataServiceServer) DeleteShard(context.Context, *DeleteShardRequest) (*DeleteShardResponse, error) {
	return nil, status.Errorf(codes.Unimplemented, "method DeleteShard not implemented")
}
func (UnimplementedDataServiceServer) PullShardData(context.Context, *PullShardDataRequest) (*PullShardDataResponse, error) {
	return nil, status.Errorf(codes.Unimplemented, "method PullShardData not implemented")
}
func (UnimplementedDataServiceServer) TransferShardLeader(context.Context, *TransferShardLeaderRequest) (*TransferShardLeaderResponse, error) {
	return nil, status.Errorf(codes.Unimplemented, "method TransferShardLeader not implemented")
}
func (UnimplementedDataServiceServer) ShardRead(context.Context, *ShardReadRequest) (*ShardReadResponse, error) {
	return nil, status.Errorf(codes.Unimplemented, "method ShardRead not implemented")
}
func (UnimplementedDataServiceServer) ShardWrite(context.Context, *ShardWriteRequest) (*ShardWriteResponse, error) {
	return nil, status.Errorf(codes.Unimplemented, "method ShardWrite not implemented")
}
func (UnimplementedDataServiceServer) mustEmbedUnimplementedDataServiceServer() {}

// UnsafeDataServiceServer may be embedded to opt out of forward compatibility for this service.
// Use of this interface is not recommended, as added methods to DataServiceServer will
// result in compilation errors.
type UnsafeDataServiceServer interface {
	mustEmbedUnimplementedDataServiceServer()
}

func RegisterDataServiceServer(s grpc.ServiceRegistrar, srv DataServiceServer) {
	s.RegisterService(&DataService_ServiceDesc, srv)
}

func _DataService_CreateShard_Handler(srv interface{}, ctx context.Context, dec func(interface{}) error, interceptor grpc.UnaryServerInterceptor) (interface{}, error) {
	in := new(CreateShardRequest)
	if err := dec(in); err != nil {
		return nil, err
	}
	if interceptor == nil {
		return srv.(DataServiceServer).CreateShard(ctx, in)
	}
	info := &grpc.UnaryServerInfo{
		Server:     srv,
		FullMethod: "/service_pb.DataService/CreateShard",
	}
	handler := func(ctx context.Context, req interface{}) (interface{}, error) {
		return srv.(DataServiceServer).CreateShard(ctx, req.(*CreateShardRequest))
	}
	return interceptor(ctx, in, info, handler)
}

func _DataService_DeleteShard_Handler(srv interface{}, ctx context.Context, dec func(interface{}) error, interceptor grpc.UnaryServerInterceptor) (interface{}, error) {
	in := new(DeleteShardRequest)
	if err := dec(in); err != nil {
		return nil, err
	}
	if interceptor == nil {
		return srv.(DataServiceServer).DeleteShard(ctx, in)
	}
	info := &grpc.UnaryServerInfo{
		Server:     srv,
		FullMethod: "/service_pb.DataService/DeleteShard",
	}
	handler := func(ctx context.Context, req interface{}) (interface{}, error) {
		return srv.(DataServiceServer).DeleteShard(ctx, req.(*DeleteShardRequest))
	}
	return interceptor(ctx, in, info, handler)
}

func _DataService_PullShardData_Handler(srv interface{}, ctx context.Context, dec func(interface{}) error, interceptor grpc.UnaryServerInterceptor) (interface{}, error) {
	in := new(PullShardDataRequest)
	if err := dec(in); err != nil {
		return nil, err
	}
	if interceptor == nil {
		return srv.(DataServiceServer).PullShardData(ctx, in)
	}
	info := &grpc.UnaryServerInfo{
		Server:     srv,
		FullMethod: "/service_pb.DataService/PullShardData",
	}
	handler := func(ctx context.Context, req interface{}) (interface{}, error) {
		return srv.(DataServiceServer).PullShardData(ctx, req.(*PullShardDataRequest))
	}
	return interceptor(ctx, in, info, handler)
}

func _DataService_TransferShardLeader_Handler(srv interface{}, ctx context.Context, dec func(interface{}) error, interceptor grpc.UnaryServerInterceptor) (interface{}, error) {
	in := new(TransferShardLeaderRequest)
	if err := dec(in); err != nil {
		return nil, err
	}
	if interceptor == nil {
		return srv.(DataServiceServer).TransferShardLeader(ctx, in)
	}
	info := &grpc.UnaryServerInfo{
		Server:     srv,
		FullMethod: "/service_pb.DataService/TransferShardLeader",
	}
	handler := func(ctx context.Context, req interface{}) (interface{}, error) {
		return srv.(DataServiceServer).TransferShardLeader(ctx, req.(*TransferShardLeaderRequest))
	}
	return interceptor(ctx, in, info, handler)
}

func _DataService_ShardRead_Handler(srv interface{}, ctx context.Context, dec func(interface{}) error, interceptor grpc.UnaryServerInterceptor) (interface{}, error) {
	in := new(ShardReadRequest)
	if err := dec(in); err != nil {
		return nil, err
	}
	if interceptor == nil {
		return srv.(DataServiceServer).ShardRead(ctx, in)
	}
	info := &grpc.UnaryServerInfo{
		Server:     srv,
		FullMethod: "/service_pb.DataService/ShardRead",
	}
	handler := func(ctx context.Context, req interface{}) (interface{}, error) {
		return srv.(DataServiceServer).ShardRead(ctx, req.(*ShardReadRequest))
	}
	return interceptor(ctx, in, info, handler)
}

func _DataService_ShardWrite_Handler(srv interface{}, ctx context.Context, dec func(interface{}) error, interceptor grpc.UnaryServerInterceptor) (interface{}, error) {
	in := new(ShardWriteRequest)
	if err := dec(in); err != nil {
		return nil, err
	}
	if interceptor == nil {
		return srv.(DataServiceServer).ShardWrite(ctx, in)
	}
	info := &grpc.UnaryServerInfo{
		Server:     srv,
		FullMethod: "/service_pb.DataService/ShardWrite",
	}
	handler := func(ctx context.Context, req interface{}) (interface{}, error) {
		return srv.(DataServiceServer).ShardWrite(ctx, req.(*ShardWriteRequest))
	}
	return interceptor(ctx, in, info, handler)
}

// DataService_ServiceDesc is the grpc.ServiceDesc for DataService service.
// It's only intended for direct use with grpc.RegisterService,
// and not to be introspected or modified (even as a copy)
var DataService_ServiceDesc = grpc.ServiceDesc{
	ServiceName: "service_pb.DataService",
	HandlerType: (*DataServiceServer)(nil),
	Methods: []grpc.MethodDesc{
		{
			MethodName: "CreateShard",
			Handler:    _DataService_CreateShard_Handler,
		},
		{
			MethodName: "DeleteShard",
			Handler:    _DataService_DeleteShard_Handler,
		},
		{
			MethodName: "PullShardData",
			Handler:    _DataService_PullShardData_Handler,
		},
		{
			MethodName: "TransferShardLeader",
			Handler:    _DataService_TransferShardLeader_Handler,
		},
		{
			MethodName: "ShardRead",
			Handler:    _DataService_ShardRead_Handler,
		},
		{
			MethodName: "ShardWrite",
			Handler:    _DataService_ShardWrite_Handler,
		},
	},
	Streams:  []grpc.StreamDesc{},
	Metadata: "service.proto",
}
package rpc

import (
	"context"

	"google.golang.org/grpc"
)

const MasterServiceName = "mapreduce.v1.MasterService"

type MasterServiceServer interface {
	SubmitJob(context.Context, *SubmitJobRequest) (*SubmitJobResponse, error)
	RegisterWorker(context.Context, *RegisterWorkerRequest) (*RegisterWorkerResponse, error)
	RequestTask(context.Context, *RequestTaskRequest) (*RequestTaskResponse, error)
	SubmitTaskResult(context.Context, *SubmitTaskResultRequest) (*SubmitTaskResultResponse, error)
	Heartbeat(context.Context, *HeartbeatRequest) (*HeartbeatResponse, error)
	GetJobStatus(context.Context, *GetJobStatusRequest) (*GetJobStatusResponse, error)
}

func RegisterMasterServiceServer(server *grpc.Server, implementation MasterServiceServer) {
	server.RegisterService(&grpc.ServiceDesc{
		ServiceName: MasterServiceName,
		HandlerType: (*MasterServiceServer)(nil),
		Methods: []grpc.MethodDesc{
			{MethodName: "SubmitJob", Handler: unaryHandler(implementation.SubmitJob)},
			{MethodName: "RegisterWorker", Handler: unaryHandler(implementation.RegisterWorker)},
			{MethodName: "RequestTask", Handler: unaryHandler(implementation.RequestTask)},
			{MethodName: "SubmitTaskResult", Handler: unaryHandler(implementation.SubmitTaskResult)},
			{MethodName: "Heartbeat", Handler: unaryHandler(implementation.Heartbeat)},
			{MethodName: "GetJobStatus", Handler: unaryHandler(implementation.GetJobStatus)},
		},
		Streams:  []grpc.StreamDesc{},
		Metadata: "proto/mapreduce.proto",
	}, implementation)
}

type unaryFunc[Req any, Resp any] func(context.Context, *Req) (*Resp, error)

func unaryHandler[Req any, Resp any](fn unaryFunc[Req, Resp]) func(any, context.Context, func(any) error, grpc.UnaryServerInterceptor) (any, error) {
	return func(srv any, ctx context.Context, dec func(any) error, interceptor grpc.UnaryServerInterceptor) (any, error) {
		request := new(Req)
		if err := dec(request); err != nil {
			return nil, err
		}
		if interceptor == nil {
			return fn(ctx, request)
		}
		info := &grpc.UnaryServerInfo{Server: srv}
		handler := func(ctx context.Context, req any) (any, error) {
			return fn(ctx, req.(*Req))
		}
		return interceptor(ctx, request, info, handler)
	}
}

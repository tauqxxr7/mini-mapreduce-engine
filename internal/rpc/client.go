package rpc

import (
	"context"

	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
)

type MasterServiceClient interface {
	SubmitJob(context.Context, *SubmitJobRequest, ...grpc.CallOption) (*SubmitJobResponse, error)
	RegisterWorker(context.Context, *RegisterWorkerRequest, ...grpc.CallOption) (*RegisterWorkerResponse, error)
	RequestTask(context.Context, *RequestTaskRequest, ...grpc.CallOption) (*RequestTaskResponse, error)
	SubmitTaskResult(context.Context, *SubmitTaskResultRequest, ...grpc.CallOption) (*SubmitTaskResultResponse, error)
	Heartbeat(context.Context, *HeartbeatRequest, ...grpc.CallOption) (*HeartbeatResponse, error)
	GetJobStatus(context.Context, *GetJobStatusRequest, ...grpc.CallOption) (*GetJobStatusResponse, error)
}

type masterServiceClient struct {
	conn *grpc.ClientConn
}

func NewMasterServiceClient(conn *grpc.ClientConn) MasterServiceClient {
	return &masterServiceClient{conn: conn}
}

func DialOptions() []grpc.DialOption {
	return []grpc.DialOption{
		grpc.WithTransportCredentials(insecure.NewCredentials()),
		grpc.WithDefaultCallOptions(grpc.ForceCodec(jsonCodec{})),
	}
}

func (c *masterServiceClient) SubmitJob(ctx context.Context, in *SubmitJobRequest, opts ...grpc.CallOption) (*SubmitJobResponse, error) {
	out := new(SubmitJobResponse)
	err := c.conn.Invoke(ctx, "/"+MasterServiceName+"/SubmitJob", in, out, opts...)
	return out, err
}

func (c *masterServiceClient) RegisterWorker(ctx context.Context, in *RegisterWorkerRequest, opts ...grpc.CallOption) (*RegisterWorkerResponse, error) {
	out := new(RegisterWorkerResponse)
	err := c.conn.Invoke(ctx, "/"+MasterServiceName+"/RegisterWorker", in, out, opts...)
	return out, err
}

func (c *masterServiceClient) RequestTask(ctx context.Context, in *RequestTaskRequest, opts ...grpc.CallOption) (*RequestTaskResponse, error) {
	out := new(RequestTaskResponse)
	err := c.conn.Invoke(ctx, "/"+MasterServiceName+"/RequestTask", in, out, opts...)
	return out, err
}

func (c *masterServiceClient) SubmitTaskResult(ctx context.Context, in *SubmitTaskResultRequest, opts ...grpc.CallOption) (*SubmitTaskResultResponse, error) {
	out := new(SubmitTaskResultResponse)
	err := c.conn.Invoke(ctx, "/"+MasterServiceName+"/SubmitTaskResult", in, out, opts...)
	return out, err
}

func (c *masterServiceClient) Heartbeat(ctx context.Context, in *HeartbeatRequest, opts ...grpc.CallOption) (*HeartbeatResponse, error) {
	out := new(HeartbeatResponse)
	err := c.conn.Invoke(ctx, "/"+MasterServiceName+"/Heartbeat", in, out, opts...)
	return out, err
}

func (c *masterServiceClient) GetJobStatus(ctx context.Context, in *GetJobStatusRequest, opts ...grpc.CallOption) (*GetJobStatusResponse, error) {
	out := new(GetJobStatusResponse)
	err := c.conn.Invoke(ctx, "/"+MasterServiceName+"/GetJobStatus", in, out, opts...)
	return out, err
}

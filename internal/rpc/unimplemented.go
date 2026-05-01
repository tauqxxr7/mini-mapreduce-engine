package rpc

import (
	"context"
	"errors"
)

type UnimplementedMasterServiceServer struct{}

func (UnimplementedMasterServiceServer) SubmitJob(context.Context, *SubmitJobRequest) (*SubmitJobResponse, error) {
	return nil, errors.New("SubmitJob is not implemented")
}

func (UnimplementedMasterServiceServer) RegisterWorker(context.Context, *RegisterWorkerRequest) (*RegisterWorkerResponse, error) {
	return nil, errors.New("RegisterWorker is not implemented")
}

func (UnimplementedMasterServiceServer) RequestTask(context.Context, *RequestTaskRequest) (*RequestTaskResponse, error) {
	return nil, errors.New("RequestTask is not implemented")
}

func (UnimplementedMasterServiceServer) SubmitTaskResult(context.Context, *SubmitTaskResultRequest) (*SubmitTaskResultResponse, error) {
	return nil, errors.New("SubmitTaskResult is not implemented")
}

func (UnimplementedMasterServiceServer) Heartbeat(context.Context, *HeartbeatRequest) (*HeartbeatResponse, error) {
	return nil, errors.New("Heartbeat is not implemented")
}

func (UnimplementedMasterServiceServer) GetJobStatus(context.Context, *GetJobStatusRequest) (*GetJobStatusResponse, error) {
	return nil, errors.New("GetJobStatus is not implemented")
}

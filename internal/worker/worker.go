package worker

import (
	"context"
	"fmt"
	"log/slog"
	"time"

	"github.com/tauqxxr7/mini-mapreduce-engine/internal/mapreduce"
	"github.com/tauqxxr7/mini-mapreduce-engine/internal/rpc"
	"github.com/tauqxxr7/mini-mapreduce-engine/internal/storage"
)

type Config struct {
	ID                string
	StorageRoot       string
	PollInterval      time.Duration
	HeartbeatInterval time.Duration
}

type Worker struct {
	id     string
	client rpc.MasterServiceClient
	engine *mapreduce.Engine
	logger *slog.Logger
	cfg    Config
}

func New(client rpc.MasterServiceClient, cfg Config, logger *slog.Logger) *Worker {
	if cfg.ID == "" {
		cfg.ID = fmt.Sprintf("worker-%d", time.Now().UnixNano())
	}
	if cfg.StorageRoot == "" {
		cfg.StorageRoot = "data"
	}
	if cfg.PollInterval == 0 {
		cfg.PollInterval = time.Second
	}
	if cfg.HeartbeatInterval == 0 {
		cfg.HeartbeatInterval = 2 * time.Second
	}
	store := storage.NewLocalStore(cfg.StorageRoot)
	return &Worker{
		id:     cfg.ID,
		client: client,
		engine: mapreduce.NewEngine(store, mapreduce.WordCountMap, mapreduce.WordCountReduce),
		logger: logger.With("worker_id", cfg.ID),
		cfg:    cfg,
	}
}

func (w *Worker) Run(ctx context.Context) error {
	registered, err := w.client.RegisterWorker(ctx, &rpc.RegisterWorkerRequest{WorkerID: w.id})
	if err != nil {
		return err
	}
	w.id = registered.WorkerID
	w.logger.Info("worker started")

	go w.heartbeatLoop(ctx)

	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		default:
		}
		task, err := w.client.RequestTask(ctx, &rpc.RequestTaskRequest{WorkerID: w.id})
		if err != nil {
			w.logger.Warn("request task failed", "error", err)
			sleep(ctx, w.cfg.PollInterval)
			continue
		}
		switch task.TaskType {
		case rpc.TaskTypeMap, rpc.TaskTypeReduce:
			w.execute(ctx, task)
		case rpc.TaskTypeExit:
			return nil
		default:
			sleep(ctx, w.cfg.PollInterval)
		}
	}
}

func (w *Worker) execute(ctx context.Context, task *rpc.RequestTaskResponse) {
	w.logger.Info("task started", "job_id", task.JobID, "task_id", task.TaskID, "type", task.TaskType)
	var outputs []string
	var taskErr error
	switch task.TaskType {
	case rpc.TaskTypeMap:
		outputs, taskErr = w.engine.RunMap(task.JobID, task.MapTaskID, task.InputPath, task.NumReducers)
	case rpc.TaskTypeReduce:
		output, err := w.engine.RunReduce(task.JobID, task.ReduceTaskID, task.IntermediatePaths, task.OutputPath)
		if err == nil {
			outputs = []string{output}
		}
		taskErr = err
	}
	result := &rpc.SubmitTaskResultRequest{
		WorkerID:    w.id,
		JobID:       task.JobID,
		TaskID:      task.TaskID,
		TaskType:    task.TaskType,
		OutputPaths: outputs,
	}
	if taskErr != nil {
		result.Error = taskErr.Error()
	}
	if _, err := w.client.SubmitTaskResult(ctx, result); err != nil {
		w.logger.Warn("submit task result failed", "task_id", task.TaskID, "error", err)
		return
	}
	w.logger.Info("task finished", "job_id", task.JobID, "task_id", task.TaskID, "error", result.Error)
}

func (w *Worker) heartbeatLoop(ctx context.Context) {
	ticker := time.NewTicker(w.cfg.HeartbeatInterval)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			_, err := w.client.Heartbeat(ctx, &rpc.HeartbeatRequest{WorkerID: w.id})
			if err != nil {
				w.logger.Warn("heartbeat failed", "error", err)
			}
		}
	}
}

func sleep(ctx context.Context, d time.Duration) {
	timer := time.NewTimer(d)
	defer timer.Stop()
	select {
	case <-ctx.Done():
	case <-timer.C:
	}
}

package master

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"path/filepath"
	"sync"
	"time"

	"github.com/tauqxxr7/mini-mapreduce-engine/internal/rpc"
	"github.com/tauqxxr7/mini-mapreduce-engine/internal/storage"
)

type taskState string

const (
	taskPending   taskState = "pending"
	taskRunning   taskState = "running"
	taskCompleted taskState = "completed"
	taskFailed    taskState = "failed"
)

type Config struct {
	StorageRoot     string
	TaskLease       time.Duration
	WorkerTimeout   time.Duration
	DefaultReducers int
	DefaultChunk    int64
}

type task struct {
	id                string
	typ               rpc.TaskType
	mapID             int
	reduceID          int
	inputPath         string
	intermediatePaths []string
	outputPath        string
	state             taskState
	assignedWorker    string
	leaseDeadline     time.Time
	attempts          int
	lastError         string
}

type job struct {
	id          string
	status      rpc.JobStatus
	inputPath   string
	outputPath  string
	numReducers int
	mapTasks    []*task
	reduceTasks []*task
	error       string
}

type workerState struct {
	id            string
	lastHeartbeat time.Time
}

type Coordinator struct {
	rpc.UnimplementedMasterServiceServer

	mu      sync.Mutex
	store   *storage.LocalStore
	logger  *slog.Logger
	cfg     Config
	jobs    map[string]*job
	workers map[string]*workerState
	nextJob int64
}

func NewCoordinator(cfg Config, logger *slog.Logger) *Coordinator {
	if cfg.StorageRoot == "" {
		cfg.StorageRoot = "data"
	}
	if cfg.TaskLease == 0 {
		cfg.TaskLease = 10 * time.Second
	}
	if cfg.WorkerTimeout == 0 {
		cfg.WorkerTimeout = 30 * time.Second
	}
	if cfg.DefaultReducers <= 0 {
		cfg.DefaultReducers = 3
	}
	if cfg.DefaultChunk <= 0 {
		cfg.DefaultChunk = 512 * 1024
	}
	return &Coordinator{
		store:   storage.NewLocalStore(cfg.StorageRoot),
		logger:  logger,
		cfg:     cfg,
		jobs:    make(map[string]*job),
		workers: make(map[string]*workerState),
	}
}

func (c *Coordinator) SubmitJob(_ context.Context, req *rpc.SubmitJobRequest) (*rpc.SubmitJobResponse, error) {
	if req.InputPath == "" {
		return nil, errors.New("input path is required")
	}
	outputPath := req.OutputPath
	if outputPath == "" {
		outputPath = filepath.Join(c.cfg.StorageRoot, "output")
	}
	reducers := int(req.NumReducers)
	if reducers <= 0 {
		reducers = c.cfg.DefaultReducers
	}
	chunkSize := int64(req.ChunkSizeBytes)
	if chunkSize <= 0 {
		chunkSize = c.cfg.DefaultChunk
	}

	c.mu.Lock()
	c.nextJob++
	jobID := fmt.Sprintf("job-%06d", c.nextJob)
	c.mu.Unlock()

	chunks, err := c.store.SplitInput(jobID, req.InputPath, chunkSize)
	if err != nil {
		return nil, err
	}
	mapTasks := make([]*task, 0, len(chunks))
	for i, path := range chunks {
		mapTasks = append(mapTasks, &task{
			id:        fmt.Sprintf("%s-map-%05d", jobID, i),
			typ:       rpc.TaskTypeMap,
			mapID:     i,
			inputPath: path,
			state:     taskPending,
		})
	}

	c.mu.Lock()
	c.jobs[jobID] = &job{
		id:          jobID,
		status:      rpc.JobStatusRunning,
		inputPath:   req.InputPath,
		outputPath:  outputPath,
		numReducers: reducers,
		mapTasks:    mapTasks,
	}
	c.mu.Unlock()
	c.logger.Info("job submitted", "job_id", jobID, "chunks", len(chunks), "reducers", reducers)
	return &rpc.SubmitJobResponse{JobID: jobID}, nil
}

func (c *Coordinator) RegisterWorker(_ context.Context, req *rpc.RegisterWorkerRequest) (*rpc.RegisterWorkerResponse, error) {
	workerID := req.WorkerID
	if workerID == "" {
		workerID = fmt.Sprintf("worker-%d", time.Now().UnixNano())
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	c.workers[workerID] = &workerState{id: workerID, lastHeartbeat: time.Now()}
	c.logger.Info("worker registered", "worker_id", workerID)
	return &rpc.RegisterWorkerResponse{WorkerID: workerID, Message: "registered"}, nil
}

func (c *Coordinator) Heartbeat(_ context.Context, req *rpc.HeartbeatRequest) (*rpc.HeartbeatResponse, error) {
	c.mu.Lock()
	defer c.mu.Unlock()
	if worker, ok := c.workers[req.WorkerID]; ok {
		worker.lastHeartbeat = time.Now()
		return &rpc.HeartbeatResponse{Acknowledged: true}, nil
	}
	return &rpc.HeartbeatResponse{Acknowledged: false}, nil
}

func (c *Coordinator) RequestTask(_ context.Context, req *rpc.RequestTaskRequest) (*rpc.RequestTaskResponse, error) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.reclaimExpiredLocked(time.Now())

	for _, j := range c.jobs {
		if j.status != rpc.JobStatusRunning {
			continue
		}
		if t := firstAssignable(j.mapTasks); t != nil {
			c.assignLocked(t, req.WorkerID)
			return taskResponse(j, t), nil
		}
		if allCompleted(j.mapTasks) && len(j.reduceTasks) == 0 {
			j.reduceTasks = c.buildReduceTasksLocked(j)
		}
		if allCompleted(j.mapTasks) {
			if t := firstAssignable(j.reduceTasks); t != nil {
				c.assignLocked(t, req.WorkerID)
				return taskResponse(j, t), nil
			}
			if len(j.reduceTasks) > 0 && allCompleted(j.reduceTasks) {
				j.status = rpc.JobStatusCompleted
				c.logger.Info("job completed", "job_id", j.id, "output_path", j.outputPath)
			}
		}
	}
	return &rpc.RequestTaskResponse{TaskType: rpc.TaskTypeWait, Message: "no task available"}, nil
}

func (c *Coordinator) SubmitTaskResult(_ context.Context, req *rpc.SubmitTaskResultRequest) (*rpc.SubmitTaskResultResponse, error) {
	c.mu.Lock()
	defer c.mu.Unlock()

	j, ok := c.jobs[req.JobID]
	if !ok {
		return &rpc.SubmitTaskResultResponse{Accepted: false, Message: "unknown job"}, nil
	}
	t := findTask(j, req.TaskID)
	if t == nil {
		return &rpc.SubmitTaskResultResponse{Accepted: false, Message: "unknown task"}, nil
	}
	if t.state == taskCompleted {
		return &rpc.SubmitTaskResultResponse{Accepted: false, Message: "task already completed"}, nil
	}
	if t.state != taskRunning || t.assignedWorker != req.WorkerID {
		return &rpc.SubmitTaskResultResponse{Accepted: false, Message: "task lease is no longer active"}, nil
	}
	if req.Error != "" {
		t.state = taskPending
		t.assignedWorker = ""
		t.lastError = req.Error
		t.attempts++
		c.logger.Warn("task failed", "job_id", req.JobID, "task_id", req.TaskID, "error", req.Error)
		return &rpc.SubmitTaskResultResponse{Accepted: true, Message: "task will be retried"}, nil
	}
	t.state = taskCompleted
	t.assignedWorker = ""
	t.outputPath = first(req.OutputPaths)
	if t.typ == rpc.TaskTypeMap {
		t.intermediatePaths = append([]string(nil), req.OutputPaths...)
	}
	c.logger.Info("task completed", "job_id", req.JobID, "task_id", req.TaskID, "worker_id", req.WorkerID)
	return &rpc.SubmitTaskResultResponse{Accepted: true, Message: "accepted"}, nil
}

func (c *Coordinator) GetJobStatus(_ context.Context, req *rpc.GetJobStatusRequest) (*rpc.GetJobStatusResponse, error) {
	c.mu.Lock()
	defer c.mu.Unlock()
	j, ok := c.jobs[req.JobID]
	if !ok {
		return nil, errors.New("unknown job")
	}
	return &rpc.GetJobStatusResponse{
		JobID:           j.id,
		Status:          j.status,
		MapCompleted:    countCompleted(j.mapTasks),
		MapTotal:        len(j.mapTasks),
		ReduceCompleted: countCompleted(j.reduceTasks),
		ReduceTotal:     len(j.reduceTasks),
		OutputPath:      j.outputPath,
		Error:           j.error,
	}, nil
}

func (c *Coordinator) reclaimExpiredLocked(now time.Time) {
	for _, j := range c.jobs {
		for _, t := range append(j.mapTasks, j.reduceTasks...) {
			if t.state != taskRunning {
				continue
			}
			workerTimedOut := false
			if worker, ok := c.workers[t.assignedWorker]; ok {
				workerTimedOut = now.Sub(worker.lastHeartbeat) > c.cfg.WorkerTimeout
			}
			if now.After(t.leaseDeadline) || workerTimedOut {
				c.logger.Warn("task reclaimed", "job_id", j.id, "task_id", t.id, "worker_id", t.assignedWorker, "worker_timeout", workerTimedOut)
				t.state = taskPending
				t.assignedWorker = ""
				t.attempts++
			}
		}
	}
}

func (c *Coordinator) assignLocked(t *task, workerID string) {
	t.state = taskRunning
	t.assignedWorker = workerID
	t.leaseDeadline = time.Now().Add(c.cfg.TaskLease)
	t.attempts++
}

func (c *Coordinator) buildReduceTasksLocked(j *job) []*task {
	tasks := make([]*task, 0, j.numReducers)
	for reduceID := 0; reduceID < j.numReducers; reduceID++ {
		var inputs []string
		for _, mt := range j.mapTasks {
			if reduceID < len(mt.intermediatePaths) {
				inputs = append(inputs, mt.intermediatePaths[reduceID])
			}
		}
		tasks = append(tasks, &task{
			id:                fmt.Sprintf("%s-reduce-%05d", j.id, reduceID),
			typ:               rpc.TaskTypeReduce,
			reduceID:          reduceID,
			intermediatePaths: inputs,
			state:             taskPending,
		})
	}
	return tasks
}

func firstAssignable(tasks []*task) *task {
	for _, t := range tasks {
		if t.state == taskPending || t.state == taskFailed {
			return t
		}
	}
	return nil
}

func taskResponse(j *job, t *task) *rpc.RequestTaskResponse {
	return &rpc.RequestTaskResponse{
		JobID:             j.id,
		TaskID:            t.id,
		TaskType:          t.typ,
		InputPath:         t.inputPath,
		MapTaskID:         t.mapID,
		ReduceTaskID:      t.reduceID,
		NumReducers:       j.numReducers,
		IntermediatePaths: append([]string(nil), t.intermediatePaths...),
		OutputPath:        j.outputPath,
	}
}

func allCompleted(tasks []*task) bool {
	if len(tasks) == 0 {
		return false
	}
	for _, t := range tasks {
		if t.state != taskCompleted {
			return false
		}
	}
	return true
}

func countCompleted(tasks []*task) int {
	var n int
	for _, t := range tasks {
		if t.state == taskCompleted {
			n++
		}
	}
	return n
}

func findTask(j *job, id string) *task {
	for _, t := range append(j.mapTasks, j.reduceTasks...) {
		if t.id == id {
			return t
		}
	}
	return nil
}

func first(paths []string) string {
	if len(paths) == 0 {
		return ""
	}
	return paths[0]
}

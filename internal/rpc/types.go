package rpc

type TaskType string

const (
	TaskTypeUnspecified TaskType = "UNSPECIFIED"
	TaskTypeMap         TaskType = "MAP"
	TaskTypeReduce      TaskType = "REDUCE"
	TaskTypeWait        TaskType = "WAIT"
	TaskTypeExit        TaskType = "EXIT"
)

type JobStatus string

const (
	JobStatusPending   JobStatus = "PENDING"
	JobStatusRunning   JobStatus = "RUNNING"
	JobStatusCompleted JobStatus = "COMPLETED"
	JobStatusFailed    JobStatus = "FAILED"
)

type SubmitJobRequest struct {
	InputPath      string `json:"input_path"`
	OutputPath     string `json:"output_path"`
	NumReducers    int32  `json:"num_reducers"`
	ChunkSizeBytes int32  `json:"chunk_size_bytes"`
}

type SubmitJobResponse struct {
	JobID string `json:"job_id"`
}

type RegisterWorkerRequest struct {
	WorkerID string `json:"worker_id"`
}

type RegisterWorkerResponse struct {
	WorkerID string `json:"worker_id"`
	Message  string `json:"message"`
}

type RequestTaskRequest struct {
	WorkerID string `json:"worker_id"`
}

type RequestTaskResponse struct {
	JobID             string   `json:"job_id"`
	TaskID            string   `json:"task_id"`
	TaskType          TaskType `json:"task_type"`
	InputPath         string   `json:"input_path"`
	MapTaskID         int      `json:"map_task_id"`
	ReduceTaskID      int      `json:"reduce_task_id"`
	NumReducers       int      `json:"num_reducers"`
	IntermediatePaths []string `json:"intermediate_paths"`
	OutputPath        string   `json:"output_path"`
	Message           string   `json:"message"`
}

type SubmitTaskResultRequest struct {
	WorkerID    string   `json:"worker_id"`
	JobID       string   `json:"job_id"`
	TaskID      string   `json:"task_id"`
	TaskType    TaskType `json:"task_type"`
	OutputPaths []string `json:"output_paths"`
	Error       string   `json:"error"`
}

type SubmitTaskResultResponse struct {
	Accepted bool   `json:"accepted"`
	Message  string `json:"message"`
}

type HeartbeatRequest struct {
	WorkerID string `json:"worker_id"`
}

type HeartbeatResponse struct {
	Acknowledged bool `json:"acknowledged"`
}

type GetJobStatusRequest struct {
	JobID string `json:"job_id"`
}

type GetJobStatusResponse struct {
	JobID           string    `json:"job_id"`
	Status          JobStatus `json:"status"`
	MapCompleted    int       `json:"map_completed"`
	MapTotal        int       `json:"map_total"`
	ReduceCompleted int       `json:"reduce_completed"`
	ReduceTotal     int       `json:"reduce_total"`
	OutputPath      string    `json:"output_path"`
	Error           string    `json:"error"`
}

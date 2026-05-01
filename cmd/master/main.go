package main

import (
	"context"
	"flag"
	"log"
	"net"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/tauqxxr7/mini-mapreduce-engine/internal/master"
	"github.com/tauqxxr7/mini-mapreduce-engine/internal/rpc"
	"github.com/tauqxxr7/mini-mapreduce-engine/internal/utils"
	"google.golang.org/grpc"
)

func main() {
	addr := flag.String("addr", ":50051", "address for the master gRPC server")
	storageRoot := flag.String("storage-root", "data", "directory for chunks and intermediate files")
	autoInput := flag.String("auto-input", "", "optional input file to submit when master starts")
	autoOutput := flag.String("auto-output", "data/output", "optional output dir for auto-submitted job")
	reducers := flag.Int("reducers", 3, "number of reduce tasks")
	chunkSize := flag.Int64("chunk-size", 512*1024, "input split size in bytes")
	lease := flag.Duration("task-lease", 10*time.Second, "task lease duration before retry")
	flag.Parse()

	logger := utils.NewLogger("master")
	coord := master.NewCoordinator(master.Config{
		StorageRoot:     *storageRoot,
		TaskLease:       *lease,
		DefaultReducers: *reducers,
		DefaultChunk:    *chunkSize,
	}, logger)

	listener, err := net.Listen("tcp", *addr)
	if err != nil {
		log.Fatal(err)
	}
	server := grpc.NewServer()
	rpc.RegisterMasterServiceServer(server, coord)

	if *autoInput != "" {
		go func() {
			resp, err := coord.SubmitJob(context.Background(), &rpc.SubmitJobRequest{
				InputPath:      *autoInput,
				OutputPath:     *autoOutput,
				NumReducers:    int32(*reducers),
				ChunkSizeBytes: int32(*chunkSize),
			})
			if err != nil {
				logger.Error("auto submit failed", "error", err)
				return
			}
			logger.Info("auto submitted job", "job_id", resp.JobID)
		}()
	}

	go func() {
		logger.Info("master listening", "addr", *addr)
		if err := server.Serve(listener); err != nil {
			logger.Error("master stopped", "error", err)
		}
	}()

	stop := make(chan os.Signal, 1)
	signal.Notify(stop, os.Interrupt, syscall.SIGTERM)
	<-stop
	logger.Info("shutting down")
	server.GracefulStop()
}

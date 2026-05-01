package main

import (
	"context"
	"flag"
	"fmt"
	"log"
	"time"

	"github.com/tauqxxr7/mini-mapreduce-engine/internal/rpc"
	"google.golang.org/grpc"
)

func main() {
	masterAddr := flag.String("master", "localhost:50051", "master gRPC address")
	input := flag.String("input", "", "input text file")
	output := flag.String("output", "data/output", "output directory")
	reducers := flag.Int("reducers", 3, "number of reducers")
	chunkSize := flag.Int("chunk-size", 512*1024, "input split size in bytes")
	wait := flag.Bool("wait", true, "wait until the job completes")
	flag.Parse()

	if *input == "" {
		log.Fatal("-input is required")
	}
	conn, err := grpc.Dial(*masterAddr, rpc.DialOptions()...)
	if err != nil {
		log.Fatal(err)
	}
	defer conn.Close()
	client := rpc.NewMasterServiceClient(conn)

	ctx := context.Background()
	submitted, err := client.SubmitJob(ctx, &rpc.SubmitJobRequest{
		InputPath:      *input,
		OutputPath:     *output,
		NumReducers:    int32(*reducers),
		ChunkSizeBytes: int32(*chunkSize),
	})
	if err != nil {
		log.Fatal(err)
	}
	fmt.Printf("submitted %s\n", submitted.JobID)
	if !*wait {
		return
	}
	for {
		time.Sleep(time.Second)
		status, err := client.GetJobStatus(ctx, &rpc.GetJobStatusRequest{JobID: submitted.JobID})
		if err != nil {
			log.Fatal(err)
		}
		fmt.Printf("status=%s map=%d/%d reduce=%d/%d\n", status.Status, status.MapCompleted, status.MapTotal, status.ReduceCompleted, status.ReduceTotal)
		if status.Status == rpc.JobStatusCompleted {
			fmt.Printf("output: %s\n", status.OutputPath)
			return
		}
		if status.Status == rpc.JobStatusFailed {
			log.Fatalf("job failed: %s", status.Error)
		}
	}
}

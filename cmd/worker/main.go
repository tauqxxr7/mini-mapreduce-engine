package main

import (
	"context"
	"flag"
	"log"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/tauqxxr7/mini-mapreduce-engine/internal/rpc"
	"github.com/tauqxxr7/mini-mapreduce-engine/internal/utils"
	workerpkg "github.com/tauqxxr7/mini-mapreduce-engine/internal/worker"
	"google.golang.org/grpc"
)

func main() {
	masterAddr := flag.String("master", "localhost:50051", "master gRPC address")
	id := flag.String("id", "", "worker id")
	storageRoot := flag.String("storage-root", "data", "shared storage directory")
	poll := flag.Duration("poll", time.Second, "task polling interval")
	flag.Parse()

	logger := utils.NewLogger("worker")
	conn, err := grpc.Dial(*masterAddr, rpc.DialOptions()...)
	if err != nil {
		log.Fatal(err)
	}
	defer conn.Close()

	ctx, cancel := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer cancel()

	w := workerpkg.New(rpc.NewMasterServiceClient(conn), workerpkg.Config{
		ID:           *id,
		StorageRoot:  *storageRoot,
		PollInterval: *poll,
	}, logger)
	if err := w.Run(ctx); err != nil && ctx.Err() == nil {
		log.Fatal(err)
	}
}

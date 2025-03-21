package main

import (
	"context"
	"flag"
	"log"
	pb "project/proto"
	"strconv"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
)


func main() {
	masterAddr := flag.String("master", "localhost:50051", "Master server address")
	workerID := flag.Int("id", 0, "Worker ID")

	flag.Parse()

	// Connect to the master server
	conn, err := grpc.NewClient(*masterAddr, grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		log.Fatalf("Failed to connect to master: %v", err)
	}
	defer conn.Close()

	client := pb.NewMasterServiceClient(conn)
	ctx := context.Background()

	// Ask if this worker should map
	mapResp, err := client.ShouldMap(ctx, &pb.WorkerRequest{})
	if err != nil {
		log.Fatalf("Error asking to map: %v", err)
	}

	if mapResp.ShouldMap {
		log.Printf("Worker assigned as mapper %d", mapResp.MapperIndex)

		// Perform mapping task
		index:= strconv.Itoa(int(mapResp.MapperIndex))
		callMapper(index, mapResp.Task , int(mapResp.NumReducers)) // Task hardcoded for simplicity

		// Notify master that mapping is complete
		_, err := client.MapDone(ctx, &pb.WorkerDone{
			Index:    mapResp.MapperIndex,
		})
		if err != nil {
			log.Fatalf("Error reporting map completion: %v", err)
		}
	} else {
		log.Printf("Worker %d not needed for mapping", *workerID)
	}

	// Ask if this worker should reduce (poll until we get an answer or termination)
	for {
		// Check if we should terminate
		termResp, err := client.ShouldTerminate(ctx, &pb.WorkerRequest{WorkerId: int32(*workerID)})
		if err != nil {
			log.Printf("Error checking termination: %v", err)
		} else if termResp.ShouldTerminate {
			log.Printf("Worker %d received termination signal", *workerID)
			break
		}

		// Ask if we should reduce
		reduceResp, err := client.ShouldReduce(ctx, &pb.WorkerRequest{WorkerId: int32(*workerID)})
		if err != nil {
			log.Printf("Error asking to reduce: %v", err)
			time.Sleep(1 * time.Second)
			continue
		}

		if reduceResp.ShouldReduce {
			log.Printf("Worker %d assigned as reducer %d", *workerID, reduceResp.ReducerIndex)

			// Perform reducing task
			println("HUHGEVGHWVEGQWVGHE")
			println(reduceResp.ReducerIndex)
			index := strconv.Itoa(int(reduceResp.ReducerIndex))
			callReducer(index, "wc") // Task hardcoded for simplicity

			// Notify master that reducing is complete
			_, err := client.ReduceDone(ctx, &pb.WorkerDone{
				WorkerId: int32(*workerID),
				Index:    reduceResp.ReducerIndex,
			})
			if err != nil {
				log.Fatalf("Error reporting reduce completion: %v", err)
			}
		} else {
			log.Printf("Worker %d not yet needed for reducing, waiting...", *workerID)
			time.Sleep(1 * time.Second)
		}
	}

	log.Printf("Worker %d terminating", *workerID)
}
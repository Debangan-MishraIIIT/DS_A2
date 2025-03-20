package main

import (
	"context"
	"fmt"
	"log"
	"os"
	"strconv"
	"time"

	pb "project/proto"

	"github.com/shirou/gopsutil/mem"
	"go.etcd.io/etcd/client/v3"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
)

func (s *DirectServer) SleepRequest(ctx context.Context, req *pb.ClientRequest) (*pb.SleepResponse, error) {
	log.Printf("Received request from client ID: %d, Sleep Duration: %d", req.ClientID, req.SleepDuration)
	time.Sleep(time.Duration(req.SleepDuration) * time.Second)
	return &pb.SleepResponse{
		Response: fmt.Sprintf("Done serving Client %d for %d seconds", req.ClientID, req.SleepDuration),
	}, nil
}

func getMemoryUsage() float32 {
	v, err := mem.VirtualMemory()
	if err != nil {
		log.Printf("Error getting memory usage: %v", err)
		return 0.0
	}
	return float32(v.UsedPercent)
}

func sendMemoryUsage(port int) {
	conn, err := grpc.NewClient(serverAddr, grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		log.Printf("Failed to connect to LB server: %v", err)
		return
	}
	defer conn.Close()

	client := pb.NewMemoryServiceClient(conn)
	listeningPort := basePort + port - 1
	clientPort := fmt.Sprintf("localhost:%d", listeningPort)

	for {
		usage := getMemoryUsage()
		_, err := client.SendMemoryUsage(context.Background(), &pb.MemoryRequest{
			ClientPort: clientPort,
			Usage:      usage,
		})
		if err != nil {
			log.Printf("Failed to send memory usage: %v", err)
		}
		time.Sleep(3 * time.Second) // Update every 3 sec
	}
}

func registerWithEtcd(cli *clientv3.Client, port int32) {
	key := fmt.Sprintf("/services/clients/%d", port)
	listeningPort := basePort + port - 1
	addr := fmt.Sprintf("localhost:%d", listeningPort)

	ctx := context.Background()
	leaseResp, err := cli.Grant(ctx, 3)
	if err != nil {
		log.Fatal(err)
	}

	if _, err := cli.Put(ctx, key, addr, clientv3.WithLease(leaseResp.ID)); err != nil {
		log.Fatal(err)
	}

	ch, err := cli.KeepAlive(ctx, leaseResp.ID)
	if err != nil {
		log.Fatal(err)
	}

	go func() {
		for range ch {
		}
		log.Println("Etcd keep-alive channel closed")
	}()
}

func handleInput() (int32, string) {
	if len(os.Args) < 2 {
		log.Fatal("Usage: go run direct_server.go <serverNumber>")
	}
	serverNumber, err := strconv.Atoi(os.Args[1])
	if err != nil || serverNumber <= 0 {
		log.Fatal("Invalid server number: must be a positive integer")
	}
	listeningPort := basePort + serverNumber - 1
	listeningAddr := ":" + strconv.Itoa(listeningPort)
	return int32(serverNumber), listeningAddr
}
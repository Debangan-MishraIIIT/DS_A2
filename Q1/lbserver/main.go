package main

import (
	"log"
	"net"
	"sync"
	"time"

	pb "project/proto"

	"go.etcd.io/etcd/client/v3"
	"google.golang.org/grpc"
)

const (
	basePort    = 50001
	etcdAddress = "localhost:2379"
	lbPort      = ":8080"
)

type Server struct {
	pb.UnimplementedLBCommunicationServer
	pb.UnimplementedMemoryServiceServer
	etcdClient  *clientv3.Client
	memoryData  sync.Map // Store memory usage of direct servers
	rrIndex     int      // Index for round-robin selection
	policy      string  // Load balancing policy
}

func main() {
	etcdClient, err := clientv3.New(clientv3.Config{
		Endpoints:   []string{etcdAddress},
		DialTimeout: 3 * time.Second,
	})
	if err != nil {
		log.Fatalf("Failed to connect to etcd: %v", err)
	}
	defer etcdClient.Close()

	policy := getUserPolicy()

	listener, err := net.Listen("tcp", lbPort)
	if err != nil {
		log.Fatalf("Failed to listen: %v", err)
	}

	grpcServer := grpc.NewServer()
	s := &Server{etcdClient: etcdClient, policy: policy, rrIndex: -1}

	pb.RegisterLBCommunicationServer(grpcServer, s)
	pb.RegisterMemoryServiceServer(grpcServer, s)

	log.Println("Load Balancer Server is running on port 8080...")

	if err := grpcServer.Serve(listener); err != nil {
		log.Fatalf("Failed to serve: %v", err)
	}
}
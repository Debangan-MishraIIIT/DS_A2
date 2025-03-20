package main

import (
	"log"
	"net"
	"time"
	pb "project/proto"
	"go.etcd.io/etcd/client/v3"
	"google.golang.org/grpc"
)

const (
	basePort    = 50001
	serverAddr  = "localhost:8080"
	etcdAddress = "localhost:2379"
)

type DirectServer struct {
	pb.UnimplementedDirectCommunicationServer
}

func main() {
	serverNumber, listeningAddr := handleInput()
	listener, err := net.Listen("tcp", listeningAddr)
	if err != nil {
		log.Fatalf("Failed to listen: %v", err)
	}

	etcdClient, err := clientv3.New(clientv3.Config{
		Endpoints:   []string{etcdAddress},
		DialTimeout: 3 * time.Second,
	})
	if err != nil {
		log.Fatal(err)
	}
	defer etcdClient.Close()

	go registerWithEtcd(etcdClient, serverNumber)
	go sendMemoryUsage(int(serverNumber))

	grpcServer := grpc.NewServer()
	pb.RegisterDirectCommunicationServer(grpcServer, &DirectServer{})
	log.Printf("Server %d is running on port %s...", int32(serverNumber), listeningAddr)

	if err := grpcServer.Serve(listener); err != nil {
		log.Fatalf("Failed to serve: %v", err)
	}
}

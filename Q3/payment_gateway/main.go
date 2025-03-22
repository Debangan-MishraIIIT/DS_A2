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
	gatewayAddr  = "localhost:8080"
	adminAddr   = "localhost:8081"
	etcdAddress = "localhost:2379"
)

type AdminServer struct {
	pb.UnimplementedAdminServiceServer
	etcdClient *clientv3.Client
}

type GatewayServer struct {
	pb.UnimplementedPaymentGatewayServer
}


func main() {
	// Connect to etcd
	etcdClient, err := clientv3.New(clientv3.Config{
		Endpoints:   []string{etcdAddress},
		DialTimeout: time.Second,
	})
	if err != nil {
		log.Fatalf("Failed to connect to etcd: %v", err)
	}
	defer etcdClient.Close()

	// Admin Server Setup
	listenerAdmin, err := net.Listen("tcp", adminAddr)
	if err != nil {
		log.Fatalf("Failed to listen: %v", err)
	}

	adminServer := grpc.NewServer()
	pb.RegisterAdminServiceServer(adminServer, &AdminServer{etcdClient: etcdClient})

	go func() { // Run admin server in a goroutine
		log.Printf("Admin Server running on %v...\n", adminAddr)
		if err := adminServer.Serve(listenerAdmin); err != nil {
			log.Fatalf("Failed to serve: %v", err)
		}
	}()

	// Gateway Server Setup
	listenerGateway, err := net.Listen("tcp", gatewayAddr)
	if err != nil {
		log.Fatalf("Failed to listen: %v", err)
	}

	gatewayServer := grpc.NewServer()
	pb.RegisterPaymentGatewayServer(gatewayServer, &GatewayServer{})

	log.Printf("Gateway Server running on %v...\n", gatewayAddr)
	if err := gatewayServer.Serve(listenerGateway); err != nil {
		log.Fatalf("Failed to serve: %v", err)
	}

	log.Printf("Gateway Server running on %v...\n", gatewayAddr)
	if err := gatewayServer.Serve(listenerGateway); err != nil {
		log.Fatalf("Failed to serve: %v", err)
	}
}

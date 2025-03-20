package main

import (
	"context"
	"fmt"
	"log"
	"time"
	pb "project/proto"
	"go.etcd.io/etcd/client/v3"
)


func (s *Server) SendMemoryUsage(ctx context.Context, req *pb.MemoryRequest) (*pb.MemoryResponse, error) {
	s.memoryData.Store(req.ClientPort, req.Usage)
	return &pb.MemoryResponse{}, nil
}

func (s *Server) fetchAvailableServers() []string {
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()

	resp, err := s.etcdClient.Get(ctx, "/services/clients/", clientv3.WithPrefix())
	if err != nil {
		log.Printf("Error fetching available servers: %v", err)
		return nil
	}

	var servers []string
	for _, kv := range resp.Kvs {
		serverAddr := string(kv.Value)
		servers = append(servers, serverAddr)
	}
	return servers
}

func (s *Server) chooseBestServer() string {
	servers := s.fetchAvailableServers()
	if len(servers) == 0 {
		log.Println("No available servers found")
		return ""
	}
	return strategySelectServer(s, servers, s.policy)
}

func strategySelectServer(s *Server, servers []string, policy string) string {
	switch policy {
	case "fc":
		return firstAvailableServer(servers)
	case "rr":
		return roundRobinServer(s, servers)
	case "lb":
		return leastMemoryUsageServer(s, servers)
	default:
		log.Println("Invalid policy, defaulting to lb")
		return leastMemoryUsageServer(s, servers)
	}
}

func firstAvailableServer(servers []string) string {
	if len(servers) > 0 {
		return servers[0]
	}
	return ""
}

func roundRobinServer(s *Server, servers []string) string {
	if len(servers) == 0 {
		return ""
	}
	s.rrIndex = (s.rrIndex + 1) % len(servers)
	return servers[s.rrIndex]
}

func leastMemoryUsageServer(s *Server, servers []string) string {
	var bestServer string
	minUsage := float32(100.0)

	for _, server := range servers {
		if usage, ok := s.memoryData.Load(server); ok {
			if usageFloat, valid := usage.(float32); valid && usageFloat < minUsage {
				minUsage = usageFloat
				bestServer = server
			}
		} else {
			bestServer = server
		}
	}
	return bestServer
}

func (s *Server) GreetLB(ctx context.Context, req *pb.ClientInfo) (*pb.ServerInfo, error) {
	log.Printf("Received request from client ID: %d", req.ClientID)
	selectedServer := s.chooseBestServer()
	if selectedServer == "" {
		return nil, fmt.Errorf("no available server")
	}
	return &pb.ServerInfo{ServerID: 1, ServerAddr: selectedServer}, nil
}

func getUserPolicy() string {
	var policy string
	fmt.Print("Enter load balancing policy (fc/rr/lb): ")
	fmt.Scanln(&policy)
	if policy != "fc" && policy != "rr" && policy != "lb" {
		log.Println("Invalid policy, defaulting to lb")
		return "lb"
	}
	return policy
}
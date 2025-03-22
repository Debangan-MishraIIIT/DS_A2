package main

import (
	"context"
	"encoding/csv"
	"fmt"
	"log"
	"os"
	"time"
	"regexp"
	pb "project/proto"
	"go.etcd.io/etcd/client/v3"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
)

// Authenticate user by checking ../account_details/<bank_number>.csv
func authenticateUser(bankNumber, username, password string) bool {
	filePath := fmt.Sprintf("../account_details/%s.csv", bankNumber)
	file, err := os.Open(filePath)
	if err != nil {
		log.Printf("Failed to open account details for bank %s: %v", bankNumber, err)
		return false
	}
	defer file.Close()

	reader := csv.NewReader(file)
	records, err := reader.ReadAll()
	if err != nil {
		log.Printf("Error reading account details for bank %s: %v", bankNumber, err)
		return false
	}

	for _, record := range records {
		if len(record) < 3 {
			continue
		}
		if record[0] == username && record[1] == password {
			return true
		}
	}

	log.Printf("Authentication failed for user %s at bank %s", username, bankNumber)
	return false
}

// ProcessTransaction function
func (s *GatewayServer) ProcessTransaction(ctx context.Context, req *pb.TransactionRequest) (*pb.TransactionResponse, error) {

	// Authenticate user
	if !authenticateUser(req.BankNumber, req.Username, req.Password) {
		return &pb.TransactionResponse{
			Success: false,
			Message: "Authentication failed",
			Balance: 0,
		}, nil
	}

	// Get available bank servers from etcd
	etcdClient, err := clientv3.New(clientv3.Config{
		Endpoints:   []string{"localhost:2379"},
		DialTimeout: time.Second,
	})
	if err != nil {
		log.Printf("Failed to connect to etcd: %v", err)
		return nil, err
	}
	defer etcdClient.Close()

	bankServers := fetchAvailableBanks(etcdClient)
	bankAddr, exists := bankServers[req.BankNumber]
	if !exists {
		return &pb.TransactionResponse{
			Success: false,
			Message: "Bank server not found",
			Balance: 0,
		}, nil
	}

	// Forward request to the bank server
	conn, err := grpc.Dial(bankAddr, grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		log.Printf("Failed to connect to bank server %s: %v", bankAddr, err)
		return nil, err
	}
	defer conn.Close()

	bankClient := pb.NewBankServiceClient(conn)
	response, err := bankClient.HandleTransaction(ctx, req)
	if err != nil {
		log.Printf("Error processing transaction with bank %s: %v", bankAddr, err)
		return nil, err
	}

	// Return response from bank to client
	return response, nil
}


// Fetch available banks from etcd
func fetchAvailableBanks(etcdClient *clientv3.Client) map[string]string {
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()

	resp, err := etcdClient.Get(ctx, "/services/clients/", clientv3.WithPrefix())
	if err != nil {
		log.Printf("Error fetching available banks: %v", err)
		return nil
	}

	servers := make(map[string]string)
	for _, kv := range resp.Kvs {
		path := string(kv.Key)
		re := regexp.MustCompile(`/services/clients/(\d+)$`)
		match := re.FindStringSubmatch(path)
		if len(match) == 2 {
			servers[match[1]] = string(kv.Value)
		}
	}

	return servers
}
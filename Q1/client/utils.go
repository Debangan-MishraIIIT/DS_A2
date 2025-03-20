package main

import (
	"context"
	"log"
	pb "project/proto"
	"time"
	"strconv"
	"os"
)

func callGreetLB(client pb.LBCommunicationClient, clientInfo *pb.ClientInfo) (int32, string) {
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()

	res, err := client.GreetLB(ctx, clientInfo)
	if err != nil {
		log.Fatalf("Could not greet: %v", err)
	}
	return res.ServerID, res.ServerAddr
}

func callDirectCommunication(client pb.DirectCommunicationClient, clientRequest *pb.ClientRequest) {
	ctx := context.Background()
	res, err := client.SleepRequest(ctx, clientRequest)
	if err != nil {
		log.Fatalf("Failed to send sleep request: %v", err)
	}
	log.Printf("Received Response: %s", res.Response)
}

func handleInput() (int, int) {
	if len(os.Args) < 3 {
		log.Fatal("Usage: go run client.go <serverNumber>")
	}
	clientID, err := strconv.Atoi(os.Args[1])
	if err != nil || clientID <= 0 {
		log.Fatal("Invalid server number: must be a positive integer")
	}
	queryVal, err := strconv.Atoi(os.Args[2])
	if err != nil || queryVal <= 1 {
		log.Fatal("Invalid query number: must be greater than 1")
	}
	return clientID, queryVal
}
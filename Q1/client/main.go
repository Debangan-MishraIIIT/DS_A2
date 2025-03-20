package main

import (
	"log"
	pb "project/proto"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
)

const (
	lbServerAddress = "localhost:8080"
)

func main() {
	clientNum, queryVal := handleInput()
	clientInfo := &pb.ClientInfo{
		ClientID: int32(clientNum),
	}

	conn, err := grpc.NewClient(lbServerAddress, grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		log.Fatalf("Did not connect: %v", err)
	}
	defer conn.Close()

	client := pb.NewLBCommunicationClient(conn)
	serverID, serverAddr := callGreetLB(client, clientInfo)

	log.Printf("Communicating with Server %d at %s", serverID, serverAddr)
	serverConn, err := grpc.NewClient(serverAddr, grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		log.Fatalf("Failed to connect to Server %d, %v", serverID, err)
	}
	defer serverConn.Close()

	directClient := pb.NewDirectCommunicationClient(serverConn)
	sleepRequest := pb.ClientRequest{
		ClientID: int32(clientNum),
		SleepDuration: int32(queryVal),
	}

	callDirectCommunication(directClient, &sleepRequest)
}

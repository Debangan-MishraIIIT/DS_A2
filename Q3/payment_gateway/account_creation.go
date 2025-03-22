package main

import (
	"context"
	"encoding/csv"
	"fmt"
	"log"
	"os"
	pb "project/proto"
)


// Account Creation Utilities

func (s *AdminServer) MakeAccount(ctx context.Context, req *pb.CreationRequest) (*pb.CreationResponse, error) {
	log.Printf("Received request: Username=%s, BankNumber=%s, Balance=%d", req.GetUsername(), req.GetBankNumber(), req.GetBalance())

	aliveBanks := fetchAvailableBanks(s.etcdClient)
	if _, exists := aliveBanks[req.GetBankNumber()]; !exists {
		log.Printf("Bank %s is not alive", req.GetBankNumber())
		return &pb.CreationResponse{
			Response: fmt.Sprintf("Error: Bank %s is not alive", req.GetBankNumber()),
		}, nil
	}

	dirName := "../account_details"
	err := os.MkdirAll(dirName, 0755)
	if err != nil {
		log.Fatalf("Error creating directory: %v", err)
	}

	fileName := fmt.Sprintf("../account_details/%s.csv", req.GetBankNumber())
	if accountExists(fileName, req.GetUsername()) {
		log.Printf("Account already exists for username: %s", req.GetUsername())
		return &pb.CreationResponse{
			Response: "Error: Account with this username already exists",
		}, nil
	}

	file, err := os.OpenFile(fileName, os.O_CREATE|os.O_APPEND|os.O_WRONLY, 0644)
	if err != nil {
		log.Printf("Error creating/opening CSV file: %v", err)
		return &pb.CreationResponse{
			Response: "Error: Failed to create/open account file",
		}, nil
	}
	defer file.Close()

	writer := csv.NewWriter(file)
	defer writer.Flush()

	record := []string{req.GetUsername(), req.GetPassword(), fmt.Sprintf("%d", req.GetBalance())}
	if err := writer.Write(record); err != nil {
		log.Printf("Error writing to CSV file: %v", err)
		return &pb.CreationResponse{
			Response: "Error: Failed to write account details",
		}, nil
	}

	log.Printf("Account created successfully for %s", req.GetUsername())
	return &pb.CreationResponse{
		Response: fmt.Sprintf("Account created successfully for %s", req.GetUsername()),
	}, nil
}

func accountExists(fileName, username string) bool {
	file, err := os.Open(fileName)
	if err != nil {
		if os.IsNotExist(err) {
			return false
		}
		log.Printf("Error opening CSV file: %v", err)
		return false
	}
	defer file.Close()

	reader := csv.NewReader(file)
	records, err := reader.ReadAll()
	if err != nil {
		log.Printf("Error reading CSV file: %v", err)
		return false
	}

	for _, record := range records {
		if len(record) > 0 && record[0] == username {
			return true
		}
	}
	return false
}

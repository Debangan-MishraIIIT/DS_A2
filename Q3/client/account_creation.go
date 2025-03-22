package main

import (
	"bufio"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"log"
	pb "project/proto"
	"strconv"
	"strings"
	"time"
)

func HashPassword(password string) (string, error) {
	hash := sha256.Sum256([]byte(password))
	return hex.EncodeToString(hash[:]), nil
}

func handleAccountCreation(username string, scanner *bufio.Scanner, adminClient pb.AdminServiceClient) {
	fmt.Print("Enter password: ")
	scanner.Scan()
	password := strings.TrimSpace(scanner.Text())
	hashed, err := HashPassword(password)
	if err != nil {
		log.Fatalf("Error in Password Hashing: %v", err)
		return
	}

	fmt.Print("Enter bank number: ")
	scanner.Scan()
	bankNumber := strings.TrimSpace(scanner.Text())

	fmt.Print("Enter initial balance: ")
	scanner.Scan()
	balance, err := strconv.Atoi(strings.TrimSpace(scanner.Text()))
	if err != nil {
		log.Print("Invalid balance, please enter a number.\n\n")
		return
	}

	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	req := &pb.CreationRequest{
		Username:   username,
		Password:   hashed,
		BankNumber: bankNumber,
		Balance:    int32(balance),
	}

	res, err := adminClient.MakeAccount(ctx, req)
	cancel()
	if err != nil {
		log.Printf("Error creating account: %v\n\n", err)
		return
	}

	log.Printf("Response from server: %s\n\n", res.GetResponse())
}

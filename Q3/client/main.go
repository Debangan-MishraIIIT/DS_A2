package main

import (
	"bufio"
	"context"
	"flag"
	"fmt"
	"github.com/google/uuid"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
	"log"
	"os"
	pb "project/proto"
	"strconv"
	"strings"
	"sync"
	"time"
)

const (
	adminAddr   = "localhost:8081"
	gatewayAddr = "localhost:8080"
)

type Request struct {
	Command string
	Args    map[string]string
}

var (
	failedState bool
	queue       []Request
	mu          sync.Mutex
)

func main() {
	username := flag.String("username", "temp", "Specify the username")
	flag.Parse()

	adminConn, err := grpc.Dial(adminAddr, grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		log.Fatalf("Failed to connect: %v", err)
	}
	defer adminConn.Close()
	adminClient := pb.NewAdminServiceClient(adminConn)

	gatewayConn, err := grpc.Dial(gatewayAddr, grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		log.Fatalf("Failed to connect: %v", err)
	}
	defer gatewayConn.Close()
	gatewayClient := pb.NewPaymentGatewayClient(gatewayConn)

	scanner := bufio.NewScanner(os.Stdin)
	for {
		fmt.Print("Enter command: ")
		scanner.Scan()
		command := strings.TrimSpace(scanner.Text())

		if command == "fail" {
			mu.Lock()
			failedState = true
			mu.Unlock()
			fmt.Println("System is now in failure mode. Transactions will be queued.")
			continue
		}
		if command == "recover" {
			mu.Lock()
			failedState = false
			processQueue(gatewayClient)
			mu.Unlock()
			continue
		}

		if command == "make account" {
			if failedState {
				fmt.Println("Account creation is disabled in failure mode.")
				continue
			} else {
				handleAccountCreation(*username, scanner, adminClient)
			}
		}

		args := collectArgs(command, *username, scanner)
		request := Request{Command: command, Args: args}

		mu.Lock()
		if failedState {
			queue = append(queue, request)
			fmt.Println("Request queued.")
		} else {
			executeRequest(request, gatewayClient)
		}
		mu.Unlock()
	}
}

func collectArgs(command string, username string, scanner *bufio.Scanner) map[string]string {
	args := make(map[string]string)
	if command == "make transaction" || command == "make payment" {
		fmt.Print("Enter bank number: ")
		scanner.Scan()
		args["bankNumber"] = strings.TrimSpace(scanner.Text())

		if command == "make transaction" {
			fmt.Print("Enter type of transaction (deposit/withdraw/view): ")
			scanner.Scan()
			args["transactionType"] = strings.TrimSpace(scanner.Text())

			if args["transactionType"] != "view" {
				fmt.Print("Enter amount: ")
				scanner.Scan()
				args["amount"] = strings.TrimSpace(scanner.Text())
			}
		}
		if command == "make payment" {
			fmt.Print("Enter recipient bank number: ")
			scanner.Scan()
			args["targetBankNumber"] = strings.TrimSpace(scanner.Text())

			fmt.Print("Enter recipient username: ")
			scanner.Scan()
			args["targetUsername"] = strings.TrimSpace(scanner.Text())

			fmt.Print("Enter amount to transfer: ")
			scanner.Scan()
			args["amount"] = strings.TrimSpace(scanner.Text())
		}

		fmt.Print("Enter password: ")
		scanner.Scan()
		args["password"] = strings.TrimSpace(scanner.Text())
		args["username"] = username
	}
	return args
}

func executeRequest(req Request, gatewayClient pb.PaymentGatewayClient) {
	switch req.Command {
	case "make transaction":
		handleTransaction(req.Args, gatewayClient)
	case "make payment":
		handlePayment(req.Args, gatewayClient)
	default:
		fmt.Println("Unknown command.")
	}
}

func processQueue(gatewayClient pb.PaymentGatewayClient) {
	fmt.Println("Processing queued requests...")
	for _, req := range queue {
		executeRequest(req, gatewayClient)
	}
	queue = nil
	fmt.Println("All queued requests processed.")
}

func handleTransaction(args map[string]string, gatewayClient pb.PaymentGatewayClient) {
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()

	amount, _ := strconv.Atoi(args["amount"])
	hashed, _ := HashPassword(args["password"])

	req := &pb.TransactionRequest{
		Username:        args["username"],
		Uuid:            uuid.New().String(),
		TransactionType: args["transactionType"],
		BankNumber:      args["bankNumber"],
		Amount:          int32(amount),
		Password:        hashed,
	}

	res, err := gatewayClient.ProcessTransaction(ctx, req)
	if err != nil {
		log.Printf("Transaction failed: %v", err)
		return
	}
	log.Printf("Transaction Successful! Message: %s, Balance remaining: %d", res.Message, res.Balance)
}

func handlePayment(args map[string]string, gatewayClient pb.PaymentGatewayClient) {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	amount, _ := strconv.Atoi(args["amount"])
	hashed, _ := HashPassword(args["password"])

	req := &pb.PaymentRequest{
		SourceUsername:   args["username"],
		SourceBankNumber: args["bankNumber"],
		TargetUsername:   args["targetUsername"],
		TargetBankNumber: args["targetBankNumber"],
		Amount:           int32(amount),
		Password:         hashed,
		Uuid:             uuid.New().String(),
	}

	res, err := gatewayClient.ProcessPayment(ctx, req)
	if err != nil {
		log.Printf("Payment failed: %v", err)
		return
	}
	log.Printf("Payment Successful! Message: %s, Balance remaining: %d", res.Message, res.SourceBalance)
}

package main

import (
	"context"
	"encoding/csv"
	"fmt"
	"log"
	"net"
	"os"
	"strconv"
	"sync"
	"time"

	"go.etcd.io/etcd/client/v3"
	"google.golang.org/grpc"
	pb "project/proto"
)

const (
	basePort    = 50001
	etcdAddress = "localhost:2379"
)

// Mutex for thread-safe file access
var (
	mu        sync.Mutex
	processed = make(map[string]bool)
)

type BankServer struct {
	pb.UnimplementedBankServiceServer
}

// HandleTransaction processes deposit, withdrawal, or balance check
func (s *BankServer) HandleTransaction(ctx context.Context, req *pb.TransactionRequest) (*pb.TransactionResponse, error) {
	filePath := fmt.Sprintf("../account_details/%s.csv", req.BankNumber)

	mu.Lock()
	defer mu.Unlock()

	// UUID for Idempotency
	if processed[req.Uuid] {
		return &pb.TransactionResponse{Success: false, Message: "Duplicate request detected"}, nil
	}
	processed[req.Uuid] = true


	// Authenticate user before processing transaction
	authenticated, records, index := authenticateUser(filePath, req.Username, req.Password)
	if !authenticated {
		return &pb.TransactionResponse{Success: false, Message: "Authentication failed"}, nil
	}

	// Fetch current balance
	currentBalanceInt, _ := strconv.Atoi(records[index][2])
	currentBalance := int32(currentBalanceInt)

	switch req.TransactionType {
	case "deposit":
		currentBalance += req.Amount
		records[index][2] = strconv.Itoa(int(currentBalance))

	case "withdraw":
		if currentBalance < req.Amount {
			return &pb.TransactionResponse{Success: false, Message: "Insufficient balance", Balance: currentBalance}, nil
		}
		currentBalance -= req.Amount
		records[index][2] = strconv.Itoa(int(currentBalance))

	case "view":

	default:
		return &pb.TransactionResponse{Success: false, Message: "Invalid transaction type"}, nil
	}

	// Save updated balance if necessary
	if req.TransactionType == "deposit" || req.TransactionType == "withdraw" {
		if err := writeCSV(filePath, records); err != nil {
			return &pb.TransactionResponse{Success: false, Message: "Failed to update balance"}, err
		}
	}

	return &pb.TransactionResponse{Success: true, Message: "Transaction successful", Balance: currentBalance}, nil
}

// Authenticate user by checking credentials in CSV file
func authenticateUser(filePath, username, password string) (bool, [][]string, int) {
	records, err := readCSV(filePath)
	if err != nil {
		log.Println("Error reading CSV:", err)
		return false, nil, -1
	}

	for i, record := range records {
		if len(record) < 3 {
			continue
		}
		if record[0] == username && record[1] == password {
			return true, records, i
		}
	}
	return false, nil, -1
}

// Read CSV File
func readCSV(filePath string) ([][]string, error) {
	file, err := os.Open(filePath)
	if err != nil {
		return nil, err
	}
	defer file.Close()

	reader := csv.NewReader(file)
	return reader.ReadAll()
}

// Write CSV File
func writeCSV(filePath string, records [][]string) error {
	file, err := os.Create(filePath)
	if err != nil {
		return err
	}
	defer file.Close()

	writer := csv.NewWriter(file)
	defer writer.Flush()

	return writer.WriteAll(records)
}

// Register the bank server with etcd
func registerWithEtcd(cli *clientv3.Client, port int32) {
	key := fmt.Sprintf("/services/clients/%d", port)
	listeningPort := basePort + port - 1
	addr := fmt.Sprintf("localhost:%d", listeningPort)

	ctx := context.Background()
	leaseResp, err := cli.Grant(ctx, 3)
	if err != nil {
		log.Fatal(err)
	}

	if _, err := cli.Put(ctx, key, addr, clientv3.WithLease(leaseResp.ID)); err != nil {
		log.Fatal(err)
	}

	ch, err := cli.KeepAlive(ctx, leaseResp.ID)
	if err != nil {
		log.Fatal(err)
	}

	go func() {
		for range ch {
		}
		log.Println("Etcd keep-alive channel closed")
	}()
}

// Parse input arguments to determine server number & address
func handleInput() (int32, string) {
	if len(os.Args) < 2 {
		log.Fatal("Usage: go run bank_server.go <serverNumber>")
	}
	serverNumber, err := strconv.Atoi(os.Args[1])
	if err != nil || serverNumber <= 0 {
		log.Fatal("Invalid server number: must be a positive integer")
	}
	listeningPort := basePort + serverNumber - 1
	listeningAddr := ":" + strconv.Itoa(listeningPort)
	return int32(serverNumber), listeningAddr
}

type PreparedTx struct {
    TransactionId string
    Username      string
    Amount        int32
    Operation     string  // "debit" or "credit"
    Timestamp     time.Time
}

var (
    preparedTxs = make(map[string]*PreparedTx)
    preparedMu  sync.Mutex

    // Timeout cleanup
    txTimeout   = 30 * time.Second // Make this configurable
)

func main() {
	// Handle server input (server number and address)
	serverNumber, listeningAddr := handleInput()

	// Start gRPC Server
	listener, err := net.Listen("tcp", listeningAddr)
	if err != nil {
		log.Fatalf("Failed to listen: %v", err)
	}

	grpcServer := grpc.NewServer()
	pb.RegisterBankServiceServer(grpcServer, &BankServer{})

	// Register server with etcd
	etcdClient, err := clientv3.New(clientv3.Config{
		Endpoints:   []string{etcdAddress},
		DialTimeout: time.Second,
	})
	if err != nil {
		log.Fatal(err)
	}
	defer etcdClient.Close()

	go registerWithEtcd(etcdClient, serverNumber)
	go cleanupExpiredTransactions()

	log.Printf("Bank Server %d running on %s...\n", serverNumber, listeningAddr)
	if err := grpcServer.Serve(listener); err != nil {
		log.Fatalf("Failed to serve: %v", err)
	}
}


func cleanupExpiredTransactions() {
    ticker := time.NewTicker(5 * time.Second)
    defer ticker.Stop()

    for range ticker.C {
        now := time.Now()
        preparedMu.Lock()

        for txID, tx := range preparedTxs {
            if now.Sub(tx.Timestamp) > txTimeout {
                log.Printf("Transaction %s timed out, cleaning up", txID)
                delete(preparedTxs, txID)
            }
        }

        preparedMu.Unlock()
    }
}

// Prepare RPC handler for 2PC
func (s *BankServer) Prepare(ctx context.Context, req *pb.PrepareRequest) (*pb.PrepareResponse, error) {
    txID := req.TransactionId

    // Check if this transaction is already prepared
    preparedMu.Lock()
    if _, exists := preparedTxs[txID]; exists {
        preparedMu.Unlock()
        return &pb.PrepareResponse{
            Ready:   true,
            Message: "Already prepared",
        }, nil
    }
    preparedMu.Unlock()

    filePath := fmt.Sprintf("../account_details/%s.csv", req.BankNumber)

    mu.Lock()
    defer mu.Unlock()

    var authenticated bool
    var records [][]string
    var index int

    // For debit operations, we need to authenticate and check balance
    if req.Operation == "debit" {
        authenticated, records, index = authenticateUser(filePath, req.Username, req.Password)
        if !authenticated {
            return &pb.PrepareResponse{
                Ready:   false,
                Message: "Authentication failed",
            }, nil
        }

        // Check if user has enough balance
        currentBalanceInt, _ := strconv.Atoi(records[index][2])
        currentBalance := int32(currentBalanceInt)

        if currentBalance < req.Amount {
            return &pb.PrepareResponse{
                Ready:   false,
                Message: "Insufficient balance",
            }, nil
        }
    } else if req.Operation == "credit" {
        // For credit operations, just check if the user exists
        records, err := readCSV(filePath)
        if err != nil {
            return &pb.PrepareResponse{
                Ready:   false,
                Message: fmt.Sprintf("Error reading account details: %v", err),
            }, nil
        }

        userExists := false
        for _, record := range records {
            if len(record) >= 3 && record[0] == req.Username {
                userExists = true
                break
            }
        }

        if !userExists {
            return &pb.PrepareResponse{
                Ready:   false,
                Message: "Target user not found",
            }, nil
        }
    } else {
        return &pb.PrepareResponse{
            Ready:   false,
            Message: "Invalid operation",
        }, nil
    }

    // Store prepared transaction
    preparedMu.Lock()
    preparedTxs[txID] = &PreparedTx{
        TransactionId: txID,
        Username:      req.Username,
        Amount:        req.Amount,
        Operation:     req.Operation,
        Timestamp:     time.Now(),
    }
    preparedMu.Unlock()

    return &pb.PrepareResponse{
        Ready:   true,
        Message: "Prepared successfully",
    }, nil
}

// Commit RPC handler for 2PC
func (s *BankServer) Commit(ctx context.Context, req *pb.CommitRequest) (*pb.CommitResponse, error) {
    txID := req.TransactionId

    // Check if this transaction is prepared
    preparedMu.Lock()
    tx, exists := preparedTxs[txID]
    if !exists {
        preparedMu.Unlock()
        return &pb.CommitResponse{
            Success: false,
            Message: "Transaction not prepared",
        }, nil
    }

    // Remove from prepared transactions
    delete(preparedTxs, txID)
    preparedMu.Unlock()

    // Process the transaction
    filePath := fmt.Sprintf("../account_details/%s.csv", getBankNumberFromContext())

    mu.Lock()
    defer mu.Unlock()

    // Find user in records
    records, err := readCSV(filePath)
    if err != nil {
        return &pb.CommitResponse{
            Success: false,
            Message: fmt.Sprintf("Error reading account details: %v", err),
        }, nil
    }

    userIndex := -1
    for i, record := range records {
        if len(record) >= 3 && record[0] == tx.Username {
            userIndex = i
            break
        }
    }

    if userIndex == -1 {
        return &pb.CommitResponse{
            Success: false,
            Message: "User not found",
        }, nil
    }

    // Update balance
    currentBalanceInt, _ := strconv.Atoi(records[userIndex][2])
    currentBalance := int32(currentBalanceInt)

    if tx.Operation == "debit" {
        currentBalance -= tx.Amount
    } else if tx.Operation == "credit" {
        currentBalance += tx.Amount
    }

    records[userIndex][2] = strconv.Itoa(int(currentBalance))

    // Save updated balance
    if err := writeCSV(filePath, records); err != nil {
        return &pb.CommitResponse{
            Success: false,
            Message: "Failed to update balance",
        }, err
    }

    return &pb.CommitResponse{
        Success: true,
        Message: "Transaction committed successfully",
        Balance: currentBalance,
    }, nil
}

// Abort RPC handler for 2PC
func (s *BankServer) Abort(ctx context.Context, req *pb.AbortRequest) (*pb.AbortResponse, error) {
    txID := req.TransactionId

    // Remove from prepared transactions
    preparedMu.Lock()
    _, exists := preparedTxs[txID]
    if exists {
        delete(preparedTxs, txID)
    }
    preparedMu.Unlock()

    return &pb.AbortResponse{
        Success: true,
        Message: "Transaction aborted",
    }, nil
}

// Helper function to get bank number from context
func getBankNumberFromContext() string {
    if len(os.Args) < 2 {
        return ""
    }
    return os.Args[1]
}

func (s *BankServer) TransferWithinBank(ctx context.Context, req *pb.TransferRequest) (*pb.TransferResponse, error) {
    bankNumber := getBankNumberFromContext()
    filePath := fmt.Sprintf("../account_details/%s.csv", bankNumber)

    mu.Lock()
    defer mu.Unlock()

    // Authenticate source user
    authSource, records, sourceIndex := authenticateUser(filePath, req.SourceUsername, req.Password)
    if !authSource {
        return &pb.TransferResponse{
            Success: false,
            Message: "Source authentication failed",
        }, nil
    }

    // Find target user
    targetIndex := -1
    for i, record := range records {
        if len(record) >= 3 && record[0] == req.TargetUsername {
            targetIndex = i
            break
        }
    }
    if targetIndex == -1 {
        return &pb.TransferResponse{
            Success: false,
            Message: "Target user not found",
        }, nil
    }

    // Check source balance
    sourceBalance, _ := strconv.Atoi(records[sourceIndex][2])
    if int32(sourceBalance) < req.Amount {
        return &pb.TransferResponse{
            Success: false,
            Message: "Insufficient funds",
        }, nil
    }

    // Perform transfer
    sourceBalance -= int(req.Amount)
    targetBalance, _ := strconv.Atoi(records[targetIndex][2])
    targetBalance += int(req.Amount)

    // Update records
    records[sourceIndex][2] = strconv.Itoa(sourceBalance)
    records[targetIndex][2] = strconv.Itoa(targetBalance)

    // Write back to CSV
    if err := writeCSV(filePath, records); err != nil {
        return &pb.TransferResponse{
            Success: false,
            Message: "Failed to update accounts",
        }, err
    }

    // Mark as processed
    processed[req.SourceUsername+"-"+req.TargetUsername+"-"+strconv.Itoa(int(req.Amount))] = true

    return &pb.TransferResponse{
        Success:     true,
        Message:     "Transfer completed successfully",
        NewBalance:  int32(sourceBalance),
    }, nil
}
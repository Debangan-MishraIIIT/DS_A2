package main

import (
	"context"
	"fmt"
	"go.etcd.io/etcd/client/v3"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
	"log"
	pb "project/proto"
	"sync"
	"time"
)

type TransactionState struct {
	ID             string
	SourceBank     string
	SourceUsername string
	TargetBank     string
	TargetUsername string
	Amount         int32
	SourcePassword string
	PrepareTimeout time.Duration
	Status         string // "pending", "prepared", "committed", "aborted"
	SourcePrepared bool
	TargetPrepared bool
	mu             sync.Mutex
}

var (
	transactions = make(map[string]*TransactionState)
	txMutex      sync.Mutex
)

// ProcessPayment implements the 2PC coordinator
func (s *GatewayServer) ProcessPayment(ctx context.Context, req *pb.PaymentRequest) (*pb.PaymentResponse, error) {
	// Input validation
	if req.Amount <= 0 {
		return &pb.PaymentResponse{
			Success: false,
			Message: "Invalid amount for payment",
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

	// Check if source and target banks exist
	sourceAddr, sourceExists := bankServers[req.SourceBankNumber]
	targetAddr, targetExists := bankServers[req.TargetBankNumber]

	if !sourceExists {
		return &pb.PaymentResponse{
			Success: false,
			Message: "Source bank server not found",
		}, nil
	}

	if !targetExists {
		return &pb.PaymentResponse{
			Success: false,
			Message: "Target bank server not found",
		}, nil
	}

	// Create transaction ID and state
	txID := fmt.Sprintf("payment-%s", req.Uuid)

	// Check for duplicate transaction
	txMutex.Lock()
	if _, exists := transactions[txID]; exists {
		txMutex.Unlock()
		return &pb.PaymentResponse{
			Success: false,
			Message: "Duplicate payment request detected",
		}, nil
	}

	// Configure transaction timeout (make this configurable)
	prepareTimeout := 5 * time.Second

	// Create and store transaction state
	tx := &TransactionState{
		ID:             txID,
		SourceBank:     req.SourceBankNumber,
		SourceUsername: req.SourceUsername,
		TargetBank:     req.TargetBankNumber,
		TargetUsername: req.TargetUsername,
		Amount:         req.Amount,
		SourcePassword: req.Password,
		PrepareTimeout: prepareTimeout,
		Status:         "pending",
		SourcePrepared: false,
		TargetPrepared: false,
	}
	transactions[txID] = tx
	txMutex.Unlock()

	// same bank handling
	sameBank := req.SourceBankNumber == req.TargetBankNumber
	if sameBank {
		// Handle as single-bank transfer
		return s.processSameBankTransfer(ctx, tx, sourceAddr)
	}

	// Set up connections to source and target banks
	sourceConn, err := grpc.Dial(sourceAddr, grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		log.Printf("Failed to connect to source bank: %v", err)
		return abortTransaction(tx, "Failed to connect to source bank")
	}
	defer sourceConn.Close()
	sourceClient := pb.NewBankServiceClient(sourceConn)

	targetConn, err := grpc.Dial(targetAddr, grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		log.Printf("Failed to connect to target bank: %v", err)
		return abortTransaction(tx, "Failed to connect to target bank")
	}
	defer targetConn.Close()
	targetClient := pb.NewBankServiceClient(targetConn)

	// PHASE 1: PREPARE
	// Create a timeout context for prepare phase
	prepareCtx, cancel := context.WithTimeout(ctx, prepareTimeout)
	defer cancel()

	// Prepare source bank (debit)
	sourcePrepareReq := &pb.PrepareRequest{
		TransactionId: txID,
		Username:      req.SourceUsername,
		BankNumber:    req.SourceBankNumber,
		Amount:        req.Amount,
		Operation:     "debit",
		Password:      req.Password,
	}

	sourcePrepareRes, err := sourceClient.Prepare(prepareCtx, sourcePrepareReq)
	if err != nil || !sourcePrepareRes.Ready {
		log.Printf("Source bank prepare failed: %v, %v", err, sourcePrepareRes)
		var errMsg string
		if err != nil {
			errMsg = fmt.Sprintf("Source bank not ready: %v", err.Error())
		} else {
			errMsg = fmt.Sprintf("Source bank not ready: %v", sourcePrepareRes.Message)
		}
		return abortTransaction(tx, errMsg)
	}

	// Set source as prepared
	tx.mu.Lock()
	tx.SourcePrepared = true
	tx.mu.Unlock()

	// Prepare target bank (credit)
	targetPrepareReq := &pb.PrepareRequest{
		TransactionId: txID,
		Username:      req.TargetUsername,
		BankNumber:    req.TargetBankNumber,
		Amount:        req.Amount,
		Operation:     "credit",
		Password:      "", // No password needed for credit
	}

	targetPrepareRes, err := targetClient.Prepare(prepareCtx, targetPrepareReq)
	if err != nil || !targetPrepareRes.Ready {
		log.Printf("Target bank prepare failed: %v, %v", err, targetPrepareRes)

		var errMsg string
		if err != nil {
			errMsg = fmt.Sprintf("Target bank not ready: %v", err.Error())
		} else {
			errMsg = fmt.Sprintf("Target bank not ready: %v", targetPrepareRes.Message)
		}

		return abortTransaction(tx, errMsg)
	}

	// Set target as prepared
	tx.mu.Lock()
	tx.TargetPrepared = true
	tx.Status = "prepared"
	tx.mu.Unlock()

	// PHASE 2: COMMIT
	// Create a commit request
	commitReq := &pb.CommitRequest{
		TransactionId: txID,
	}

	// Commit on source bank
	sourceCommitRes, err := sourceClient.Commit(ctx, commitReq)
	if err != nil || !sourceCommitRes.Success {
		log.Printf("Source bank commit failed: %v", err)
		return abortTransaction(tx, "Source bank commit failed")
	}

	// Commit on target bank
	targetCommitRes, err := targetClient.Commit(ctx, commitReq)
	if err != nil || !targetCommitRes.Success {
		log.Printf("Target bank commit failed: %v", err)
		// This is a critical failure - the source was committed but target failed
		// In a real system, this would require manual intervention or a recovery process
		return &pb.PaymentResponse{
			Success:       false,
			Message:       "CRITICAL ERROR: Inconsistent state, source committed but target failed",
			SourceBalance: sourceCommitRes.Balance,
		}, nil
	}

	// Update transaction status
	tx.mu.Lock()
	tx.Status = "committed"
	tx.mu.Unlock()

	return &pb.PaymentResponse{
		Success: true,
		Message: fmt.Sprintf("Payment of %d successfully transferred to %s at bank %s",
			req.Amount, req.TargetUsername, req.TargetBankNumber),
		SourceBalance: sourceCommitRes.Balance,
	}, nil
}

// Helper function to abort transaction
func abortTransaction(tx *TransactionState, reason string) (*pb.PaymentResponse, error) {
	tx.mu.Lock()
	tx.Status = "aborted"
	tx.mu.Unlock()

	// If any node was prepared, we need to send abort messages
	if tx.SourcePrepared || tx.TargetPrepared {
		abortTransactionOnBanks(tx)
	}

	return &pb.PaymentResponse{
		Success: false,
		Message: reason,
	}, nil
}

// Send abort to all prepared banks
func abortTransactionOnBanks(tx *TransactionState) {
	ctx := context.Background()
	etcdClient, err := clientv3.New(clientv3.Config{
		Endpoints:   []string{"localhost:2379"},
		DialTimeout: time.Second,
	})
	if err != nil {
		log.Printf("Failed to connect to etcd during abort: %v", err)
		return
	}
	defer etcdClient.Close()

	bankServers := fetchAvailableBanks(etcdClient)

	// Abort on source bank if prepared
	if tx.SourcePrepared {
		if sourceAddr, exists := bankServers[tx.SourceBank]; exists {
			sourceConn, err := grpc.Dial(sourceAddr, grpc.WithTransportCredentials(insecure.NewCredentials()))
			if err == nil {
				defer sourceConn.Close()
				sourceClient := pb.NewBankServiceClient(sourceConn)

				abortReq := &pb.AbortRequest{
					TransactionId: tx.ID,
				}

				_, err = sourceClient.Abort(ctx, abortReq)
				if err != nil {
					log.Printf("Error aborting on source bank: %v", err)
				}
			}
		}
	}

	// Abort on target bank if prepared
	if tx.TargetPrepared {
		if targetAddr, exists := bankServers[tx.TargetBank]; exists {
			targetConn, err := grpc.Dial(targetAddr, grpc.WithTransportCredentials(insecure.NewCredentials()))
			if err == nil {
				defer targetConn.Close()
				targetClient := pb.NewBankServiceClient(targetConn)

				abortReq := &pb.AbortRequest{
					TransactionId: tx.ID,
				}

				_, err = targetClient.Abort(ctx, abortReq)
				if err != nil {
					log.Printf("Error aborting on target bank: %v", err)
				}
			}
		}
	}
}

// Helper for same bank transfers
func (s *GatewayServer) processSameBankTransfer(ctx context.Context, tx *TransactionState, bankAddr string) (*pb.PaymentResponse, error) {
	// Establish connection to the bank
	conn, err := grpc.Dial(bankAddr, grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		log.Printf("Failed to connect to bank: %v", err)
		return abortTransaction(tx, "Bank connection failed")
	}
	defer conn.Close()
	bankClient := pb.NewBankServiceClient(conn)

	// Direct transfer request (combines debit/credit in one operation)
	transferReq := &pb.TransferRequest{
		SourceUsername: tx.SourceUsername,
		TargetUsername: tx.TargetUsername,
		Amount:         tx.Amount,
		Password:       tx.SourcePassword,
	}

	// Single RPC call for atomic transfer
	transferRes, err := bankClient.TransferWithinBank(ctx, transferReq)
	if err != nil {
		log.Printf("Same-bank transfer failed: %v", err)
		return &pb.PaymentResponse{
			Success: false,
			Message: fmt.Sprintf("Transfer failed: %v", err.Error()),
		}, nil
	}

	if !transferRes.Success {
		return &pb.PaymentResponse{
			Success: false,
			Message: transferRes.Message,
		}, nil
	}

	// Update transaction state directly to committed
	tx.mu.Lock()
	tx.Status = "committed"
	tx.mu.Unlock()

	return &pb.PaymentResponse{
		Success:       true,
		Message:       fmt.Sprintf("Successfully transferred %d to %s", tx.Amount, tx.TargetUsername),
		SourceBalance: transferRes.NewBalance,
	}, nil
}

package main

import (
	"context"
	"flag"
	"fmt"
	"log"
	"math/rand"
	"net"
	"os"
	pb "project/proto"
	"strconv"
	"strings"
	"sync"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
)

type Commander struct {
	id        int
	isTraitor bool
	n         int
	traitors  map[int]bool
	t         int

	mu              sync.RWMutex
	messagesByRound map[int32][]*pb.OrderRequest
	clients         map[int]pb.BFTServiceClient
}

type server struct {
	pb.UnimplementedBFTServiceServer
	general *Commander
}

func (s *server) SendOrder(ctx context.Context, req *pb.OrderRequest) (*pb.OrderResponse, error) {
	s.general.mu.Lock()
	defer s.general.mu.Unlock()

	log.Printf("[%d] Received %s from %d (Round %d)", s.general.id, req.Order, req.SenderId, req.Round)

	round := req.Round
	if _, ok := s.general.messagesByRound[round]; !ok {
		s.general.messagesByRound[round] = []*pb.OrderRequest{}
	}
	s.general.messagesByRound[round] = append(s.general.messagesByRound[round], req)

	return &pb.OrderResponse{Received: true}, nil
}

func NewCommander(id int, isTraitor bool, n int, traitors []int) *Commander {
	tMap := make(map[int]bool)
	for _, t := range traitors {
		tMap[t] = true
	}
	return &Commander{
		id:              id,
		isTraitor:       isTraitor,
		n:               n,
		traitors:        tMap,
		t:               len(traitors),
		messagesByRound: make(map[int32][]*pb.OrderRequest),
		clients:         make(map[int]pb.BFTServiceClient),
	}
}

func (g *Commander) startServer() {
	port := 50000 + g.id
	lis, err := net.Listen("tcp", fmt.Sprintf(":%d", port))
	if err != nil {
		log.Fatalf("[%d] Failed to listen: %v", g.id, err)
	}

	s := grpc.NewServer()
	pb.RegisterBFTServiceServer(s, &server{general: g})
	log.Printf("[%d] Server started on port %d", g.id, port)

	if err := s.Serve(lis); err != nil {
		log.Fatalf("[%d] Failed to serve: %v", g.id, err)
	}
}

func (g *Commander) connectToPeers() {
	for i := 0; i < g.n; i++ {
		if i == g.id {
			continue
		}
		conn, err := grpc.NewClient(
			fmt.Sprintf("localhost:%d", 50000+i),
			grpc.WithTransportCredentials(insecure.NewCredentials()),
		)
		if err != nil {
			log.Fatalf("[%d] Failed to connect to %d: %v", g.id, i, err)
		}
		g.clients[i] = pb.NewBFTServiceClient(conn)
	}
}

func (g *Commander) runRounds() {
	time.Sleep(5 * time.Second) // Wait for network initialization

	rounds := g.t + 1
	if 3*g.t >= g.n {
		log.Printf("[%d] WARNING: T >= N/3 (Consensus may fail)", g.id)
	}

	for currentRound := 0; currentRound < rounds; currentRound++ {
		if g.id == 0 && currentRound == 0 {
			g.sendInitialOrders()
		}

		time.Sleep(2 * time.Second)
		g.relayMessages(currentRound)
	}

	g.decide()
}

func (g *Commander) sendInitialOrders() {
	baseOrder := "Attack"
	if rand.Intn(2) == 0 {
		baseOrder = "Retreat"
	}

	for i := 1; i < g.n; i++ {
		order := baseOrder
		if g.isTraitor {
			if rand.Intn(2) == 0 {
				order = "Attack"
			} else {
				order = "Retreat"
			}
			log.Printf("[%d] (TRAITOR) Sending %s to %d", g.id, order, i)
		}
		g.sendOrder(i, 0, order)
	}
}

func (g *Commander) relayMessages(currentRound int) {
	nextRound := currentRound + 1
	if nextRound > g.t {
		return
	}

	g.mu.RLock()
	messages := g.messagesByRound[int32(currentRound)]
	g.mu.RUnlock()

	for _, msg := range messages {
		for i := 0; i < g.n; i++ {
			if i == g.id {
				continue
			}

			order := msg.Order
			if g.isTraitor && rand.Intn(2) == 0 {
				if order == "Attack" {
					order = "Retreat"
				} else {
					order = "Attack"
				}
				log.Printf("[%d] (TRAITOR) Flipping to %s for %d", g.id, order, i)
			}
			g.sendOrder(i, nextRound, order)
		}
	}
}

func (g *Commander) sendOrder(target, round int, order string) {
	if target == g.id {
		return
	}

	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()

	_, err := g.clients[target].SendOrder(ctx, &pb.OrderRequest{
		SenderId: int32(g.id),
		Round:    int32(round),
		Order:    order,
	})

	if err != nil {
		log.Printf("[%d] Error sending to %d: %v", g.id, target, err)
	} else {
		log.Printf("[%d] Sent %s to %d (Round %d)", g.id, order, target, round)
	}
}

func (g *Commander) decide() {
	orders := []string{}
	g.mu.RLock()
	for _, roundMsgs := range g.messagesByRound {
		for _, msg := range roundMsgs {
			orders = append(orders, msg.Order)
		}
	}
	g.mu.RUnlock()

	counts := make(map[string]int)
	for _, o := range orders {
		counts[o]++
	}

	max, decision := 0, ""
	for o, c := range counts {
		if c > max {
			max = c
			decision = o
		} else if c == max {
			decision = "TIE"
		}
	}

	if !g.isTraitor {
		log.Printf("[%d] FINAL DECISION: %s (Counts: %v)", g.id, decision, counts)
	} else {
		log.Printf("[%d] (TRAITOR) Final counts: %v", g.id, counts)
	}
}

func main() {
	index := flag.Int("index", -1, "General's index")
	isTraitor := flag.Bool("traitor", false, "Is traitor?")
	n := flag.Int("n", 0, "Total generals")
	traitors := flag.String("traitors", "", "Comma-separated traitor indices")

	flag.Parse()

	if *index == -1 || *n == 0 || *traitors == "" {
		flag.Usage()
		os.Exit(1)
	}

	traitorIndices := []int{}
	for _, t := range strings.Split(*traitors, ",") {
		i, err := strconv.Atoi(t)
		if err != nil {
			log.Fatal("Invalid traitor index: ", t)
		}
		traitorIndices = append(traitorIndices, i)
	}

	general := NewCommander(*index, *isTraitor, *n, traitorIndices)
	go general.startServer()
	general.connectToPeers()
	general.runRounds()

	select {}
}

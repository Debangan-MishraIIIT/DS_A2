package main

import (
	"context"
	"flag"
	"fmt"
	"log"
	"net"
	"sync"
	pb "project/proto"
	"google.golang.org/grpc"
)

type masterServer struct {
    pb.UnimplementedMasterServiceServer

    // Configuration
    numMappers   int
    numReducers  int
    task         string

    // Synchronization
    mu           sync.Mutex
    mappersMu    sync.Mutex
    reducersMu   sync.Mutex

    // State tracking
    activeMappers   map[int32]bool
    assignedMapperIndices map[int32]int32
	activeReducers  map[int32]bool
    assignedReducerIndices map[int32]int32
    availableReducerIndices []int32
    completedReducerIndices map[int32]bool

	mappersDone     int
    reduceQueue     []int32
    isTerminating   bool
	nextMapperIndex int32
}

func newMasterServer(numMappers, numReducers int, task string) *masterServer {
    availableReducerIndices := make([]int32, numReducers)
    for i := 0; i < numReducers; i++ {
        availableReducerIndices[i] = int32(i)
    }

    return &masterServer{
        numMappers:      numMappers,
        numReducers:     numReducers,
        task:            task,
        activeMappers:   make(map[int32]bool),
        assignedMapperIndices: make(map[int32]int32),
        activeReducers:  make(map[int32]bool),
        assignedReducerIndices: make(map[int32]int32),
        availableReducerIndices: availableReducerIndices,
        completedReducerIndices: make(map[int32]bool),
        mappersDone:     0,
        reduceQueue:     []int32{},
        isTerminating:   false,
		nextMapperIndex: 0,
    }
}

func (s *masterServer) ShouldMap(ctx context.Context, req *pb.WorkerRequest) (*pb.MapResponse, error) {
	s.mappersMu.Lock()
	defer s.mappersMu.Unlock()

	workerID := req.WorkerId

	if s.nextMapperIndex >= int32(s.numMappers) {
		log.Printf("Worker %d asked to map but we have enough mappers", workerID)
		return &pb.MapResponse{ShouldMap: false, MapperIndex: -1, NumReducers: -1, Task: s.task}, nil
	}

	mapperIndex := s.nextMapperIndex
	s.activeMappers[workerID] = true
	s.nextMapperIndex++ 

	log.Printf("Assigned worker %d as mapper %d", workerID, mapperIndex)
	return &pb.MapResponse{
		ShouldMap:    true,
		MapperIndex:  mapperIndex,
		NumReducers:  int32(s.numReducers),
		Task:        s.task,
	}, nil
}

func (s *masterServer) MapDone(ctx context.Context, req *pb.WorkerDone) (*pb.AckResponse, error) {
	s.mappersMu.Lock()
	defer s.mappersMu.Unlock()

	workerID := req.WorkerId

	if !s.activeMappers[workerID] {
		return &pb.AckResponse{Received: false}, nil
	}

	delete(s.activeMappers, workerID)
	s.mappersDone++

	if s.mappersDone == s.numMappers {
		s.reducersMu.Lock()
		defer s.reducersMu.Unlock()

		for len(s.reduceQueue) > 0 && len(s.availableReducerIndices) > 0 {
			workerID := s.reduceQueue[0]
			s.reduceQueue = s.reduceQueue[1:]

			if _, exists := s.activeReducers[workerID]; exists {
				continue
			}

			index := s.availableReducerIndices[0]
			s.availableReducerIndices = s.availableReducerIndices[1:]

			s.activeReducers[workerID] = true
			s.assignedReducerIndices[workerID] = index
		}
	}

	return &pb.AckResponse{Received: true}, nil
}

func (s *masterServer) ShouldReduce(ctx context.Context, req *pb.WorkerRequest) (*pb.ReduceResponse, error) {
	s.reducersMu.Lock()
	defer s.reducersMu.Unlock()

	workerID := req.WorkerId

	if s.mappersDone < s.numMappers {
		for _, id := range s.reduceQueue {
			if id == workerID {
				return &pb.ReduceResponse{ShouldReduce: false, ReducerIndex: -1, Task: s.task}, nil
			}
		}
		s.reduceQueue = append(s.reduceQueue, workerID)
		return &pb.ReduceResponse{ShouldReduce: false, ReducerIndex: -1, Task: s.task}, nil
	}

	if index, ok := s.assignedReducerIndices[workerID]; ok {
		return &pb.ReduceResponse{ShouldReduce: true, ReducerIndex: index, Task: s.task}, nil
	}

	if len(s.availableReducerIndices) == 0 {
		s.reduceQueue = append(s.reduceQueue, workerID)
		return &pb.ReduceResponse{ShouldReduce: false, ReducerIndex: -1, Task: s.task}, nil
	}

	index := s.availableReducerIndices[0]
	s.availableReducerIndices = s.availableReducerIndices[1:]
	s.assignedReducerIndices[workerID] = index
	s.activeReducers[workerID] = true

	return &pb.ReduceResponse{ShouldReduce: true, ReducerIndex: index, Task: s.task}, nil
}

func (s *masterServer) ReduceDone(ctx context.Context, req *pb.WorkerDone) (*pb.AckResponse, error) {
	s.reducersMu.Lock()
	defer s.reducersMu.Unlock()

	workerID := req.WorkerId

	if !s.activeReducers[workerID] {
		return &pb.AckResponse{Received: false}, nil
	}

	index, ok := s.assignedReducerIndices[workerID]
	if !ok {
		return &pb.AckResponse{Received: false}, nil
	}

	delete(s.activeReducers, workerID)
	delete(s.assignedReducerIndices, workerID)
	s.completedReducerIndices[index] = true

	if len(s.completedReducerIndices) == s.numReducers {
		s.mu.Lock()
		s.isTerminating = true
		s.mu.Unlock()
	} else {
		for len(s.reduceQueue) > 0 && len(s.availableReducerIndices) > 0 {
			nextWorkerID := s.reduceQueue[0]
			s.reduceQueue = s.reduceQueue[1:]

			if _, exists := s.assignedReducerIndices[nextWorkerID]; exists {
				continue
			}

			index := s.availableReducerIndices[0]
			s.availableReducerIndices = s.availableReducerIndices[1:]
			s.assignedReducerIndices[nextWorkerID] = index
			s.activeReducers[nextWorkerID] = true
		}
	}

	return &pb.AckResponse{Received: true}, nil
}

func (s *masterServer) ShouldTerminate(ctx context.Context, req *pb.WorkerRequest) (*pb.TerminateResponse, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	return &pb.TerminateResponse{ShouldTerminate: s.isTerminating}, nil
}


func main() {
	numMappers := flag.Int("mappers", 3, "Number of mappers")
	numReducers := flag.Int("reducers", 2, "Number of reducers")
	task := flag.String("task", "wc", "Task to perform (wc/ii)")
	port := flag.Int("port", 50051, "Port to listen on")

	flag.Parse()

	lis, err := net.Listen("tcp", fmt.Sprintf(":%d", *port))
	if err != nil {
		log.Fatalf("Failed to listen: %v", err)
	}

	grpcServer := grpc.NewServer()
	masterServer := newMasterServer(*numMappers, *numReducers, *task)
	pb.RegisterMasterServiceServer(grpcServer, masterServer)

	log.Printf("Master server started on port %d with %d mappers and %d reducers for task %s",
		*port, *numMappers, *numReducers, *task)

	if err := grpcServer.Serve(lis); err != nil {
		log.Fatalf("Failed to serve: %v", err)
	}
}
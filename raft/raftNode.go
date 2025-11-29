package raft;

import (
	"fmt"
	"log"
	"slices"
	"time"
	"context"
	"math/rand"
	"raft-go/HaskellRipoff/Data"

	pb "raft-go/grpc"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
)

type NodeId uint32;
type Role byte;

const (
	Follower Role = iota
	Candidate
	Leader
)

type NodeInfo struct {
	id NodeId
	connectionaddr string
}

type Peer struct {
	info NodeInfo
	grpcConn *grpc.ClientConn
	client pb.RaftServiceClient
}

func (peer *Peer) InitPeer(info NodeInfo) {
	peer.info = info
	conn, err := grpc.NewClient(
		info.connectionaddr,
		grpc.WithTransportCredentials(insecure.NewCredentials()))

	if err != nil {
		panic(err.Error())
	}

	peer.grpcConn = conn
	peer.client = pb.NewRaftServiceClient(peer.grpcConn)
}

type MessageKind byte; 

const (
	HeartbeatMessage MessageKind = iota
	VoteRequestMessage
	VoteResponseMessage
)

type Message struct {
	kind MessageKind

	// All messages have these
	id NodeId
	currentTerm uint32

	// VoteRequest
	logLength uint32
	lastTerm uint32

	// VoteResponse
	accept bool
}

type NodeState struct {
	info NodeInfo
	peers map[NodeId]Peer

	currentTerm uint32;
	votedFor Data.Maybe[NodeId];
	currentRole Role
	currentLeader Data.Maybe[NodeId];

	votesReceived []NodeId

	timeoutDuration time.Duration
//	timeoutDeadline time.Time

	messages chan Message

	sentLength []uint32
	ackedLength []uint32
}

func (state *NodeState) Initialize(id NodeId, nodes []NodeInfo) {
	state.info = nodes[id]
	state.peers = make(map[NodeId]Peer, len(nodes))
	state.messages = make(chan Message, 10)

	for i, n := range nodes {
		if NodeId(i) == id { continue }

		state.peers[n.id] = Peer {}
		v, _ := state.peers[n.id]
		v.InitPeer(n)
	}

	state.currentTerm = 0;
	state.votedFor = Data.Nothing[NodeId]();
	state.currentRole = Follower;
	state.currentLeader = Data.Nothing[NodeId]();
	state.timeoutDuration = time.Duration(0);
}

func (state *NodeState) resetTime() {
	timeout := (rand.Int() % 150) + 150;
	state.timeoutDuration = time.Duration(timeout)*time.Millisecond
	//state.timeoutDeadline = time.Now().Add(time.Duration(timeout)*time.Millisecond)
}

func (server *NodeState) onVoteResponse(msg Message) error {
	log.Printf("Got a vote response from Node ID: %d", msg.id)

	if server.currentRole == Candidate && server.currentTerm == msg.currentTerm && msg.accept {
		log.Printf("Node ID %d accepted my vote request", msg.id)
		if !slices.Contains(server.votesReceived[:], msg.id) {
			server.votesReceived = append(server.votesReceived, msg.id)
		}

		numOfVotes := len(server.votesReceived)
		neededTmp := (len(server.peers) + 2) 
		needed := neededTmp / 2 + (neededTmp % 2);

		if numOfVotes >= needed {
			server.currentRole = Leader
			server.currentLeader = Data.Just(server.info.id)
			// cancel election timer

//			for _, _ := range server.peers {
//				 sentLength[follower] = log.length
//				 sentLength[follower] = log.length
//			     ReplicateLog(server.info.id, follower)	
//			}
		}
	} else if msg.currentTerm > server.currentTerm {
		log.Printf("Node ID %d has higher current term, cancelling my election", msg.id)
		server.currentTerm = msg.currentTerm
		server.currentRole = Follower
		server.votedFor = Data.Nothing[NodeId]()
		// cancel election timer
	}

	return nil
}

func (server *NodeState) onVoteRequest(msg Message) error {
	log.Printf("Got vote request from Node ID: %d", msg.id)

	peer, found := server.peers[msg.id]
	if !found {
		return fmt.Errorf("Node ID not found: %d", msg.id)
	}

	accepted := msg.currentTerm > server.currentTerm

	if accepted {
		server.currentTerm = msg.currentTerm
		server.currentRole = Follower
		server.votedFor = Data.Nothing[NodeId]()
	}

	var logLength uint32 = 0
	var lastTerm uint32 = 0
	// if log.length > 0 then lastTerm := log[log.length - 1].term; end if

	logOk := (msg.lastTerm > server.currentTerm) || (msg.lastTerm == lastTerm && msg.logLength >= logLength)

	if msg.currentTerm == server.currentTerm && logOk && server.votedFor.EqualOrNothing(server.info.id) {
		server.votedFor = Data.Just(NodeId(msg.id))
		accepted = true
		log.Printf("Accepting vote from Node ID: %d", msg.id)
	} else {
		accepted = false
		log.Printf("Rejecting vote from Node ID: %d", msg.id)
	}

	rep := pb.VoteResponse { 
		Id: uint32(server.info.id),  
		CurrentTerm: server.currentTerm,
		Accept: accepted,
	}

	peer.client.Response(context.Background(), &rep)

	return nil
}

func (state *NodeState) onHeartbeat(msg Message) {
	peer, found := state.peers[msg.id]
	if !found {
		log.Printf("Got heartbeat from unknown node: %d", msg.id)
	}

	log.Printf("Got hearbeat from node: %v", peer)

	state.currentLeader = Data.Just(msg.id)
	state.currentRole = Follower
	state.currentTerm = msg.currentTerm

	state.resetTime()
}

func (state *NodeState) onTimeout() {
	log.Println("Timeout occured, assuming leader is dead, holding election")
	state.currentTerm += 1
	state.currentRole = Candidate
	state.votedFor = Data.Just(state.info.id)

	state.votesReceived = nil
	state.votesReceived = append(state.votesReceived, state.info.id)
//	lastTerm := 0

	// if log.length > 0 then lastTerm := log[log.length - 1].term; end if

	msg := pb.VoteRequest {
		Id: uint32(state.info.id),
		CurrentTerm: state.currentTerm,
		LogLength: 0,
		LastTerm: 0,
	}

	for _, node := range state.peers {
		node.client.Vote(context.Background(), &msg)
	}
	state.resetTime()
}

func (state *NodeState) Loop() {
	state.resetTime()
	for {
		select {
			case msg := <-state.messages:
				switch msg.kind {
				case HeartbeatMessage:
				state.onHeartbeat(msg)
				case VoteRequestMessage:
				state.onVoteRequest(msg)
				case VoteResponseMessage:
				state.onVoteResponse(msg)
				}
			case <-time.After(state.timeoutDuration):
				state.onTimeout();
		}
	}
}



// gRPC handlers

func (server *NodeState) Heartbeat(
	ctx context.Context, req *pb.HeartbeatMsg) (*pb.Acknowledgement, error) {
	server.messages <- Message {kind: HeartbeatMessage}
	return &pb.Acknowledgement{}, nil
}

func (server *NodeState) Response(
	ctx context.Context, req *pb.VoteResponse) (*pb.Acknowledgement, error) {
	server.messages <- Message {
		kind: VoteResponseMessage,
		id: NodeId(req.Id),
		currentTerm: req.CurrentTerm,
		accept: req.Accept,
	}
	return &pb.Acknowledgement{}, nil
}

func (server *NodeState) Election(
	ctx context.Context, req *pb.VoteRequest) (*pb.Acknowledgement, error) {

	server.messages <- Message {
		kind: VoteRequestMessage,
		id: NodeId(server.info.id),
		currentTerm: server.currentTerm,
		logLength: 0,
		lastTerm: 0,
	}

	return &pb.Acknowledgement{}, nil
}

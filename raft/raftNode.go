package raft;

import (
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
)

type Message struct {
	kind MessageKind
}

type NodeState struct {
	info NodeInfo
	peers []NodeInfo

	currentTerm uint32;
	votedFor Data.Maybe[NodeId];
	currentRole Role
	currentLeader Data.Maybe[NodeId];

	votesReceived []uint

	timeoutDeadline time.Time

	messages chan Message

	//sentLength []uint
	//ackedLength []uint
}

func (state *NodeState) Initialize(id NodeId, nodes []NodeInfo) {
	state.info = nodes[id]
	state.peers = nodes

	state.currentTerm = 0;
	state.votedFor = Data.Nothing[NodeId]();
	state.currentRole = Follower;
	state.currentLeader = Data.Nothing[NodeId]();
	state.timeoutDeadline = time.Now();
}

func (state *NodeState) resetTime() {
	timeout := (rand.Int() % 150) + 150;
	state.timeoutDeadline = time.Now().Add(time.Duration(timeout)*time.Millisecond)
}

func (server *NodeState) Heartbeat(
	ctx context.Context, req *pb.HeartbeatMsg) (*pb.Acknowledgement, error) {
	server.messages <- Message {kind: HeartbeatMessage}
	return &pb.Acknowledgement{}, nil
}

func (server *NodeState) Election(
	ctx context.Context, req *pb.VoteRequest) (*pb.VoteResponse, error) {

	msg := pb.VoteResponse { 
		Id: uint32(server.info.id),  
		CurrentTerm: server.currentTerm,
		Accept: true,
	}

	return &msg, nil
}

func (state *NodeState) onTimeout() {
	state.currentTerm += 1
	state.currentRole = Candidate
	state.votedFor = Data.Just(state.info.id)

	// state.votesReceived = {state.info.id}
	//lastTerm := 0

	// if log.length > 0 then lastTerm := log[log.length - 1].term; end if

}

func (state *NodeState) Loop() {
	state.resetTime()
	for {
		msg := <- state.messages

		switch msg.kind {
		case HeartbeatMessage:
		state.resetTime();
		}
	}
}



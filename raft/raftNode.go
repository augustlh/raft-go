package raft;

import (
	"os"
	"fmt"
	"log"
	"net"
	"slices"
	"strings"
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
	Id NodeId
	ConnectionAddr string
}

func LoadServers(file string) ([]NodeInfo, error) {
	b, err := os.ReadFile(file)
	if err != nil { return nil, err }

	lines := strings.Split(string(b), "\n")

	nodes := make([]NodeInfo, len(lines))

	for i, l := range lines {
		if l == "" { continue }
		nodes[i].ConnectionAddr = l
		nodes[i].Id = NodeId(i)
		fmt.Println(i, l)
	}
	return nodes, nil
}

type Peer struct {
	Info NodeInfo
	grpcConn *grpc.ClientConn
	client pb.RaftServiceClient
}

func (peer *Peer) InitPeer(info NodeInfo) {
	peer.Info = info
	conn, err := grpc.NewClient(
		info.ConnectionAddr,
		grpc.WithTransportCredentials(insecure.NewCredentials()))

	fmt.Printf("PEER INIT: %s",peer.Info.ConnectionAddr)

	if err != nil {
		panic(err.Error())
	}

	peer.grpcConn = conn
	peer.client = pb.NewRaftServiceClient(peer.grpcConn)

	if peer.client == nil {
		panic("Client is nil")
	}
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
	pb.UnimplementedRaftServiceServer
	info NodeInfo
	peers []Peer

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
	state.messages = make(chan Message, 10)

	for i, n := range nodes {
		if NodeId(i) == id { continue }

		p := Peer {}
		p.InitPeer(n)

		state.peers = append(state.peers, p)
	}

	state.currentTerm = 0;
	state.votedFor = Data.Nothing[NodeId]();
	state.currentRole = Follower;
	state.currentLeader = Data.Nothing[NodeId]();
	state.timeoutDuration = time.Duration(0);
}

func (state *NodeState) resetTime() {
	timeout := (rand.Int() % 150) + 500;
	state.timeoutDuration = time.Duration(timeout)*time.Millisecond
	//state.timeoutDeadline = time.Now().Add(time.Duration(timeout)*time.Millisecond)
}

func (state *NodeState) findPeer(id NodeId) Data.Maybe[Peer] {
	for _, v := range state.peers {
		if v.Info.Id == id { return Data.Just(v) }
	}

	return Data.Nothing[Peer]()
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
			server.currentLeader = Data.Just(server.info.Id)
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

//	peer, found := server.peers[msg.id]

	peer := server.findPeer(msg.id)
	if peer.IsNothing() {
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

	if msg.currentTerm == server.currentTerm && logOk && server.votedFor.EqualOrNothing(server.info.Id) {
		server.votedFor = Data.Just(NodeId(msg.id))
		accepted = true
		log.Printf("Accepting vote from Node ID: %d", msg.id)
	} else {
		accepted = false
		log.Printf("Rejecting vote from Node ID: %d", msg.id)
	}

	rep := pb.VoteResponse { 
		Id: uint32(server.info.Id),  
		CurrentTerm: server.currentTerm,
		Accept: accepted,
	}

	_, err := peer.FromJust().client.Response(context.Background(), &rep)

	if err != nil {
		log.Printf("Error when responding: %s", err.Error())
	}

	return nil
}

func (state *NodeState) onHeartbeat(msg Message) {
//	peer, found := state.peers[msg.id]
	peer := state.findPeer(msg.id)

	if peer.IsNothing() {
		log.Printf("Got heartbeat from unknown node: %d", msg.id)
		return
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
	state.votedFor = Data.Just(state.info.Id)

	state.votesReceived = nil
	state.votesReceived = append(state.votesReceived, state.info.Id)
//	lastTerm := 0

	// if log.length > 0 then lastTerm := log[log.length - 1].term; end if

	msg := pb.VoteRequest {
		Id: uint32(state.info.Id),
		CurrentTerm: state.currentTerm,
		LogLength: 0,
		LastTerm: 0,
	}

	for _, node := range state.peers {
		_, err := node.client.Vote(context.Background(), &msg)

		if err != nil {
			log.Printf("Error when responding: %s", err.Error())
		}
	}
	state.resetTime()
}

func (state *NodeState) leaderLoop() {

	for {
		if state.currentRole != Leader { continue }

		time.Sleep( 100 * time.Millisecond )

		msg := pb.HeartbeatMsg{Id: uint32(state.info.Id), CurrentTerm: state.currentTerm}
		for _, n := range state.peers {
			n.client.Heartbeat(context.Background(), &msg)
		}
	}
}

func (state *NodeState) Loop() {

	grpcServer := grpc.NewServer()
	pb.RegisterRaftServiceServer(grpcServer, state)

	tcpConnection, err := net.Listen("tcp", state.info.ConnectionAddr)

	if err != nil {
		panic(err.Error())
	}

	go grpcServer.Serve(tcpConnection)
	go state.leaderLoop()

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

func (server *NodeState) Vote(
	ctx context.Context, req *pb.VoteRequest) (*pb.Acknowledgement, error) {

	server.messages <- Message {
		kind: VoteRequestMessage,
		id: NodeId(server.info.Id),
		currentTerm: server.currentTerm,
		logLength: 0,
		lastTerm: 0,
	}

	return &pb.Acknowledgement{}, nil
}

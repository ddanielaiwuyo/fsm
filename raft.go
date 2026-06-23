package main

import (
	"context"
	"fmt"
	"io"
	"log"
	"math/rand/v2"
	"os"
	"sync"
	"sync/atomic"
	"time"
)

type RaftState int

const (
	Leader RaftState = iota
	Follower
	Candidate
)

type RPCKind int

const (
	AppendEntry RPCKind = iota
)

type RPC struct {
	kind    RPCKind
	payload any
	reply   chan RPCReply
}

type RPCReply struct {
	kind    RPCKind
	payload any
}

type Raft struct {
	id    string
	mu    sync.RWMutex
	state RaftState

	serverAddr string
	server     *Server

	// IPAddr of other nodes in a cluster
	peers []string

	// currentTerm this node is perceived to be in
	term atomic.Uint64

	// max amount of time before a node in the [Follower] state can go
	// before transitioning into a [Leader]
	electionTimeout time.Duration

	// rpcRequests are forwarded from the server for the node to process
	incoming chan RPC

	// used by RaftStates to communicate what [RaftState] node should go into
	// after their exit or preconditions are met
	transition chan RaftState

	// each raft state will be cancelled via this context. After each cancel
	// a new ctx is created
	stateCtx context.Context
	// cancels the goroutine state of a [RaftState]
	stateCtxCancel context.CancelFunc

	// TODO: this is meant for debug purposes and will probably be enforced later
	// on to be able to determine what states are running in a sep goroutine actively.
	_inflight atomic.Uint64
	log       *log.Logger
}

const (
	bufferChanSize = 1
)

func NewRaft(
	id string,
	addr string,
	peers []string,
	electionTimeout time.Duration,
	output io.Writer,
) *Raft {
	incoming := make(chan RPC, bufferChanSize)
	// we only want one state transition to happen at a time
	transition := make(chan RaftState)
	server := NewServer(incoming, nil)

	var l *log.Logger
	if output == nil {
		l = log.New(os.Stdout, "(raft) ", log.Default().Flags())
	} else {
		l = log.New(output, "(raft) ", log.Default().Flags())
	}

	return &Raft{
		id:              id,
		mu:              sync.RWMutex{},
		state:           Follower,
		serverAddr:      addr,
		server:          server,
		peers:           peers,
		term:            atomic.Uint64{},
		electionTimeout: electionTimeout,
		incoming:        incoming,
		transition:      transition,
		_inflight:       atomic.Uint64{},
		log:             l,
	}
}

func (r *Raft) Run(parentCtx context.Context) {
	errCh := make(chan error)
	ctx, cancel := context.WithCancel(parentCtx)
	defer cancel()

	go func() {
		if err := r.server.Listen(ctx, r.serverAddr); err != nil {
			log.Println("(node) error occured while starting the sever")
			errCh <- err
		}
	}()

	stateCtx, stateCtxCancel := context.WithCancel(parentCtx)
	// used to terminate states that might be running in the seperate routines
	r.stateCtx = stateCtx
	r.stateCtxCancel = stateCtxCancel
	defer stateCtxCancel()

	r.log.Println("starting raft node: ", r.Diagnostics())
	go func() {
		r.startFollower()
	}()

	for {
		select {
		case <-parentCtx.Done():
			return
		case err := <-errCh:
			r.log.Println("error from server: ", err)
			return

		case raftState := <-r.transition:
			r.stateCtxCancel()
			r.newStateContext(parentCtx)

			switch raftState {
			case Leader:
				r.log.Println("received transition request to Leader from: ", r.getCurrentState())
				if r.getCurrentState() == Leader {
					r.log.Panicf("currently a Leader and recvd Leader transition currState: %s to: %s\n",
						raftState.String(), r.getCurrentState().String())
				}
				r.electionTimeout = randomTimeout(time.Millisecond)
				r.updateRaftState(raftState)
				r.log.Println("updated state to Leader: ", r.Diagnostics())

				go r.startLeader()
			case Follower:
				r.log.Println("received transition request to Follower from: ", r.getCurrentState())
				if r.getCurrentState() == Follower {
					r.log.Panicf("currently a Follower and recvd Follower transition currState: %s to: %s\n",
						raftState.String(), r.getCurrentState().String())
				}

				r.electionTimeout = randomTimeout(time.Millisecond)
				r.updateRaftState(raftState)
				go r.startFollower()

			case Candidate:
				r.log.Println("received transition request to Candidate from: ", r.getCurrentState().String())
				if r.getCurrentState() == Candidate {
					r.log.Panicf("currently a Candidate and recvd Candidate transition currState: %s to: %s\n",
						raftState.String(), r.getCurrentState().String())
				}

				panic("Candidate state not implemented yet!")
				// go r.startCandidate()
			}
		}
	}
}

func randomTimeout(d time.Duration) time.Duration {
	minInterval, maxInterval := 100, 500
	n := rand.IntN(maxInterval-minInterval) + minInterval

	return d * time.Duration(n)
}

func (rs RaftState) String() string {
	switch rs {
	case Candidate:
		return "Candidate"
	case Follower:
		return "Follower"
	case Leader:
		return "Leader"
	default:
		return fmt.Sprintf("unexpected main.RaftState: %#v", rs)
	}
}

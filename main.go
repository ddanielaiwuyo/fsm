package main

import "sync"

type RaftState int

const (
	Leader RaftState = iota
	Follower
	Candidate
)

type Term struct {
}

type Raft struct {
	mu    sync.Mutex
	state RaftState
	term  Term
}

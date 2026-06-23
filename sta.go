package main

import (
	"context"
	"time"
)

type Node struct {
	state      RaftState
	rpcChan    chan RPC
	transition chan RaftState
	statCtx    context.Context
	statCancel context.CancelFunc
}

func (n *Node) stat(ctx context.Context) {
  defer n.statCancel()
	for {
		select {
		case <-ctx.Done():
      return
		case s := <-n.transition:
			n.state = s
			switch s {
			case Leader:
				n.leader_stat()
			case Follower:
				n.follower_stat()
			case Candidate:
				n.candiate_stat()
			}

		}
	}
}
func (n *Node) leader_stat() {}

func (n *Node) follower_stat() {
	timer := time.NewTimer(500 * time.Millisecond)
	resetTimer := func(dur time.Duration) {
		if !timer.Stop() {
			<-timer.C
		}

		timer.Reset(dur)
	}

	defer timer.Stop()
	for {
		select {
		case <-timer.C:
			n.transition <- Candidate
			return
		case <-n.statCtx.Done():
			return
			// each states can all use the rpcChan
		case rpc := <-n.rpcChan:
			if rpc.kind == AppendEntry {
				resetTimer(500 * time.Millisecond)
				continue
			} else {
				// while a follower, anything other than AppendEntry should be dropped
				// if its' an outside request from the cluster, forward it to the leader
			}
		}
	}
}
func (n *Node) candiate_stat() {}

package main

import (
	"fmt"
	"time"
)

func (n *Node) runFollower() {
	ticker := time.NewTicker(n.raft.electionTimeout)
	defer ticker.Stop()

	fmt.Println("")

	for {
		select {
		case <-n.stateCtx.Done():
			return
		case <-ticker.C:
			fmt.Println("did not recv heartbeat from leader")
			n.transition <- Candidate
			return
		case req := <-n.incoming:
			fmt.Printf("recvd rpc, reseting timer %+v\n", req)
			ticker.Reset(n.raft.electionTimeout)
		}
	}

}

// for when the custom logger has been impl
func (n *Node) inheritLogger(prefix string) {}

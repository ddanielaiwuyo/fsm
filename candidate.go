package main

import (
	"fmt"
	"time"
)

func (n *Node) runCandidate() {
	fmt.Println("candidate state succesfully initiated")
	timer := time.NewTimer(n.raft.electionTimeout)
	defer func() {
		if !timer.Stop() {
			<-timer.C
		}

		fmt.Println("candidate mode terminated succesfully ")
	}()

	select {
	case <-timer.C:
		fmt.Println("election timer fired dropping back to Follower")
		n.transition <- Follower
		return
	case <-n.stateCtx.Done():
		return
	}

}

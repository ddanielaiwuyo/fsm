package main

import (
	"context"
	"log"
	"time"
)

// runFollower starts the Node in a [Follower] state.
// It listens for updates on the [Raft.heartbeat] channel and also an internal
// ticker, that fires based of [Raft.electionTimeout]. If the electionTimeout
// fires, this function returns, and updates the state of this node to
// a [Candidate], otherwise it keeps running until it's ctx is cancelled
func (r *Raft) runFollower() {
	r.activeState.Store(1)
	timer := time.NewTimer(r.electionTimeout)
	log.Println("(follower) becoming a follower: ", r.Diagnostics())
	defer func() {
		if !timer.Stop() {
			go func() {
				<-timer.C
			}()
		}
		r.activeState.Store(0)
    r.runCandidate()
	}()

	for {
		select {
		case <-r.heartbeat:
			log.Println("(follower) heartbeat met: ", time.Now())
			if !timer.Stop() {
				<-timer.C
			}
			timer.Reset(r.electionTimeout)
			log.Println("(follower) reseting timeout: ", r.electionTimeout)

		case <-r.stateCtx.Done():
			log.Println("(follower) myState has been cancel, outting")
			return
		case <-timer.C:
			log.Printf("(follower) timeout elasped, becoming candidate after %s, at %s\n", r.electionTimeout, time.Now())
			r.updateRaftState(Candidate)
			return
		default:
		}
	}

}

func (r *Raft) runCandidate() {
	// log.Println("running for candidate")
	// newTimeout := generateRandomTimeout(time.Millisecond)
	// r.electionTimeout = newTimeout
	//
	// newTerm := r.incrementTerm()
	// log.Println("going all out for a newTerm: ", newTerm)
	// // STUB to transition into leader
	// if seed := generateRandomTimeout(time.Millisecond).Milliseconds(); seed%2 == 0 {
	// 	r.updateRaftState(Leader)
	// } else {
	// 	r.updateRaftState(Follower)
	// }

	log.Println("(candidate) running  for candidate: ", r.Diagnostics())
	r.activeState.Store(1)
	timer := time.NewTimer(generateRandomTimeout(time.Millisecond))
	defer func() {
		r.activeState.Store(0)
		if !timer.Stop() {
			go func() { <-timer.C }()
		}
		log.Println("(candidate) done with cleanup")
	}()

	select {
	case <-timer.C:
		log.Println("(candidate) timer fired, changing state of node ")
		if seed := generateRandomTimeout(time.Millisecond).Milliseconds(); seed%2 == 0 {
			r.updateRaftState(Leader)
		} else {
			r.updateRaftState(Follower)
		}
	case <-r.stateCtx.Done():
		log.Println("(candidate) exiting state forcefully")
		return

	}

}

func (r *Raft) runLeader(ctx context.Context) {
	log.Println("running as leader")
	time.Sleep(1 * time.Second)
}

// Updates the current state of this node
func (r *Raft) updateRaftState(state RaftState) {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.state = state
	r.recentChange.Store(true)
}

// getCurrentState returns the current state of the Node
func (r *Raft) getCurrentState() RaftState {
	r.mu.RLock()
	defer r.mu.RUnlock()
	currentState := r.state
	return currentState
}

func (r *Raft) incrementTerm() int {
	return int(r.term.Add(1))
}

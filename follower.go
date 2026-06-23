package main

import (
	"fmt"
	"time"
)

// runFollower has an internal timer that goes off on [Raft.electionTimeout]
// When the timer fire, it transists into a [Candidate] state. If an [AppendEntry] rpc
// or `heartbeat` arrives before the timer fires, the node remains in this state 
// and resets it's internal timer. If it receives an [AppendEntry] rpcs from another
// nodes whose term is higher it updates this nodes term to the rpc provided in the request
func (r *Raft) runFollower(opts *Opts) {
	var o *Opts
	if opts == nil {
		o = defaultOpts()
	} else {
		o = opts
	}

	o.log.SetPrefix(fmt.Sprintf("(%s:follower) ", r.id))
	timer := time.NewTimer(r.electionTimeout)
	defer func() {
		if !timer.Stop() {
			go func() { <-timer.C }()
		}
		o.log.Println("exiting state ")
	}()

	resetTimer := func() {
		if !timer.Stop() {
			<-timer.C
		}
		timer.Reset(r.electionTimeout)
	}

	for {
		select {
		case <-r.stateCtx.Done():
			return
		case rpc := <-r.incoming:
			switch rpc.kind {
			case AppendEntry:
				payload, ok := rpc.payload.(AppendEntryReq)
				if !ok {
					o.log.Panicf("expected appendEntry from payload, recvd: %+v\n", payload)
				}

				if transit := r.handleAppendEntryRPC(o, payload, rpc.reply); transit {
					resetTimer()
					r.term.Store(payload.Term)
					o.log.Println("updated term info and reset timer")
					continue
				}

			default:
				rpc.reply <- RPCReply{
					kind: AppendEntry,
					payload: &AppendEntryRes{
						Id:           r.id,
						Term:         r.term.Load(),
						Data:         "I dont understand this rpc call yet",
						Acknowledged: false,
					},
				}
				o.log.Printf("rpcRequest not understood: %+v\n", rpc)
			}

		case <-timer.C:
			r.transition <- Candidate
			o.log.Println("timeout reached without hearbeat, tranisitioning to Candidate ")
			return
		}
	}
}

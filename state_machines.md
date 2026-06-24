Finite State Machine FSM
---
A Raft Node is an example of a FSM. It has three possible states: 
- Leader
- Candidate
- Follower


State Transitions
---
Each State has a finite set of valid transition states. If these invariants aren't 
upheld, it's best to panic, crash and restart.


#### Leader -> Candidate
A node in Leader can only move to a Candidate state
> invariants -> Candidate
---
1. Recvs a higher term AppendEntryRPC

#### Follower -> Candidate
A node in Follower can only move to a Candidate state

> invariants -> Candidate
---
1. No AppendEntryRPC (heartbeat) from Leader


#### Candidate -> Follower, Candidate -> Leader
A node in Candidate can move to a Leader or Follower

> invariants -> Leader
---
1. Wins an election


> invariants -> Follower
---
1. Loses an election
2. Recvs a higher term AppendEntryRPC
3. Electoral process timedout



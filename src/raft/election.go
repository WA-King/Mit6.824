package raft

//
// this is an outline of the API that raft must expose to
// the service (or tester). see comments below for
// each of these functions for more details.
//
// rf = Make(...)
//   create a new Raft server.
// rf.Start(command interface{}) (index, term, isleader)
//   start agreement on a new log entry
// rf.GetState() (term, isLeader)
//   ask a Raft for its current term, and whether it thinks it is leader
// ApplyMsg
//   each time a new entry is committed to the log, each Raft peer
//   should send an ApplyMsg to the service (or tester)
//   in the same server.
//

import (
	"time"
)

//
// example RequestVote RPC arguments structure.
// field names must start with capital letters!
//
type RequestVoteArgs struct {
	// Your data here (2A, 2B).
	Term         int
	CanidateId   int
	LastLogIndex int
	LastLogTerm  int
	PreVote      bool
}

//
// example RequestVote RPC reply structure.
// field names must start with capital letters!
//
type RequestVoteReply struct {
	// Your data here (2A).
	Term        int
	VoteGranted bool
}

type MyReply struct {
	reply RequestVoteReply
	ok    bool
}

func (rf *Raft) sendVote(id int, args RequestVoteArgs, ch chan MyReply, stopch chan struct{}) {
	reply := RequestVoteReply{}
	ok := rf.sendRequestVote(id, &args, &reply)
	select {
	case <-stopch:
		return
	default:
		ch <- MyReply{reply: reply, ok: ok}
	}
}

func (rf *Raft) goVote(id int, args RequestVoteArgs, timeout time.Time) (RequestVoteReply, bool) {
	reply := MyReply{}
	ch := make(chan MyReply, 15)
	stopch := make(chan struct{})
	send_cnt := 10
	for send_cnt > 0 {
		send_cnt--
		go rf.sendVote(id, args, ch, stopch)
		time.Sleep(25 * time.Millisecond)
		select {
		case reply = <-ch:
			close(stopch)
			return reply.reply, reply.ok
		default:
			continue
		}
	}

	return RequestVoteReply{}, false
}

//
// example RequestVote RPC handler.
//
func (rf *Raft) RequestVote(args *RequestVoteArgs, reply *RequestVoteReply) {
	// Your code here (2A, 2B).
	rf.mu.Lock()
	defer rf.mu.Unlock()
	if args.PreVote {
		lastLogIndex := rf.LogLength()
		lastLogTerm := rf.LogTerm(lastLogIndex)
		reply.VoteGranted = false
		if args.Term > rf.currentTerm && rf.state == PreCanidate && (args.LastLogTerm > lastLogTerm || (args.LastLogTerm == lastLogTerm && args.LastLogIndex >= lastLogIndex)) {
			reply.VoteGranted = true
		}
		return
	}
	needPersist := false
	reply.Term = rf.currentTerm
	if args.Term < rf.currentTerm {
		reply.VoteGranted = false
	} else {
		if args.Term > rf.currentTerm {
			rf.currentTerm = args.Term
			rf.votedFor = -1
			rf.state = Follower
			rf.InitTimeout()
			needPersist = true
		}
		lastLogIndex := rf.LogLength()
		lastLogTerm := rf.LogTerm(lastLogIndex)
		if (rf.votedFor == -1 || rf.votedFor == args.CanidateId) && (args.LastLogTerm > lastLogTerm || (args.LastLogTerm == lastLogTerm && args.LastLogIndex >= lastLogIndex)) {
			DPrintf("id:%v term:%v votedFor:%v index&Term:%v %v canidate:%v %v\n", rf.me, rf.currentTerm, args.CanidateId, lastLogIndex, lastLogTerm, args.LastLogIndex, args.LastLogTerm)
			rf.InitTimeout()
			if rf.votedFor == -1 {
				needPersist = true
			}
			rf.votedFor = args.CanidateId
			reply.VoteGranted = true
		} else {
			reply.VoteGranted = false
		}
	}
	if needPersist {
		rf.persist(false)
	}
}

//
// example code to send a RequestVote RPC to a server.
// server is the index of the target server in rf.peers[].
// expects RPC arguments in args.
// fills in *reply with RPC reply, so caller should
// pass &reply.
// the types of the args and reply passed to Call() must be
// the same as the types of the arguments declared in the
// handler function (including whether they are pointers).
//
// The labrpc package simulates a lossy network, in which servers
// may be unreachable, and in which requests and replies may be lost.
// Call() sends a request and waits for a reply. If a reply arrives
// within a timeout interval, Call() returns true; otherwise
// Call() returns false. Thus Call() may not return for a while.
// A false return can be caused by a dead server, a live server that
// can't be reached, a lost request, or a lost reply.
//
// Call() is guaranteed to return (perhaps after a delay) *except* if the
// handler function on the server side does not return.  Thus there
// is no need to implement your own timeouts around Call().
//
// look at the comments in ../labrpc/labrpc.go for more details.
//
// if you're having trouble getting RPC to work, check that you've
// capitalized all field names in structs passed over RPC, and
// that the caller passes the address of the reply struct with &, not
// the struct itself.
//
func (rf *Raft) sendRequestVote(server int, args *RequestVoteArgs, reply *RequestVoteReply) bool {
	ok := rf.peers[server].Call("Raft.RequestVote", args, reply)
	return ok
}

func (rf *Raft) beginPreElection(tmpTerm int) {
	rf.mu.Lock()
	if rf.currentTerm != tmpTerm && rf.state == Follower {
		rf.mu.Unlock()
		return
	}
	rf.InitTimeout()
	tmpTerm++
	rf.state = PreCanidate
	rf.voteCount = 1
	timeout := rf.lastHeartBeat.Add(rf.timeout)
	rf.mu.Unlock()
	for i, up := 0, len(rf.peers); i < up; i++ {
		if i != rf.me {
			go rf.getVote(i, tmpTerm, true, timeout)
		}
	}
	for rf.killed() == false {
		rf.mu.Lock()
		nowTerm := rf.currentTerm
		nowState := rf.state
		votes := rf.voteCount
		rf.mu.Unlock()
		if tmpTerm != nowTerm+1 || nowState != PreCanidate || time.Now().After(timeout) {
			break
		}
		if votes*2 > len(rf.peers) {
			DPrintf("id:%v term:%v election\n", rf.me, nowTerm)
			rf.beginElection(nowTerm)
			break
		}
		time.Sleep(10 * time.Millisecond)
	}
}

func (rf *Raft) beginElection(tmpTerm int) {
	rf.mu.Lock()
	if rf.currentTerm != tmpTerm && rf.state == PreCanidate {
		rf.mu.Unlock()
		return
	}
	rf.InitTimeout()
	rf.state = Canidate
	rf.currentTerm++
	tmpTerm++
	rf.votedFor = rf.me
	rf.voteCount = 1
	timeout := rf.lastHeartBeat.Add(rf.timeout)
	rf.persist(false)
	rf.mu.Unlock()
	for i, up := 0, len(rf.peers); i < up; i++ {
		if i != rf.me {
			go rf.getVote(i, tmpTerm, false, timeout)
		}
	}
	for rf.killed() == false {
		rf.mu.Lock()
		nowTerm := rf.currentTerm
		nowState := rf.state
		votes := rf.voteCount
		rf.mu.Unlock()
		if tmpTerm != nowTerm || nowState != Canidate || time.Now().After(timeout) {
			break
		}
		if votes*2 > len(rf.peers) {
			rf.mu.Lock()
			DPrintf("Term:%v BecomeLeader:%v\n", rf.currentTerm, rf.me)
			rf.state = Leader
			rf.nextIndex = make([]int, len(rf.peers))
			rf.matchIndex = make([]int, len(rf.peers))
			rf.updateIndex = make([]bool, len(rf.peers))
			for i := 0; i < len(rf.peers); i++ {
				if i == rf.me {
					continue
				}
				rf.nextIndex[i] = rf.LogLength() + 1
				rf.matchIndex[i] = 0
				rf.updateIndex[i] = true
				go rf.talkToFollower(i, tmpTerm)
				go rf.updateMatch(i, tmpTerm)
			}
			rf.mu.Unlock()
			go rf.beginHeartBeat(tmpTerm)
			go rf.updateCommitIndex(tmpTerm)
			break
		}
		time.Sleep(10 * time.Millisecond)
	}
}

func (rf *Raft) getVote(id int, tmpTerm int, preVote bool, timeout time.Time) {
	rf.mu.Lock()
	args := RequestVoteArgs{}
	args.CanidateId = rf.me
	args.Term = tmpTerm
	args.LastLogIndex = rf.LogLength()
	args.LastLogTerm = rf.LogTerm(args.LastLogIndex)
	args.PreVote = preVote
	tmpState := rf.state
	if preVote {
		if tmpState != PreCanidate || rf.currentTerm+1 != tmpTerm {
			rf.mu.Unlock()
			return
		}
		rf.mu.Unlock()
		reply, ok := rf.goVote(id, args, timeout)
		if ok {
			rf.mu.Lock()
			if time.Now().After(timeout) {
				rf.mu.Unlock()
				return
			}
			if tmpTerm == rf.currentTerm+1 && rf.state == PreCanidate && reply.VoteGranted {
				rf.voteCount++
			}
			rf.mu.Unlock()
		}
		return
	}

	if tmpState != Canidate || rf.currentTerm != tmpTerm {
		rf.mu.Unlock()
		return
	}
	rf.mu.Unlock()
	reply, ok := rf.goVote(id, args, timeout)
	if ok {
		if time.Now().After(timeout) {
			return
		}
		rf.mu.Lock()
		if reply.Term > rf.currentTerm {
			rf.currentTerm = reply.Term
			rf.state = Follower
			rf.votedFor = -1
			rf.persist(false)
			rf.InitTimeout()
		}
		if tmpTerm == rf.currentTerm && reply.VoteGranted && rf.state == Canidate {
			DPrintf("Canidate:%v Term:%v getvote from:%v\n", rf.me, rf.currentTerm, id)
			rf.voteCount++
		}
		rf.mu.Unlock()
	}
}

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
	"bytes"
	"fmt"
	"math/rand"
	"sync"
	"sync/atomic"
	"time"

	"6.824/labgob"
	"6.824/labrpc"
)

type State int

const (
	Follower State = iota
	PreCanidate
	Canidate
	Leader
)

type MyData struct {
	Term              int
	Vote              int
	LastIncludedIndex int
	LastIncludedTerm  int
	Log               []LogEntry
}

type LogEntry struct {
	Term    int
	Command interface{}
}

//
// as each Raft peer becomes aware that successive log entries are
// committed, the peer should send an ApplyMsg to the service (or
// tester) on the same server, via the applyCh passed to Make(). set
// CommandValid to true to indicate that the ApplyMsg contains a newly
// committed log entry.
//
// in part 2D you'll want to send other kinds of messages (e.g.,
// snapshots) on the applyCh, but set CommandValid to false for these
// other uses.
//
type ApplyMsg struct {
	CommandValid bool
	Command      interface{}
	CommandIndex int

	// For 2D:
	SnapshotValid bool
	Snapshot      []byte
	SnapshotTerm  int
	SnapshotIndex int
}

//
// A Go object implementing a single Raft peer.
//
type Raft struct {
	mu        sync.Mutex          // Lock to protect shared access to this peer's state
	peers     []*labrpc.ClientEnd // RPC end points of all peers
	persister *Persister          // Object to hold this peer's persisted state
	me        int                 // this peer's index into peers[]
	dead      int32               // set by Kill()

	// Your data here (2A, 2B, 2C).
	// Look at the paper's Figure 2 for a description of what
	// state a Raft server must maintain.
	// 2A code
	currentTerm   int
	votedFor      int
	commitIndex   int
	lastApplied   int
	voteCount     int
	state         State
	log           []LogEntry
	lastHeartBeat time.Time
	timeout       time.Duration
	// 2B code
	nextIndex     []int
	matchIndex    []int
	updateIndex   []bool
	channelClosed bool
	msgChannel    chan ApplyMsg
	//2D code
	snapshot          []byte
	lastIncludedIndex int
	lastIncludedTerm  int
}

type AppendEntriesArgs struct {
	Term         int
	LeaderId     int
	PrevLogIndex int
	PrevLogTerm  int
	Entries      []LogEntry
	LeaderCommit int
}

type AppendEntriesReply struct {
	Term          int
	Success       bool
	Inconsistency bool
	PerTermIndex  int
	PerTerm       int
	Lagging       bool
}

func (rf *Raft) BuildAE(l int, r int) AppendEntriesArgs {
	args := AppendEntriesArgs{}
	args.Term = rf.currentTerm
	args.LeaderId = rf.me
	args.PrevLogIndex = l
	args.PrevLogTerm = rf.LogTerm(l)
	if l == r {
		args.Entries = make([]LogEntry, 0)
	} else {
		args.Entries = make([]LogEntry, r-l)
		copy(args.Entries, rf.SubLog(l, r))
	}
	args.LeaderCommit = rf.commitIndex
	return args
}

// lock by father
func (rf *Raft) InitTimeout() {
	rf.lastHeartBeat = time.Now()
	rf.timeout = time.Duration(350+rand.Intn(200)) * time.Millisecond

}

// return currentTerm and whether this server
// believes it is the leader.
func (rf *Raft) GetState() (int, bool) {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	var term int
	var isleader bool
	// Your code here (2A).
	term = rf.currentTerm
	isleader = (rf.state == Leader)
	return term, isleader
}

//
// save Raft's persistent state to stable storage,
// where it can later be retrieved after a crash and restart.
// see paper's Figure 2 for a description of what should be persistent.
//
func (rf *Raft) persist(f bool) {
	// Your code here (2C).
	// Example:
	// w := new(bytes.Buffer)
	// e := labgob.NewEncoder(w)
	// e.Encode(rf.xxx)
	// e.Encode(rf.yyy)
	// data := w.Bytes()
	// rf.persister.SaveRaftState(data)
	w := new(bytes.Buffer)
	e := labgob.NewEncoder(w)
	mydata := MyData{}
	mydata.Term = rf.currentTerm
	mydata.Vote = rf.votedFor
	mydata.Log = make([]LogEntry, len(rf.log))
	mydata.LastIncludedIndex = rf.lastIncludedIndex
	mydata.LastIncludedTerm = rf.lastIncludedTerm
	copy(mydata.Log, rf.log)
	e.Encode(mydata)
	data := w.Bytes()
	if f == false {
		rf.persister.SaveRaftState(data)
	} else {
		snapshot := make([]byte, len(rf.snapshot))
		copy(snapshot, rf.snapshot)
		rf.persister.SaveStateAndSnapshot(data, snapshot)
	}
}

//
// restore previously persisted state.
//
func (rf *Raft) readPersist(data []byte) {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	if data == nil || len(data) < 1 { // bootstrap without any state?
		return
	}
	// Your code here (2C).
	// Example:
	// r := bytes.NewBuffer(data)
	// d := labgob.NewDecoder(r)
	// var xxx
	// var yyy
	// if d.Decode(&xxx) != nil ||
	//    d.Decode(&yyy) != nil {
	//   error...
	// } else {
	//   rf.xxx = xxx
	//   rf.yyy = yyy
	// }
	r := bytes.NewBuffer(data)
	d := labgob.NewDecoder(r)
	mydata := MyData{}
	if d.Decode(&mydata) != nil {
		fmt.Printf("touxi\n")
	} else {
		//fmt.Printf("data:%v %v %v\n", mydata.Term, mydata.Vote, len(rf.log))
		rf.currentTerm = mydata.Term
		rf.votedFor = mydata.Vote
		rf.lastIncludedIndex = mydata.LastIncludedIndex
		rf.lastIncludedTerm = mydata.LastIncludedTerm
		rf.log = make([]LogEntry, len(mydata.Log))
		copy(rf.log, mydata.Log)
	}
}

func (rf *Raft) GetStateSize() int {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	return rf.persister.RaftStateSize()
}

//
// the service using Raft (e.g. a k/v server) wants to start
// agreement on the next command to be appended to Raft's log. if this
// server isn't the leader, returns false. otherwise start the
// agreement and return immediately. there is no guarantee that this
// command will ever be committed to the Raft log, since the leader
// may fail or lose an election. even if the Raft instance has been killed,
// this function should return gracefully.
//
// the first return value is the index that the command will appear at
// if it's ever committed. the second return value is the current
// term. the third return value is true if this server believes it is
// the leader.
//
func (rf *Raft) Start(command interface{}) (int, int, bool) {
	index := -1
	term := -1
	isLeader := true

	// Your code here (2B).
	rf.mu.Lock()
	defer rf.mu.Unlock()
	if rf.state != Leader {
		isLeader = false
	} else {
		isLeader = true
		term = rf.currentTerm
		rf.log = append(rf.log, LogEntry{Command: command, Term: rf.currentTerm})
		index = len(rf.log) + rf.lastIncludedIndex
		rf.persist(false)
		DPrintf("Start: Leader:%v Term:%v Index:%v Command:%v\n", rf.me, term, index, command)
	}
	return index, term, isLeader
}

//
// the tester doesn't halt goroutines created by Raft after each test,
// but it does call the Kill() method. your code can use killed() to
// check whether Kill() has been called. the use of atomic avoids the
// need for a lock.
//
// the issue is that long-running goroutines use memory and may chew
// up CPU time, perhaps causing later tests to fail and generating
// confusing debug output. any goroutine with a long-running loop
// should call killed() to check whether it should stop.
//
func (rf *Raft) Kill() {
	atomic.StoreInt32(&rf.dead, 1)
	// Your code here, if desired.
	rf.mu.Lock()
	defer rf.mu.Unlock()
	close(rf.msgChannel)
	rf.channelClosed = true
}

func (rf *Raft) killed() bool {
	z := atomic.LoadInt32(&rf.dead)
	return z == 1
}

// The ticker go routine starts a new election if this peer hasn't received
// heartsbeats recently.
func (rf *Raft) ticker() {
	for rf.killed() == false {
		// Your code here to check if a leader election should
		// be started and to randomize sleeping time using
		// time.Sleep().
		rf.mu.Lock()
		currentState := rf.state
		timeout := rf.lastHeartBeat.Add(rf.timeout)
		tmpTerm := rf.currentTerm
		rf.mu.Unlock()
		switch currentState {
		case Follower:
			{
				if time.Now().After(timeout) {
					DPrintf("id:%v term:%v startPreElection\n", rf.me, tmpTerm)
					go rf.beginPreElection(tmpTerm)
					time.Sleep(time.Millisecond * 10)
				} else {
					time.Sleep(time.Millisecond * 10)
				}
			}
		case PreCanidate:
			{
				if time.Now().After(timeout) {
					rf.mu.Lock()
					rf.state = Follower
					rf.mu.Unlock()
				} else {
					time.Sleep(time.Millisecond * 10)
				}
			}
		case Canidate:
			{
				if time.Now().After(timeout) {
					rf.mu.Lock()
					rf.state = Follower
					rf.InitTimeout()
					rf.mu.Unlock()
				} else {
					time.Sleep(time.Millisecond * 10)
				}
			}
		case Leader:
			{
				time.Sleep(time.Millisecond * 10)
			}
		}
	}
}

func (rf *Raft) beginHeartBeat(tmpTerm int) {
	rf.mu.Lock()
	if tmpTerm != rf.currentTerm {
		rf.mu.Unlock()
		return
	}
	rf.mu.Unlock()
	for rf.killed() == false {
		rf.mu.Lock()
		nowState := rf.state
		length := len(rf.peers)
		nowTerm := rf.currentTerm
		rf.mu.Unlock()
		if nowTerm == tmpTerm && nowState == Leader {
			for i := 0; i < length; i++ {
				if i == rf.me {
					continue
				}
				go rf.sendHeartBeat(i, tmpTerm)
			}
			time.Sleep(120 * time.Millisecond)
		} else {
			break
		}
	}
}

func (rf *Raft) sendHeartBeat(id int, tmpTerm int) {
	//fmt.Printf("index:%v \n",index)
	rf.mu.Lock()
	index := rf.nextIndex[id] - 1
	rf.mu.Unlock()
	rf.sendLog(id, index, index, true)
}

type MyLogReply struct {
	reply AppendEntriesReply
	ok    bool
}

func (rf *Raft) goSendLog(id int, args AppendEntriesArgs, ch chan MyLogReply, stopch chan struct{}) {
	reply := AppendEntriesReply{}
	ok := rf.peers[id].Call("Raft.AppendEntries", &args, &reply)
	select {
	case <-stopch:
		return
	default:
		ch <- MyLogReply{reply: reply, ok: ok}
	}
}

func (rf *Raft) goLog(id int, args AppendEntriesArgs, retry int) (AppendEntriesReply, bool) {
	reply := MyLogReply{}
	ch := make(chan MyLogReply, 10)
	stopch := make(chan struct{})
	send_cnt := retry
	for send_cnt > 0 {
		rf.mu.Lock()
		nowTerm := rf.currentTerm
		nowState := rf.state
		rf.mu.Unlock()
		if nowTerm != args.Term || nowState != Leader {
			select {
			case <-stopch:
			default:
				ch <- MyLogReply{reply: AppendEntriesReply{}, ok: false}
			}
		}
		send_cnt--
		go rf.goSendLog(id, args, ch, stopch)
		time.Sleep(5 * time.Millisecond)
		select {
		case reply = <-ch:
			close(stopch)
			return reply.reply, reply.ok
		default:
			continue
		}
		time.Sleep(75 * time.Millisecond)
	}

	return AppendEntriesReply{}, false
}

func (rf *Raft) sendLog(id int, l int, r int, isHeartBeat bool) bool {
	rf.mu.Lock()

	tmpState := rf.state
	if l < rf.lastIncludedIndex {
		l = rf.lastIncludedIndex
	}

	if r < rf.lastIncludedIndex {
		r = rf.lastIncludedIndex
	}
	args := rf.BuildAE(l, r)
	f := rf.updateIndex[id]
	rf.mu.Unlock()

	if (f && isHeartBeat == false) || tmpState != Leader {
		return false
	}
	retry := 5
	if isHeartBeat {
		retry = 1
	}
	reply, ok := rf.goLog(id, args, retry)
	rf.mu.Lock()
	if ok {
		if reply.Term > rf.currentTerm {
			rf.currentTerm = reply.Term
			rf.state = Follower
			rf.votedFor = -1
			rf.InitTimeout()
			rf.persist(false)
		}
		if isHeartBeat == false && reply.Inconsistency {
			rf.updateIndex[id] = true
		}
	}
	rf.mu.Unlock()
	if ok == false {
		time.Sleep(500 * time.Millisecond)
	}
	return ok && reply.Success
}

func (rf *Raft) AppendEntries(args *AppendEntriesArgs, reply *AppendEntriesReply) {
	rf.mu.Lock()
	needPersist := false
	if args.Term > rf.currentTerm || (args.Term == rf.currentTerm && rf.state != Follower) {
		if args.Term > rf.currentTerm {
			rf.votedFor = -1
		}
		rf.currentTerm = args.Term
		rf.InitTimeout()
		rf.state = Follower
		needPersist = true
	}
	length := rf.LogLength()
	reply.Term = rf.currentTerm
	reply.Inconsistency = false
	reply.PerTerm = 0
	reply.PerTermIndex = 0
	reply.Lagging = false

	if rf.state != Follower || args.Term < rf.currentTerm {
		reply.Success = false
	} else if length < args.PrevLogIndex || (args.PrevLogIndex > rf.lastIncludedIndex && rf.LogTerm(args.PrevLogIndex) != args.PrevLogTerm) {
		rf.InitTimeout()
		reply.Success = false
		reply.Inconsistency = true

		if length < args.PrevLogIndex {
			reply.PerTermIndex = length
		} else {
			reply.PerTermIndex = args.PrevLogIndex
		}

		reply.PerTerm = rf.LogTerm(reply.PerTermIndex)
		if reply.PerTerm > args.PrevLogTerm {
			reply.PerTerm = args.PrevLogTerm
		}
		reply.Lagging = true
		for i := reply.PerTermIndex; i >= rf.lastIncludedIndex; i-- {
			i_term := rf.LogTerm(i)
			if i_term <= reply.PerTerm {
				reply.PerTermIndex = i
				reply.PerTerm = i_term
				reply.Lagging = false
				break
			}
		}

	} else {
		rf.InitTimeout()
		reply.Success = true
		if args.PrevLogIndex < rf.lastIncludedIndex {
			dropLength := rf.lastIncludedIndex - args.PrevLogIndex
			if dropLength >= len(args.Entries) {
				args.Entries = make([]LogEntry, 0)
			} else {
				args.Entries = args.Entries[dropLength:]
			}
			args.PrevLogIndex = rf.lastIncludedIndex
		}
		lengthArg := len(args.Entries)
		newLength := args.PrevLogIndex + lengthArg
		lengthRf := rf.LogLength()
		if lengthArg != 0 {
			if newLength >= lengthRf {
				rf.log = rf.SubLog(rf.lastIncludedIndex, args.PrevLogIndex)
				rf.log = append(rf.log, args.Entries...)
			} else {
				pre := rf.SubLog(rf.lastIncludedIndex, args.PrevLogIndex)
				nex := rf.SubLog(newLength, len(rf.log)+rf.lastIncludedIndex)
				rf.log = append(pre, args.Entries...)
				if nex[0].Term == args.Term {
					rf.log = append(rf.log, nex...)
				}
			}
			needPersist = true
		}
		if rf.commitIndex < args.LeaderCommit {
			if newLength < args.LeaderCommit {
				rf.commitIndex = newLength
			} else {
				rf.commitIndex = args.LeaderCommit
			}
		}
	}
	if needPersist {
		rf.persist(false)
	}
	rf.mu.Unlock()
}

func (rf *Raft) updateCommitIndex(tmpTerm int) {
	rf.mu.Lock()
	commitOk := rf.commitIndex
	rf.mu.Unlock()
	for rf.killed() == false {
		rf.mu.Lock()
		nowState := rf.state
		length := rf.LogLength()
		len_peer := len(rf.peers)
		nowTerm := rf.currentTerm
		if commitOk < rf.lastIncludedIndex {
			commitOk = rf.lastIncludedIndex
			rf.commitIndex = rf.lastIncludedIndex
		}
		rf.mu.Unlock()
		if nowState != Leader || nowTerm != tmpTerm {
			break
		}
		for commitOk < length {
			cnt := 0
			rf.mu.Lock()
			for i := 0; i < len_peer; i++ {
				if i == rf.me {
					cnt++
					continue
				}
				if rf.updateIndex[i] == false && rf.matchIndex[i] >= commitOk+1 {
					cnt++
				}
			}
			rf.mu.Unlock()
			if cnt*2 > len_peer {
				rf.mu.Lock()
				commitOk++
				DPrintf("Id:%v commitok:%v matchindex:%v\n", rf.me, commitOk, rf.matchIndex)
				//fmt.Printf("isLeader:%v currentTerm:%v logTerm:%v\n",(rf.state==Leader),rf.currentTerm,rf.log[commitOk-1].Term)
				//DPrintf("Leaderid:%v commitok:%v commitokTerm %v currentTerm:%v\n",rf.me,commitOk,rf.log[commitOk-1].Term,rf.currentTerm)
				if rf.state == Leader && rf.LogTerm(commitOk) == rf.currentTerm {
					rf.commitIndex = commitOk
					//fmt.Printf("me:%v commitIndex:%v\n",rf.me,rf.commitIndex)
				}
				rf.mu.Unlock()
			} else {
				break
			}
		}
		time.Sleep(time.Millisecond * 10)
	}
}

func (rf *Raft) updateMatch(id int, tmpTerm int) {
	for rf.killed() == false {
		rf.mu.Lock()
		nowTerm := rf.currentTerm
		tmpNext := rf.nextIndex[id] - 1
		if rf.state != Leader || nowTerm != tmpTerm {
			rf.mu.Unlock()
			break
		}
		if rf.updateIndex[id] == false {
			rf.mu.Unlock()
			time.Sleep(10 * time.Millisecond)
			continue
		}
		rf.mu.Unlock()

		for true {
			rf.mu.Lock()
			nowTerm := rf.currentTerm
			if rf.updateIndex[id] == false || rf.state != Leader || nowTerm != tmpTerm {
				rf.mu.Unlock()
				break
			}
			if tmpNext < rf.lastIncludedIndex {
				tmpNext = rf.lastIncludedIndex
			}
			args := rf.BuildAE(tmpNext, tmpNext)
			rf.mu.Unlock()
			reply, ok := rf.goLog(id, args, 5)

			if ok {
				rf.mu.Lock()
				if rf.currentTerm < reply.Term {
					rf.currentTerm = reply.Term
					rf.state = Follower
					rf.votedFor = -1
					rf.InitTimeout()
					rf.persist(false)
					rf.mu.Unlock()
					return
				}
				if reply.Success && reply.Inconsistency == false {

					rf.nextIndex[id] = tmpNext + 1
					rf.matchIndex[id] = tmpNext
					rf.updateIndex[id] = false
					DPrintf("Leader:%v id:%v updateMatch:%v \n", rf.me, id, tmpNext)
				} else if reply.Lagging || tmpNext == rf.lastIncludedIndex || reply.PerTermIndex <= rf.lastIncludedIndex {
					DPrintf("Laging\n")
					rf.mu.Unlock()
					ok := rf.sendSnapshot(id)
					rf.mu.Lock()
					if ok {
						rf.matchIndex[id] = rf.lastIncludedIndex
						rf.nextIndex[id] = rf.lastIncludedIndex + 1
						rf.updateIndex[id] = false
					}
				} else if rf.LogTerm(reply.PerTermIndex) == reply.PerTerm && reply.Inconsistency == true {
					rf.nextIndex[id] = reply.PerTermIndex + 1
					rf.matchIndex[id] = reply.PerTermIndex
					rf.updateIndex[id] = false
					DPrintf("Leader:%v id:%v updateMatch:%v \n", rf.me, id, tmpNext)
				} else {
					tmpNext = reply.PerTermIndex
					tpTerm := rf.LogTerm(reply.PerTermIndex)
					if tpTerm > reply.PerTerm {
						tpTerm = reply.PerTerm
					}
					Lagging := true
					for i := tmpNext; i >= rf.lastIncludedIndex; i-- {
						i_term := rf.LogTerm(i)
						if i_term <= tpTerm {
							tmpNext = i
							Lagging = false
							break
						}
					}
					if Lagging {
						rf.mu.Unlock()
						ok := rf.sendSnapshot(id)
						rf.mu.Lock()
						if ok {
							rf.matchIndex[id] = rf.lastIncludedIndex
							rf.nextIndex[id] = rf.lastIncludedIndex + 1
							rf.updateIndex[id] = false
						}
					}
				}
				rf.mu.Unlock()
			} else {
				time.Sleep(500 * time.Millisecond)
			}
		}
	}
}

func (rf *Raft) UpdateLastApply() {
	for rf.killed() == false {
		rf.mu.Lock()
		if rf.lastApplied < rf.lastIncludedIndex {
			rf.lastApplied = rf.lastIncludedIndex
		}
		if rf.lastApplied < rf.commitIndex && rf.channelClosed == false {
			rf.lastApplied++
			msg := ApplyMsg{}
			msg.Command = rf.log[rf.lastApplied-rf.lastIncludedIndex-1].Command
			msg.CommandIndex = rf.lastApplied
			msg.CommandValid = true
			msg.SnapshotValid = false
			DPrintf("Id:%v Term:%v commitmsg:%v\n", rf.me, rf.currentTerm, msg)
			select {
			case rf.msgChannel <- msg:
				rf.mu.Unlock()
			default:
				rf.lastApplied--
				rf.mu.Unlock()
				time.Sleep(5 * time.Millisecond)
			}
		} else {
			rf.mu.Unlock()
			time.Sleep(10 * time.Millisecond)
		}
	}
}

func (rf *Raft) talkToFollower(id int, tmpTerm int) {
	for rf.killed() == false {
		rf.mu.Lock()
		nowState := rf.state
		nowTerm := rf.currentTerm
		f := rf.updateIndex[id]
		rf.mu.Unlock()
		if nowState != Leader || nowTerm != tmpTerm {
			break
		}
		if f {
			time.Sleep(10 * time.Millisecond)
			continue
		}
		rf.mu.Lock()
		nId := rf.nextIndex[id]
		//DPrintf("Leader:%v id:%v matchindex:%v nextindex:%v\n",rf.me,id,rf.matchIndex[id],rf.nextIndex[id])
		length := rf.LogLength()
		rf.mu.Unlock()
		if nId <= length {
			if rf.sendLog(id, nId-1, length, false) {
				rf.mu.Lock()
				if rf.currentTerm == tmpTerm && rf.state == Leader && length > rf.matchIndex[id] {
					DPrintf("Leader:%v id:%v talktoFollower:%v \n", rf.me, id, length)
					rf.matchIndex[id] = length
					rf.nextIndex[id] = rf.matchIndex[id] + 1
				}
				//fmt.Printf("me%v id:%v match:%v\n",rf.me,id,rf.matchIndex[id])
				rf.mu.Unlock()
			}
		}
		//fmt.Printf("nId:%v len_log:%v\n",nId,length)
		time.Sleep(time.Millisecond * 10)
	}
}

//
// the service or tester wants to create a Raft server. the ports
// of all the Raft servers (including this one) are in peers[]. this
// server's port is peers[me]. all the servers' peers[] arrays
// have the same order. persister is a place for this server to
// save its persistent state, and also initially holds the most
// recent saved state, if any. applyCh is a channel on which the
// tester or service expects Raft to send ApplyMsg messages.
// Make() must return quickly, so it should start goroutines
// for any long-running work.
//
func Make(peers []*labrpc.ClientEnd, me int,
	persister *Persister, applyCh chan ApplyMsg) *Raft {
	rf := &Raft{}
	rf.peers = peers
	rf.persister = persister
	rf.me = me

	// Your initialization code here (2A, 2B, 2C).
	rf.currentTerm = 0
	rf.votedFor = -1
	rf.log = make([]LogEntry, 0)
	rf.commitIndex = 0
	rf.lastApplied = 0
	rf.state = Follower
	rf.msgChannel = applyCh
	rf.lastIncludedIndex = 0
	rf.lastIncludedTerm = 0
	rf.InitTimeout()
	// initialize from state persisted before a crash
	rf.readPersist(persister.ReadRaftState())

	// start ticker goroutine to start elections
	go rf.UpdateLastApply()
	go rf.ticker()

	return rf
}

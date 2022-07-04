package raft

import (
    "time"
)
//
// AkJG service wants to switch to snapshot.  Only do so if Raft hasn't
// have more recent info since it communicate the snapshot on applyCh.
//
func (rf *Raft) CondInstallSnapshot(lastIncludedTerm int, lastIncludedIndex int, snapshot []byte) bool {

	// Your code here (2D).

	return true
}

// the service says it has created a snapshot that has
// all info up to and including index. this means the
// service no longer needs the log through (and including)
// that index. Raft should now trim its log as much as possible.
func (rf *Raft) Snapshot(index int, snapshot []byte) {
    rf.mu.Lock()
    if rf.lastIncludedIndex >= index {
        rf.mu.Unlock()
        return 
    }
    //rf.lastIncludedTerm = rf.log[index-rf.lastIncludedIndex-1].Term
    rf.lastIncludedTerm = rf.log[index - rf.lastIncludedIndex - 1].Term
    rf.log = rf.log[index - rf.lastIncludedIndex:]
    rf.lastIncludedIndex = index
    rf.snapshot = make([]byte, len(snapshot))
	copy(rf.snapshot, snapshot)
    if rf.lastApplied < rf.lastIncludedIndex {
        rf.lastApplied = rf.lastIncludedIndex
    }
    tmpState := rf.state
    rf.persist(true)
    DPrintf("Id:%v snapshotindex:%v\n",rf.me,index)
    rf.mu.Unlock()
    if tmpState == Leader {
        for i := 0; i < len(rf.peers); i++ {
            if i== rf.me {
                continue
            }
            go rf.sendSnapshot(i)
        }
    }
}

type InstallSnapshotArgs struct {
    Term             int
    Data             []byte
    LastIncludeIndex int
    LastIncludeTerm  int
}

type InstallSnapshotReply struct {
    Term   int
}

func (rf *Raft) InstallSnapshot(args *InstallSnapshotArgs, reply *InstallSnapshotReply) {
    needPersist := false
    rf.mu.Lock()
    defer rf.mu.Unlock()
    if rf.currentTerm < args.Term || (rf.currentTerm == args.Term && rf.state != Follower ) {
        rf.state = Follower
        if args.Term > rf.currentTerm {
            rf.votedFor = -1
            needPersist = true
        }
        rf.currentTerm = args.Term
        rf.InitTimeout()
    }

    reply.Term = rf.currentTerm
    if rf.lastIncludedIndex < args.LastIncludeIndex {
        //drop log
        dropLength := args.LastIncludeIndex - rf.lastIncludedIndex
        if dropLength < len(rf.log) {
            rf.log = rf.log[dropLength:]
        } else {
            rf.log = make([]LogEntry, 0)
        }

        rf.lastIncludedIndex = args.LastIncludeIndex
        rf.lastIncludedTerm = args.LastIncludeTerm
        rf.snapshot = make([]byte,len(args.Data))
        copy(rf.snapshot,args.Data)
        needPersist = true
        DPrintf("id:%v lastapply:%v Installsnapindex:%v\n",rf.me,rf.lastApplied,args.LastIncludeIndex)
        if rf.lastApplied < rf.lastIncludedIndex && rf.channelClosed == false {
            DPrintf("id:%v Installsnapindex:%v\n",rf.me,rf.lastApplied)
            rf.lastApplied = rf.lastIncludedIndex
            msg := ApplyMsg{}
            msg.CommandValid = false
            msg.SnapshotValid = true
            msg.Snapshot = make([]byte, len(rf.snapshot))
            copy(msg.Snapshot,rf.snapshot)
            msg.SnapshotIndex = rf.lastIncludedIndex
            msg.SnapshotTerm = rf.lastIncludedTerm
            rf.msgChannel <- msg
        }

    }
    if needPersist {
        rf.persist(true)
    }
}

type SnapshotReply struct {
    reply InstallSnapshotReply
    ok    bool
}
func (rf *Raft) sendss(id int, args InstallSnapshotArgs,ch chan SnapshotReply, stopch chan struct{}) {
    reply := InstallSnapshotReply{}
    ok := rf.peers[id].Call("Raft.InstallSnapshot",&args,&reply)
    select {
    case <- stopch :
        return
    default:
        ch <- SnapshotReply{reply: reply,ok: ok}
    }
}

func (rf *Raft) goSnapshot(id int, args InstallSnapshotArgs) (InstallSnapshotReply, bool) {
    reply := SnapshotReply{}
    ch := make(chan SnapshotReply,15)
    stopch := make(chan struct {}) 
    send_cnt := 5
    for send_cnt > 0{
        send_cnt--
        go rf.sendss(id, args, ch, stopch)
        time.Sleep(25*time.Millisecond)
        select {
        case reply = <- ch : 
            close(stopch)   
            return reply.reply, reply.ok
        default:
            continue
        }
    }

    return InstallSnapshotReply{},false
}

func (rf *Raft) sendSnapshot(id int) bool {
    rf.mu.Lock()
    DPrintf("Leader:%v sendSnapshot to:%v snapindex:%v\n",rf.me,id,rf.lastIncludedIndex)
    args := InstallSnapshotArgs{}
    args.Term = rf.currentTerm
    args.LastIncludeIndex = rf.lastIncludedIndex
    args.LastIncludeTerm = rf.lastIncludedTerm
    args.Data = make([]byte, len(rf.snapshot))
    copy(args.Data,rf.snapshot)
    rf.mu.Unlock()
    reply, ok := rf.goSnapshot(id, args)
    if ok {
        rf.mu.Lock()
        if reply.Term > rf.currentTerm {
            rf.currentTerm = reply.Term
            rf.state = Follower
            rf.votedFor = -1
            rf.InitTimeout()
            rf.persist(false)
        }
        rf.mu.Unlock()
    }
    return ok
}

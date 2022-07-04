package kvraft

import (
	"6.824/labgob"
	"6.824/labrpc"
	"6.824/raft"
	"bytes"
	"fmt"
	"log"
	"sync"
	"sync/atomic"
	"time"
)

const Debug = false

func DPrintf(format string, a ...interface{}) (n int, err error) {
	if Debug {
		log.Printf(format, a...)
	}
	return
}

type Op struct {
	// Your definitions here.
	// Field names must start with capital letters,
	// otherwise RPC will break.
	Type      OpType
	Key       string
	Value     string
	ClientId  int64
	SeqNum    int
	Messagech chan string
}

type CommandInfo struct {
	SeqNum int
	Value  string
}

type KVServer struct {
	mu      sync.Mutex
	me      int
	rf      *raft.Raft
	applyCh chan raft.ApplyMsg
	dead    int32 // set by Kill()

	maxraftstate int // snapshot if log grows this big

	// Your definitions here.
	db           datebase
	session      map[int64]CommandInfo
	maxRaftState int
	commandIndex int
}

func (kv *KVServer) Get(args *GetArgs, reply *GetReply) {
	kv.mu.Lock()
	tmp, ok := kv.session[args.ClientId]
	kv.mu.Unlock()
	if ok && tmp.SeqNum == args.SeqNum {
		reply.Value = tmp.Value
		reply.Err = OK
		return
	}
	// Your code here.
	op := Op{}
	op.Type = GET
	op.Key = args.Key
	op.Messagech = make(chan string)
	op.ClientId = args.ClientId
	op.SeqNum = args.SeqNum
	_, _, isLeader := kv.rf.Start(op)
	if isLeader == false {
		reply.Err = ErrWrongLeader
		return
	}
	select {
	case value := <-op.Messagech:
		reply.Value = value
		reply.Err = OK
	case <-time.After(1000 * time.Millisecond):
		reply.Err = ErrTimeOut
	}
}

func (kv *KVServer) PutAppend(args *PutAppendArgs, reply *PutAppendReply) {
	kv.mu.Lock()
	tmp, ok := kv.session[args.ClientId]
	kv.mu.Unlock()
	if ok && tmp.SeqNum == args.SeqNum {
		reply.Err = OK
		return
	}
	op := Op{}
	op.Type = args.Op
	op.Key = args.Key
	op.Value = args.Value
	op.Messagech = make(chan string)
	op.ClientId = args.ClientId
	op.SeqNum = args.SeqNum
	_, _, isLeader := kv.rf.Start(op)
	if isLeader == false {
		reply.Err = ErrWrongLeader
		return
	}
	DPrintf("wait\n")
	select {
	case <-op.Messagech:
		reply.Err = OK
		return
	case <-time.After(1000 * time.Millisecond):
		reply.Err = ErrTimeOut
	}
}
func (kv *KVServer) command(op Op) string {
	tmp, ok := kv.session[op.ClientId]
	DPrintf("id:%v type:%v key:%v value:%v clientId:%v seqNum:%v ok:%v\n", kv.me, op.Type, op.Key, op.Value, op.ClientId, op.SeqNum, ok)
	if ok {
		if tmp.SeqNum == op.SeqNum {
			return tmp.Value
		} else if tmp.SeqNum > op.SeqNum {
			return ""
		}
	}
	value := ""
	switch op.Type {
	case GET:
		tmp, ok := kv.db.Get(op.Key)
		if ok == false {
			value = ""
		} else {
			value = tmp
		}
	case PUT:
		kv.db.Put(op.Key, op.Value)
		value = ""
	case APPEND:
		kv.db.Append(op.Key, op.Value)
		value = ""
	default:
		value = ""
	}
	DPrintf("clientId:%v seqNum:%v value:%v\n", op.ClientId, op.SeqNum, value)
	kv.session[op.ClientId] = CommandInfo{SeqNum: op.SeqNum, Value: value}
	return value
}

func (kv *KVServer) apply() {
	for m := range kv.applyCh {
		if m.CommandValid {
			if op, ok := m.Command.(Op); ok {
				kv.mu.Lock()
				ret := kv.command(op)
				kv.commandIndex = m.CommandIndex
				if op.Messagech != nil {
					select {
					case op.Messagech <- ret:
					default:
					}
				}
				kv.mu.Unlock()
			}
		} else if m.SnapshotValid {
			kv.mu.Lock()
			kv.commandIndex = m.SnapshotIndex
			kv.mu.Unlock()
			kv.InstallSnapshot(m.Snapshot)
		}
	}
}

//
// the tester calls Kill() when a KVServer instance won't
// be needed again. for your convenience, we supply
// code to set rf.dead (without needing a lock),
// and a killed() method to test rf.dead in
// long-running loops. you can also add your own
// code to Kill(). you're not required to do anything
// about this, but it may be convenient (for example)
// to suppress debug output from a Kill()ed instance.
//
func (kv *KVServer) Kill() {
	atomic.StoreInt32(&kv.dead, 1)
	kv.rf.Kill()
	// Your code here, if desired.
}

func (kv *KVServer) killed() bool {
	z := atomic.LoadInt32(&kv.dead)
	return z == 1
}

type KvInfo struct {
	Session  map[int64]CommandInfo
	Datebase map[string]string
}

func (kv *KVServer) GetSnapShot() (int, []byte) {
	kv.mu.Lock()
	defer kv.mu.Unlock()
	w := new(bytes.Buffer)
	e := labgob.NewEncoder(w)
	info := KvInfo{}
	info.Session = kv.session
	info.Datebase = kv.db.data
	e.Encode(info)
	data := w.Bytes()
	return kv.commandIndex, data
}
func (kv *KVServer) InstallSnapshot(data []byte) {
	kv.mu.Lock()
	defer kv.mu.Unlock()
	if data == nil || len(data) < 1 { // bootstrap without any state?
		return
	}
	r := bytes.NewBuffer(data)
	d := labgob.NewDecoder(r)
	mydata := KvInfo{}
	if d.Decode(&mydata) != nil {
		fmt.Printf("touxi\n")
	} else {
		//fmt.Printf("data:%v %v %v\n", mydata.Term, mydata.Vote, len(rf.log))
		kv.session = mydata.Session
		kv.db.data = mydata.Datebase
	}
}
func (kv *KVServer) loopSnapshot() {
	if kv.maxRaftState == -1 {
		return
	}
	for kv.killed() == false {
		if kv.rf.GetStateSize() > kv.maxraftstate/10*9 {
			index, data := kv.GetSnapShot()
			kv.rf.Snapshot(index, data)
		}
		time.Sleep(10 * time.Millisecond)
	}
}

//
// servers[] contains the ports of the set of
// servers that will cooperate via Raft to
// form the fault-tolerant key/value service.
// me is the index of the current server in servers[].
// the k/v server should store snapshots through the underlying Raft
// implementation, which should call persister.SaveStateAndSnapshot() to
// atomically save the Raft state along with the snapshot.
// the k/v server should snapshot when Raft's saved state exceeds maxraftstate bytes,
// in order to allow Raft to garbage-collect its log. if maxraftstate is -1,
// you don't need to snapshot.
// StartKVServer() must return quickly, so it should start goroutines
// for any long-running work.
//
func StartKVServer(servers []*labrpc.ClientEnd, me int, persister *raft.Persister, maxraftstate int) *KVServer {
	// call labgob.Register on structures you want
	// Go's RPC library to marshall/unmarshall.
	labgob.Register(Op{})

	kv := new(KVServer)
	kv.me = me
	kv.maxraftstate = maxraftstate
	kv.db.Init()
	kv.session = make(map[int64]CommandInfo)
	// You may need initialization code here.

	kv.applyCh = make(chan raft.ApplyMsg)
	kv.rf = raft.Make(servers, me, persister, kv.applyCh)
	kv.maxRaftState = maxraftstate
	// You may need initialization code here.
	kv.InstallSnapshot(persister.ReadSnapshot())
	go kv.apply()
	go kv.loopSnapshot()
	return kv
}

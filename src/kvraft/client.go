package kvraft

import "6.824/labrpc"
import "crypto/rand"
import "math/big"

type Clerk struct {
	servers []*labrpc.ClientEnd
	// You will have to modify this struct.
	clientId   int64
	seqNum     int
	lastLeader int
}

func nrand() int64 {
	max := big.NewInt(int64(1) << 62)
	bigx, _ := rand.Int(rand.Reader, max)
	x := bigx.Int64()
	return x
}

func MakeClerk(servers []*labrpc.ClientEnd) *Clerk {
	ck := new(Clerk)
	ck.servers = servers
	// You'll have to add code here.
	ck.clientId = nrand()
	ck.seqNum = 0
	return ck
}

//
// fetch the current value for a key.
// returns "" if the key does not exist.
// keeps trying forever in the face of all other errors.
//
// you can send an RPC with code like this:
// ok := ck.servers[i].Call("KVServer.Get", &args, &reply)
//
// the types of args and reply (including whether they are pointers)
// must match the declared types of the RPC handler function's
// arguments. and reply must be passed as a pointer.
//
func (ck *Clerk) Get(key string) string {

	// You will have to modify this function.
	leader := ck.lastLeader
	args := GetArgs{}
	args.Key = key
	args.ClientId = ck.clientId
	ck.seqNum++
	args.SeqNum = ck.seqNum
	DPrintf("Get key:%v seeNum:%v\n", key, ck.seqNum)
	for {
		if leader == len(ck.servers) {
			leader = 0
		}
		reply := GetReply{}
		ok := ck.servers[leader].Call("KVServer.Get", &args, &reply)
		if ok {
			switch reply.Err {
			case OK:
				ck.lastLeader = leader
				return reply.Value
			case ErrNoKey:
				ck.lastLeader = leader
				return ""
			case ErrTimeOut:
				ck.lastLeader = leader
			case ErrWrongLeader:
				leader++
			}
		} else {
			leader++
		}
	}
}

//
// shared by Put and Append.
//
// you can send an RPC with code like this:
// ok := ck.servers[i].Call("KVServer.PutAppend", &args, &reply)
//
// the types of args and reply (including whether they are pointers)
// must match the declared types of the RPC handler function's
// arguments. and reply must be passed as a pointer.
//
func (ck *Clerk) PutAppend(key string, value string, op OpType) {
	// You will have to modify this function.
	args := PutAppendArgs{}
	args.Key = key
	args.Value = value
	args.Op = op
	args.ClientId = ck.clientId
	ck.seqNum++
	args.SeqNum = ck.seqNum
	leader := ck.lastLeader
	args.Key = key
	DPrintf("PutAppend key:%v value:%v seqNum:%v\n", key, value, ck.seqNum)
	for {
		if leader == len(ck.servers) {
			leader = 0
		}
		reply := PutAppendReply{}
		ok := ck.servers[leader].Call("KVServer.PutAppend", &args, &reply)
		if ok {
			switch reply.Err {
			case OK:
				ck.lastLeader = leader
				return
			case ErrTimeOut:
				ck.lastLeader = leader
			case ErrWrongLeader:
				leader++
			}
		} else {
			leader++
		}
	}
}

func (ck *Clerk) Put(key string, value string) {
	ck.PutAppend(key, value, PUT)
}
func (ck *Clerk) Append(key string, value string) {
	ck.PutAppend(key, value, APPEND)
}

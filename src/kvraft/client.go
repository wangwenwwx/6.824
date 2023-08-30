package kvraft

import (
	"6.5840/labrpc"
	"sync/atomic"
	"time"
)
import "crypto/rand"
import "math/big"

type Clerk struct {
	servers []*labrpc.ClientEnd
	// You will have to modify this struct.
	id          int64
	seq         int32
	serverCount int
	leader      int
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
	ck.id = nrand()
	ck.serverCount = len(servers)
	n, _ := rand.Int(rand.Reader, big.NewInt(int64(ck.serverCount)))
	ck.leader = int(n.Int64())
	ck.seq = 0
	return ck
}

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
func (ck *Clerk) Get(key string) string {

	// You will have to modify this function.
	seq := atomic.AddInt32(&ck.seq, 1)
	args := GetArgs{key, ck.id, seq}
	for {
		//DPrintf("reply:%v", reply)
		reply := GetReply{}
		done := make(chan bool)
		leader := ck.leader
		go ck.sendWithTimeout(func() {
			ck.servers[leader].Call("KVServer.Get", &args, &reply)
		}, done)
		select {
		case <-time.After(1 * time.Second):
		case <-done:
			if reply.Err == OK {
				DPrintf("ck,k:%s,v:%s,seq:%d", key, reply.Value, seq)
				return reply.Value
			}
		}
		ck.changeLeader()
	}
}

// shared by Put and Append.
//
// you can send an RPC with code like this:
// ok := ck.servers[i].Call("KVServer.PutAppend", &args, &reply)
//
// the types of args and reply (including whether they are pointers)
// must match the declared types of the RPC handler function's
// arguments. and reply must be passed as a pointer.
func (ck *Clerk) PutAppend(key string, value string, op string) {
	// You will have to modify this function.
	seq := atomic.AddInt32(&ck.seq, 1)
	args := PutAppendArgs{key, value, op, ck.id, seq}
	for {
		//DPrintf("reply:%v", reply)
		reply := PutAppendReply{}
		done := make(chan bool)
		leader := ck.leader
		go ck.sendWithTimeout(func() {
			ck.servers[leader].Call("KVServer.PutAppend", &args, &reply)
		}, done)
		select {
		case <-time.After(1 * time.Second):
		case <-done:
			if reply.Err == OK {
				DPrintf("leader:%d,k:%s,v:%s,op:%s,seq:%d", leader, key, value, op, seq)
				return
			}
		}
		ck.changeLeader()
	}
}

func (ck *Clerk) Put(key string, value string) {
	ck.PutAppend(key, value, "Put")
}
func (ck *Clerk) Append(key string, value string) {
	ck.PutAppend(key, value, "Append")
}
func (ck *Clerk) changeLeader() {
	leader := (ck.leader + 1) % ck.serverCount
	//DPrintf("clientId:%d,leader:%d", ck.id, leader)
	ck.leader = leader
}
func (ck *Clerk) sendWithTimeout(fn func(), requestDone chan bool) {
	fn()
	requestDone <- true
}

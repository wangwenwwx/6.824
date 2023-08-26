package kvraft

import (
	"6.5840/labgob"
	"6.5840/labrpc"
	"6.5840/raft"
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
	ClientId int64
	Seq      int32
	Op       string
	Key      string
	Value    string
}

type KVServer struct {
	mu      sync.Mutex
	me      int
	rf      *raft.Raft
	applyCh chan raft.ApplyMsg
	dead    int32 // set by Kill()

	maxraftstate int // snapshot if log grows this big

	// Your definitions here.
	lastAppliedIndex int
	lastAppliedOp    Op
	state            map[string]string
	appliedSeq       map[int64]int32
}

func (kv *KVServer) Get(args *GetArgs, reply *GetReply) {
	// Your code here.
	kv.mu.Lock()
	if kv.appliedSeq[args.ClientId] > args.Seq {
		reply.Err = OK
		reply.Value = kv.state[args.Key]
		kv.mu.Unlock()
		return
	}
	op := Op{
		ClientId: args.ClientId,
		Seq:      args.Seq,
		Op:       "Get",
		Key:      args.Key,
	}
	index, term, isLeader := kv.rf.Start(op)
	//DPrintf("raftIndex:%d,args:%+v", index, op)
	//defer DPrintf("get reply:%v", reply)
	if !isLeader {
		reply.Err = ErrWrongLeader
		kv.mu.Unlock()
		return
	}
	for kv.lastAppliedIndex != index {
		kv.mu.Unlock()
		time.Sleep(10 * time.Millisecond)
		kv.mu.Lock()
		DPrintf("term:%d,client:%d,me:%d,raftIndex:%d,lastIndex:%d", term, args.ClientId, kv.me, index, kv.lastAppliedIndex)
	}
	if op.ClientId != kv.lastAppliedOp.ClientId || op.Seq != kv.lastAppliedOp.Seq {
		reply.Err = ErrServerError
	} else {
		reply.Err = OK
		reply.Value = kv.state[args.Key]
	}
	//DPrintf("leader:%d,k:%s,v:%s,seq:%d", kv.me, args.Key, kv.state[args.Key], args.Seq)
	kv.mu.Unlock()
}

func (kv *KVServer) PutAppend(args *PutAppendArgs, reply *PutAppendReply) {
	// Your code here.
	kv.mu.Lock()
	if kv.appliedSeq[args.ClientId] >= args.Seq {
		reply.Err = OK
		kv.mu.Unlock()
		return
	}
	op := Op{
		ClientId: args.ClientId,
		Seq:      args.Seq,
		Op:       args.Op,
		Key:      args.Key,
		Value:    args.Value,
	}
	index, term, isLeader := kv.rf.Start(op)
	//DPrintf("raftIndex:%d,args:%+v", index, op)
	//defer DPrintf("put reply:%v", reply)
	if !isLeader {
		reply.Err = ErrWrongLeader
		kv.mu.Unlock()
		return
	}
	for i := 0; kv.lastAppliedIndex != index; i++ {
		kv.mu.Unlock()
		time.Sleep(10 * time.Millisecond)
		kv.mu.Lock()
		DPrintf("term:%d,client:%d,me:%d,raftIndex:%d,lastIndex:%d", term, args.ClientId, kv.me, index, kv.lastAppliedIndex)
	}
	if op.ClientId != kv.lastAppliedOp.ClientId || op.Seq != kv.lastAppliedOp.Seq {
		reply.Err = ErrServerError
	} else {
		reply.Err = OK
	}
	kv.mu.Unlock()
}

// the tester calls Kill() when a KVServer instance won't
// be needed again. for your convenience, we supply
// code to set rf.dead (without needing a lock),
// and a killed() method to test rf.dead in
// long-running loops. you can also add your own
// code to Kill(). you're not required to do anything
// about this, but it may be convenient (for example)
// to suppress debug output from a Kill()ed instance.
func (kv *KVServer) Kill() {
	atomic.StoreInt32(&kv.dead, 1)
	kv.rf.Kill()
	// Your code here, if desired.
}

func (kv *KVServer) killed() bool {
	z := atomic.LoadInt32(&kv.dead)
	return z == 1
}

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
func StartKVServer(servers []*labrpc.ClientEnd, me int, persister *raft.Persister, maxraftstate int) *KVServer {
	// call labgob.Register on structures you want
	// Go's RPC library to marshall/unmarshall.
	labgob.Register(Op{})

	kv := new(KVServer)
	kv.me = me
	kv.maxraftstate = maxraftstate

	// You may need initialization code here.

	kv.applyCh = make(chan raft.ApplyMsg)
	kv.rf = raft.Make(servers, me, persister, kv.applyCh)

	// You may need initialization code here.
	kv.appliedSeq = map[int64]int32{}
	kv.state = map[string]string{}
	kv.lastAppliedIndex = 0
	go kv.applier()
	return kv
}
func (kv *KVServer) applier() {
	commandMap := map[int]interface{}{}
	for m := range kv.applyCh {
		DPrintf("kvMe:%d,command:%+v", kv.me, m)
		if m.CommandValid {
			kv.mu.Lock()
			if kv.lastAppliedIndex+1 > m.CommandIndex {
				kv.mu.Unlock()
				continue
			} else if kv.lastAppliedIndex+1 < m.CommandIndex {

				//DPrintf("index:%d,lastSeq:%d,seq:%d,lastAppliedIndex:%d,commitIndex:%d", kv.me, kv.appliedSeq[op.ClientId], op.Seq, kv.lastAppliedIndex, m.CommandIndex)
			}
			commandMap[m.CommandIndex] = m.Command
			var ok bool
			var op Op
			command, ok := commandMap[kv.lastAppliedIndex+1]
			for ok {
				kv.lastAppliedIndex++
				if command != -1 {
					op = command.(Op)
					if kv.appliedSeq[op.ClientId]+1 == op.Seq {
						kv.appliedSeq[op.ClientId]++
						if op.Op != "Get" {
							value, contains := kv.state[op.Key]
							if op.Op == "Put" || !contains {
								kv.state[op.Key] = op.Value
							} else {
								kv.state[op.Key] = value + op.Value
							}
						}
					}
				} else {
					op = Op{}
				}
				kv.lastAppliedOp = op
				delete(commandMap, kv.lastAppliedIndex)
				DPrintf("me:%d,commitIndex:%d,key:%s,value:%s", kv.me, m.CommandIndex, op.Key, kv.state[op.Key])
				command, ok = commandMap[kv.lastAppliedIndex+1].(Op)
				kv.mu.Unlock()
				time.Sleep(30 * time.Millisecond)
				kv.mu.Lock()
			}
			kv.mu.Unlock()
		}
		//DPrintf("me:%d,apply end index:%d", kv.me, m.CommandIndex)
	}
}

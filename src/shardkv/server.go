package shardkv

import (
	"6.5840/labrpc"
	"6.5840/shardctrler"
	"bytes"
	"fmt"
	"log"
	"sync/atomic"
	"time"
)
import "6.5840/raft"
import "sync"
import "6.5840/labgob"

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
	Op string

	ClientId int64
	Seq      int32
	Key      string
	Value    string

	Config shardctrler.Config

	Shard          int
	ShardData      map[string]string
	AppliedSeq     map[int64]int32
	ShardConfigNum int

	Applied bool
}

type ShardKV struct {
	mu           sync.Mutex
	me           int
	rf           *raft.Raft
	applyCh      chan raft.ApplyMsg
	make_end     func(string) *labrpc.ClientEnd
	gid          int
	ctrlers      []*labrpc.ClientEnd
	maxraftstate int // snapshot if log grows this big

	// Your definitions here.
	config           shardctrler.Config
	waitShards       map[int]bool
	waitSendShards   map[int]bool
	lastAppliedIndex int
	lastAppliedOp    Op
	waitCommandMap   map[int]int
	state            [shardctrler.NShards]map[string]string
	appliedSeq       [shardctrler.NShards]map[int64]int32

	persister *raft.Persister

	dead  int32
	peers []string
}

func (kv *ShardKV) Get(args *GetArgs, reply *GetReply) {
	// Your code here.
	kv.mu.Lock()
	defer kv.mu.Unlock()
	if kv.config.Shards[args.Shard] != kv.gid {
		reply.Err = ErrWrongGroup
		return
	}
	if kv.waitShards[args.Shard] {
		reply.Err = ErrWrongGroup
		return
	}
	if kv.appliedSeq[args.Shard][args.ClientId] >= args.Seq {
		reply.Err = OK
		reply.Value = kv.state[args.Shard][args.Key]
		return
	}
	op := Op{
		Op:       "Get",
		ClientId: args.ClientId,
		Seq:      args.Seq,
		Key:      args.Key,
		Shard:    args.Shard,
	}
	index, _, isLeader := kv.rf.Start(op)
	//DPrintf("raftIndex:%d,args:%+v", index, op)
	//defer DPrintf("put reply:%v", reply)
	if !isLeader {
		reply.Err = ErrWrongLeader
		return
	}
	outTime := time.Now().Add(1 * time.Second)
	kv.waitCommandMap[index]++
	for time.Now().Before(outTime) && kv.lastAppliedIndex < index {
		kv.mu.Unlock()
		time.Sleep(10 * time.Millisecond)
		kv.mu.Lock()
		//DPrintf("get:client:%d,me:%d,raftIndex:%d,lastIndex:%d,shard:%d\n", args.ClientId, kv.me, index, kv.lastAppliedIndex, args.Shard)
	}
	kv.waitCommandMap[index]--
	if kv.waitCommandMap[index] == 0 {
		delete(kv.waitCommandMap, index)
	}
	if op.ClientId != kv.lastAppliedOp.ClientId || op.Seq != kv.lastAppliedOp.Seq {
		reply.Err = ErrWrongLeader
	} else if !op.Applied {
		reply.Err = ErrWrongGroup
	} else {
		reply.Err = OK
		reply.Value = kv.state[args.Shard][args.Key]
	}
}

func (kv *ShardKV) PutAppend(args *PutAppendArgs, reply *PutAppendReply) {
	// Your code here.
	kv.mu.Lock()
	defer kv.mu.Unlock()
	if kv.config.Shards[args.Shard] != kv.gid {
		reply.Err = ErrWrongGroup
		return
	}
	if kv.waitShards[args.Shard] {
		reply.Err = ErrWrongGroup
		return
	}
	if kv.appliedSeq[args.Shard][args.ClientId] >= args.Seq {
		reply.Err = OK
		return
	}
	op := Op{
		Op:       args.Op,
		ClientId: args.ClientId,
		Seq:      args.Seq,
		Key:      args.Key,
		Value:    args.Value,
		Shard:    args.Shard,
	}
	index, _, isLeader := kv.rf.Start(op)
	//DPrintf("raftIndex:%d,args:%+v", index, op)
	//defer DPrintf("put reply:%v", reply)
	if !isLeader {
		reply.Err = ErrWrongLeader
		return
	}
	outTime := time.Now().Add(1 * time.Second)
	kv.waitCommandMap[index]++
	for time.Now().Before(outTime) && kv.lastAppliedIndex < index {
		kv.mu.Unlock()
		time.Sleep(10 * time.Millisecond)
		kv.mu.Lock()
		//DPrintf("putAppend:client:%d,me:%d,raftIndex:%d,lastIndex:%d,shard:%d\n", args.ClientId, kv.me, index, kv.lastAppliedIndex, args.Shard)
	}
	kv.waitCommandMap[index]--
	if kv.waitCommandMap[index] == 0 {
		delete(kv.waitCommandMap, index)
	}
	if op.ClientId != kv.lastAppliedOp.ClientId || op.Seq != kv.lastAppliedOp.Seq {
		reply.Err = ErrWrongLeader
	} else if !op.Applied {
		reply.Err = ErrWrongGroup
	} else {
		reply.Err = OK
	}
}

func (kv *ShardKV) AcceptShard(args *TranShardArgs, reply *TransShardReply) {
	kv.mu.Lock()
	defer kv.mu.Unlock()
	if kv.config.Num < args.ConfigNum {
		return
	}
	if kv.config.Num > args.ConfigNum || !kv.waitShards[args.Shard] {
		reply.Err = OK
		return
	}
	op := Op{
		Op:             "ApplyShard",
		Shard:          args.Shard,
		ShardData:      args.Data,
		AppliedSeq:     args.AppliedSeq,
		ShardConfigNum: args.ConfigNum,
	}
	index, _, isLeader := kv.rf.Start(op)
	//DPrintf("raftIndex:%d,args:%+v", index, op)
	//defer DPrintf("put reply:%v", reply)
	if !isLeader {
		reply.Err = ErrWrongLeader
		return
	}
	outTime := time.Now().Add(1 * time.Second)
	kv.waitCommandMap[index]++
	for time.Now().Before(outTime) && kv.lastAppliedIndex < index {
		kv.mu.Unlock()
		time.Sleep(10 * time.Millisecond)
		kv.mu.Lock()
		//DPrintf("accept shard:%d,config:%d,me:%d-%d,raftIndex:%d,lastIndex:%d\n", args.Shard, args.ConfigNum, kv.me, kv.gid, index, kv.lastAppliedIndex)
	}
	kv.waitCommandMap[index]--
	if kv.waitCommandMap[index] == 0 {
		delete(kv.waitCommandMap, index)
	}
	if kv.config.Num > args.ConfigNum || !kv.waitShards[args.Shard] {
		reply.Err = OK
	}
}
func (kv *ShardKV) RemoveShard(args *RemoveShardArgs, reply *RemoveShardReply) {
	kv.mu.Lock()
	defer kv.mu.Unlock()
	if kv.config.Num < args.ShardConfigNum {
		return
	}
	if kv.config.Num > args.ShardConfigNum || !kv.waitSendShards[args.Shard] {
		reply.Err = OK
		return
	}
	op := Op{
		Op:             "RemoveShard",
		Shard:          args.Shard,
		ShardConfigNum: args.ShardConfigNum,
	}
	index, _, isLeader := kv.rf.Start(op)
	//DPrintf("raftIndex:%d,args:%+v", index, op)
	//defer DPrintf("put reply:%v", reply)
	if !isLeader {
		return
	}
	outTime := time.Now().Add(1 * time.Second)
	kv.waitCommandMap[index]++
	for time.Now().Before(outTime) && kv.lastAppliedIndex < index {
		kv.mu.Unlock()
		time.Sleep(10 * time.Millisecond)
		kv.mu.Lock()
		//DPrintf("remove shard:%d,config:%d,me:%d-%d,raftIndex:%d,lastIndex:%d\n", args.Shard, args.ShardConfigNum, kv.me, kv.gid, index, kv.lastAppliedIndex)
	}
	kv.waitCommandMap[index]--
	if kv.waitCommandMap[index] == 0 {
		delete(kv.waitCommandMap, index)
	}
	if kv.config.Num == args.ShardConfigNum && !kv.waitSendShards[args.Shard] || kv.config.Num > args.ShardConfigNum {
		reply.Err = OK
	}
}

func (kv *ShardKV) updateConfig(conf shardctrler.Config) bool {
	kv.mu.Lock()
	defer kv.mu.Unlock()
	if kv.config.Num >= conf.Num {
		return true
	}
	for !kv.killed() && (len(kv.waitShards) > 0 || len(kv.waitSendShards) > 0) && kv.config.Num < conf.Num {
		kv.mu.Unlock()
		time.Sleep(30 * time.Millisecond)
		kv.mu.Lock()
		//if rand.Int()%30 == 2 {
		//	DPrintf("waitShard:%v,sendShard:%v,config:%d\n", kv.waitShards, kv.waitSendShards, kv.config.Num)
		//}
	}
	op := Op{
		Op:     "ApplyConfig",
		Config: conf,
	}
	for !kv.killed() {
		if kv.config.Num >= conf.Num {
			return true
		}
		index, _, isLeader := kv.rf.Start(op)
		//defer DPrintf("put reply:%v", reply)
		if !isLeader {
			return false
		}
		DPrintf("raftIndex:%d,args:%+v", index, op)
		outTime := time.Now().Add(1 * time.Second)
		kv.waitCommandMap[index]++
		for time.Now().Before(outTime) && kv.lastAppliedIndex < index {
			kv.mu.Unlock()
			time.Sleep(10 * time.Millisecond)
			kv.mu.Lock()
			//DPrintf("update config:%d,me:%d-%d,raftIndex:%d,lastIndex:%d\n", conf.Num, kv.me, kv.gid, index, kv.lastAppliedIndex)
		}
		kv.waitCommandMap[index]--
		if kv.waitCommandMap[index] == 0 {
			delete(kv.waitCommandMap, index)
		}
		if kv.config.Num == conf.Num {
			return true
		} else if kv.config.Num > conf.Num {
			return true
		}
	}
	return false
}

// the tester calls Kill() when a ShardKV instance won't
// be needed again. you are not required to do anything
// in Kill(), but it might be convenient to (for example)
// turn off debug output from this instance.
func (kv *ShardKV) Kill() {
	kv.rf.Kill()
	// Your code here, if desired.
	atomic.StoreInt32(&kv.dead, 1)
}

func (kv *ShardKV) killed() bool {
	z := atomic.LoadInt32(&kv.dead)
	return z == 1
}

// servers[] contains the ports of the servers in this group.
//
// me is the index of the current server in servers[].
//
// the k/v server should store snapshots through the underlying Raft
// implementation, which should call persister.SaveStateAndSnapshot() to
// atomically save the Raft state along with the snapshot.
//
// the k/v server should snapshot when Raft's saved state exceeds
// maxraftstate bytes, in order to allow Raft to garbage-collect its
// log. if maxraftstate is -1, you don't need to snapshot.
//
// gid is this group's GID, for interacting with the shardctrler.
//
// pass ctrlers[] to shardctrler.MakeClerk() so you can send
// RPCs to the shardctrler.
//
// make_end(servername) turns a server name from a
// Config.Groups[gid][i] into a labrpc.ClientEnd on which you can
// send RPCs. You'll need this to send RPCs to other groups.
//
// look at client.go for examples of how to use ctrlers[]
// and make_end() to send RPCs to the group owning a specific shard.
//
// StartServer() must return quickly, so it should start goroutines
// for any long-running work.
func StartServer(servers []*labrpc.ClientEnd, me int, persister *raft.Persister, maxraftstate int, gid int, ctrlers []*labrpc.ClientEnd, make_end func(string) *labrpc.ClientEnd) *ShardKV {
	// call labgob.Register on structures you want
	// Go's RPC library to marshall/unmarshall.
	labgob.Register(Op{})

	kv := new(ShardKV)
	kv.me = me
	kv.maxraftstate = maxraftstate
	kv.make_end = make_end
	kv.gid = gid
	kv.ctrlers = ctrlers

	// Your initialization code here.

	// Use something like this to talk to the shardctrler:
	// kv.mck = shardctrler.MakeClerk(kv.ctrlers)
	kv.persister = persister
	if persister.SnapshotSize() > 0 {
		kv.applySnapshot(persister.ReadSnapshot(), -1)
	} else {
		kv.config = shardctrler.Config{}
		kv.waitShards = make(map[int]bool)
		kv.waitSendShards = make(map[int]bool)
		kv.lastAppliedIndex = 0
		kv.waitCommandMap = make(map[int]int)
	}
	kv.applyCh = make(chan raft.ApplyMsg)
	kv.rf = raft.Make(servers, me, persister, kv.applyCh)

	go kv.applier()
	go kv.refreshConfig(kv.config.Num + 1)
	go kv.heart()
	return kv
}
func (kv *ShardKV) applier() {
	for m := range kv.applyCh {
		if m.CommandValid {
			kv.applyCommand(&m)
		} else if m.SnapshotValid {
			kv.applySnapshot(m.Snapshot, m.SnapshotIndex)
		}
		//DPrintf("me:%d,apply end index:%d", kv.me, m.CommandIndex)
	}
}

func (kv *ShardKV) applyCommand(m *raft.ApplyMsg) {
	DPrintf("kvMe:%d-%d,command:%+v\n", kv.me, kv.gid, m)
	if kv.lastAppliedIndex+1 > m.CommandIndex {
		return
	} else if kv.lastAppliedIndex+1 < m.CommandIndex {

		//DPrintf("index:%d,lastSeq:%d,seq:%d,lastAppliedIndex:%d,commitIndex:%d", kv.me, kv.appliedSeq[op.ClientId], op.Seq, kv.lastAppliedIndex, m.CommandIndex)
	}
	kv.mu.Lock()
	defer kv.mu.Unlock()
	kv.lastAppliedIndex++
	op := m.Command.(Op)
	if op.Op == "Get" || op.Op == "Put" || op.Op == "Append" {
		kv.applyClientOp(&op)
	} else if op.Op == "ApplyConfig" {
		kv.applyConfig(op)
	} else if op.Op == "ApplyShard" {
		kv.applyShard(op)
	} else if op.Op == "RemoveShard" {
		if kv.config.Num == op.ShardConfigNum {
			DPrintf("remove shard:%d from config:%d,cur config:%d,gid:%d\n", op.Shard, op.ShardConfigNum, kv.config.Num, kv.gid)
			delete(kv.waitSendShards, op.Shard)
			kv.state[op.Shard] = nil
			kv.appliedSeq[op.Shard] = nil
		}
	}
	kv.lastAppliedOp = op
	if kv.maxraftstate != -1 && kv.persister.RaftStateSize() >= kv.maxraftstate {
		kv.snapshot()
	}
	for kv.waitCommandMap[kv.lastAppliedIndex] != 0 {
		kv.mu.Unlock()
		time.Sleep(10 * time.Millisecond)
		kv.mu.Lock()
	}
}
func (kv *ShardKV) applyClientOp(op *Op) {
	if kv.appliedSeq[op.Shard][op.ClientId]+1 == op.Seq {
		if kv.config.Shards[op.Shard] != kv.gid {
			op.Applied = false
			return
		}
		kv.appliedSeq[op.Shard][op.ClientId]++
		if op.Op != "Get" {
			value, contains := kv.state[op.Shard][op.Key]
			if op.Op == "Put" || !contains {
				kv.state[op.Shard][op.Key] = op.Value
			} else {
				kv.state[op.Shard][op.Key] = value + op.Value
			}
		}
		op.Applied = true
	} else if kv.appliedSeq[op.Shard][op.ClientId]+1 > op.Seq {
		op.Applied = true
	} else {
		panic(fmt.Sprintf("err Seq,clientId:%d,exp_seq:%d,now_seq:%d", op.ClientId, kv.appliedSeq[op.Shard][op.ClientId]+1, op.Seq))
	}
}
func (kv *ShardKV) applyConfig(op Op) {
	if kv.config.Num+1 > op.Config.Num {
		return
	}
	if kv.config.Num+1 < op.Config.Num {
		panic(fmt.Sprintf("want config:%d,received config:%d", kv.config.Num+1, op.Config.Num))
	}
	if len(kv.waitShards) != 0 || len(kv.waitSendShards) != 0 {
		panic(fmt.Sprintf("trans shard not complate,waitShard:%v,waitSendShard:%v,config:%v\n", kv.waitShards, kv.waitSendShards, kv.config))
	}
	old := kv.config
	for i := 0; i < shardctrler.NShards; i++ {
		if old.Shards[i] == op.Config.Shards[i] {
			continue
		}
		if old.Shards[i] != kv.gid && op.Config.Shards[i] != kv.gid {
			continue
		}
		if old.Shards[i] == kv.gid {
			kv.waitSendShards[i] = true
			args := TranShardArgs{
				Shard:      i,
				Data:       kv.state[i],
				AppliedSeq: kv.appliedSeq[i],
				ConfigNum:  op.Config.Num,
			}
			go kv.sendShard(&args, op.Config.Groups[op.Config.Shards[i]])
		} else if old.Shards[i] != 0 {
			kv.waitShards[i] = true
		} else {
			kv.state[i] = make(map[string]string)
			kv.appliedSeq[i] = make(map[int64]int32)
		}
	}
	if len(op.Config.Groups[kv.gid]) > 0 {
		kv.peers = op.Config.Groups[kv.gid]
	}
	kv.config = op.Config
}
func (kv *ShardKV) applyShard(op Op) {
	if kv.config.Num < op.ShardConfigNum {
		panic(fmt.Sprintf("miss config applied:%d,current config:%d", op.ShardConfigNum, kv.config.Num))
	}
	if kv.config.Num > op.ShardConfigNum || !kv.waitShards[op.Shard] {
		return
	}
	kv.state[op.Shard] = make(map[string]string, len(op.ShardData))
	for k, _ := range op.ShardData {
		kv.state[op.Shard][k] = op.ShardData[k]
	}
	kv.appliedSeq[op.Shard] = make(map[int64]int32, len(op.AppliedSeq))
	for k, _ := range op.AppliedSeq {
		kv.appliedSeq[op.Shard][k] = op.AppliedSeq[k]
	}
	delete(kv.waitShards, op.Shard)
}
func (kv *ShardKV) snapshot() {
	w := new(bytes.Buffer)
	e := labgob.NewEncoder(w)
	e.Encode(kv.lastAppliedIndex)
	e.Encode(kv.lastAppliedOp)
	e.Encode(kv.state)
	e.Encode(kv.appliedSeq)
	e.Encode(kv.config)
	e.Encode(kv.waitShards)
	e.Encode(kv.waitSendShards)
	e.Encode(kv.peers)
	kv.rf.Snapshot(kv.lastAppliedIndex, w.Bytes())
	//DPrintf("me:%d,snapshot index:%d", kv.me, kv.lastAppliedIndex)
}
func (kv *ShardKV) applySnapshot(snapshot []byte, index int) {
	kv.mu.Lock()
	defer kv.mu.Unlock()
	r := bytes.NewBuffer(snapshot)
	d := labgob.NewDecoder(r)
	var lastIncludedIndex int
	var lastOp Op
	var state [shardctrler.NShards]map[string]string
	var appliedSeq [shardctrler.NShards]map[int64]int32
	var conf shardctrler.Config
	var waitShard map[int]bool
	var waitSendShards map[int]bool
	var peers []string
	if d.Decode(&lastIncludedIndex) != nil ||
		d.Decode(&lastOp) != nil ||
		d.Decode(&state) != nil ||
		d.Decode(&appliedSeq) != nil ||
		d.Decode(&conf) != nil ||
		d.Decode(&waitShard) != nil ||
		d.Decode(&waitSendShards) != nil ||
		d.Decode(&peers) != nil {
		log.Fatalf("snapshot decode error")
	}
	if index != -1 && index != lastIncludedIndex {
		//DPrintf("server %v snapshot doesn't match m.SnapshotIndex", index)
	}
	kv.lastAppliedIndex = lastIncludedIndex
	kv.lastAppliedOp = lastOp
	kv.state = state
	kv.appliedSeq = appliedSeq
	kv.waitCommandMap = make(map[int]int)
	kv.config = conf
	kv.waitShards = waitShard
	kv.waitSendShards = waitSendShards
	kv.peers = peers
	for shard, _ := range waitSendShards {
		args := TranShardArgs{
			Shard:      shard,
			Data:       kv.state[shard],
			AppliedSeq: kv.appliedSeq[shard],
			ConfigNum:  kv.config.Num,
		}
		go kv.sendShard(&args, conf.Groups[conf.Shards[shard]])
	}
	//DPrintf("me:%d,apply snapshot index:%d", kv.me, kv.lastAppliedIndex)
}
func (kv *ShardKV) sendShard(args *TranShardArgs, servers []string) {
	for !kv.killed() {
		DPrintf("send shard:%d,from:%d,config:%d", args.Shard, kv.gid, args.ConfigNum)
		for si := 0; si < len(servers); si++ {
			srv := kv.make_end(servers[si])
			var reply TransShardReply
			ok := srv.Call("ShardKV.AcceptShard", args, &reply)
			if ok && reply.Err == OK {
				kv.mu.Lock()
				if kv.config.Num == args.ConfigNum {
					DPrintf("success send shard:%d,from:%d,config:%d", args.Shard, kv.gid, args.ConfigNum)
					go kv.removeShardData(args.Shard, args.ConfigNum, kv.peers)
				}
				kv.mu.Unlock()
				return
			}
			// ... not ok, or ErrWrongLeader
		}
		time.Sleep(100 * time.Millisecond)
	}
}

func (kv *ShardKV) removeShardData(shard int, configNum int, servers []string) {
	args := &RemoveShardArgs{
		Shard:          shard,
		ShardConfigNum: configNum,
	}
	for !kv.killed() {
		DPrintf("remove shard:%d,from:%d,config:%d", args.Shard, kv.gid, configNum)
		for si := 0; si < len(servers); si++ {
			srv := kv.make_end(servers[si])
			var reply RemoveShardReply
			ok := srv.Call("ShardKV.RemoveShard", args, &reply)
			if ok && reply.Err == OK {
				DPrintf("remove shard:%d success,config:%d,server:%s\n", args.Shard, args.ShardConfigNum, servers[si])
				return
			}
			// ... not ok, or ErrWrongLeader
		}
		time.Sleep(100 * time.Millisecond)
	}
}

func (kv *ShardKV) refreshConfig(configNum int) {
	clientId := nrand()
	args := &shardctrler.QueryArgs{Num: configNum, ClientId: clientId, Seq: 1}
	// Your code here.
	for !kv.killed() {
		// try each known server.
		for _, srv := range kv.ctrlers {
			var reply shardctrler.QueryReply
			ok := srv.Call("ShardCtrler.Query", args, &reply)
			if ok && reply.WrongLeader == false && reply.Config.Num == args.Num {
				for !kv.updateConfig(reply.Config) {
				}
				args.Num++
				args.Seq++
				break
			}
		}
		time.Sleep(100 * time.Millisecond)
	}
}
func (kv *ShardKV) heart() {
	op := Op{
		Op: "Empty",
	}
	for !kv.killed() {
		kv.rf.Start(op)
		time.Sleep(1 * time.Second)
	}
}

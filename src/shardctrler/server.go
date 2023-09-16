package shardctrler

import (
	"6.5840/raft"
	"fmt"
	"sort"
	"time"
)
import "6.5840/labrpc"
import "sync"
import "6.5840/labgob"

type ShardCtrler struct {
	mu      sync.Mutex
	me      int
	rf      *raft.Raft
	applyCh chan raft.ApplyMsg

	// Your data here.
	lastAppliedIndex  int
	lastAppliedOp     Op
	runningCommandMap map[int]bool
	appliedSeq        map[int64]int32

	configs []Config // indexed by config num
}

type Op struct {
	// Your data here.
	ClientId  int64
	Seq       int32
	Type      string
	JoinArgs  JoinArgs
	LeaveArgs LeaveArgs
	MoveArgs  MoveArgs
	QueryArgs QueryArgs
}

func (sc *ShardCtrler) Join(args *JoinArgs, reply *JoinReply) {
	// Your code here.
	sc.mu.Lock()
	if sc.appliedSeq[args.ClientId] >= args.Seq {
		reply.Err = OK
		sc.mu.Unlock()
		return
	}
	op := Op{
		ClientId: args.ClientId,
		Seq:      args.Seq,
		Type:     "Join",
		JoinArgs: *args,
	}
	index, _, isLeader := sc.rf.Start(op)
	//DPrintf("raftIndex:%d,args:%+v", index, op)
	//defer DPrintf("put reply:%v", reply)
	if !isLeader {
		reply.WrongLeader = true
		sc.mu.Unlock()
		return
	}
	outTime := time.Now().Add(1 * time.Second)
	sc.runningCommandMap[index] = true
	for time.Now().Before(outTime) && sc.lastAppliedIndex < index {
		sc.mu.Unlock()
		time.Sleep(10 * time.Millisecond)
		sc.mu.Lock()
		//DPrintf("term:%d,client:%d,me:%d,raftIndex:%d,lastIndex:%d", term, args.ClientId, kv.me, index, kv.lastAppliedIndex)
	}
	delete(sc.runningCommandMap, index)
	if op.ClientId != sc.lastAppliedOp.ClientId || op.Seq != sc.lastAppliedOp.Seq {
		reply.Err = ServerError
	} else {
		reply.Err = OK
	}
	sc.mu.Unlock()
}

func (sc *ShardCtrler) Leave(args *LeaveArgs, reply *LeaveReply) {
	// Your code here.
	sc.mu.Lock()
	if sc.appliedSeq[args.ClientId] >= args.Seq {
		reply.Err = OK
		sc.mu.Unlock()
		return
	}
	op := Op{
		ClientId:  args.ClientId,
		Seq:       args.Seq,
		Type:      "Leave",
		LeaveArgs: *args,
	}
	index, _, isLeader := sc.rf.Start(op)
	//DPrintf("raftIndex:%d,args:%+v", index, op)
	//defer DPrintf("put reply:%v", reply)
	if !isLeader {
		reply.WrongLeader = true
		sc.mu.Unlock()
		return
	}
	outTime := time.Now().Add(1 * time.Second)
	sc.runningCommandMap[index] = true
	for time.Now().Before(outTime) && sc.lastAppliedIndex < index {
		sc.mu.Unlock()
		time.Sleep(10 * time.Millisecond)
		sc.mu.Lock()
		//DPrintf("term:%d,client:%d,me:%d,raftIndex:%d,lastIndex:%d", term, args.ClientId, kv.me, index, kv.lastAppliedIndex)
	}
	delete(sc.runningCommandMap, index)
	if op.ClientId != sc.lastAppliedOp.ClientId || op.Seq != sc.lastAppliedOp.Seq {
		reply.Err = ServerError
	} else {
		reply.Err = OK
	}
	sc.mu.Unlock()
}

func (sc *ShardCtrler) Move(args *MoveArgs, reply *MoveReply) {
	// Your code here.
	sc.mu.Lock()
	if sc.appliedSeq[args.ClientId] >= args.Seq {
		reply.Err = OK
		sc.mu.Unlock()
		return
	}
	op := Op{
		ClientId: args.ClientId,
		Seq:      args.Seq,
		Type:     "Move",
		MoveArgs: *args,
	}
	index, _, isLeader := sc.rf.Start(op)
	//DPrintf("raftIndex:%d,args:%+v", index, op)
	//defer DPrintf("put reply:%v", reply)
	if !isLeader {
		reply.WrongLeader = true
		sc.mu.Unlock()
		return
	}
	outTime := time.Now().Add(1 * time.Second)
	sc.runningCommandMap[index] = true
	for time.Now().Before(outTime) && sc.lastAppliedIndex < index {
		sc.mu.Unlock()
		time.Sleep(10 * time.Millisecond)
		sc.mu.Lock()
		//DPrintf("term:%d,client:%d,me:%d,raftIndex:%d,lastIndex:%d", term, args.ClientId, kv.me, index, kv.lastAppliedIndex)
	}
	delete(sc.runningCommandMap, index)
	if op.ClientId != sc.lastAppliedOp.ClientId || op.Seq != sc.lastAppliedOp.Seq {
		reply.Err = ServerError
	} else {
		reply.Err = OK
	}
	sc.mu.Unlock()
}

func (sc *ShardCtrler) Query(args *QueryArgs, reply *QueryReply) {
	// Your code here.
	sc.mu.Lock()
	if sc.appliedSeq[args.ClientId] >= args.Seq {
		reply.Err = OK
		if args.Num == -1 || args.Num >= len(sc.configs) {
			reply.Config = sc.configs[len(sc.configs)-1]
		} else {
			reply.Config = sc.configs[args.Num]
		}
		sc.mu.Unlock()
		return
	}
	op := Op{
		ClientId:  args.ClientId,
		Seq:       args.Seq,
		Type:      "Query",
		QueryArgs: *args,
	}
	index, _, isLeader := sc.rf.Start(op)
	//DPrintf("raftIndex:%d,args:%+v", index, op)
	//defer DPrintf("put reply:%v", reply)
	if !isLeader {
		reply.WrongLeader = true
		sc.mu.Unlock()
		return
	}
	outTime := time.Now().Add(1 * time.Second)
	sc.runningCommandMap[index] = true
	for time.Now().Before(outTime) && sc.lastAppliedIndex < index {
		sc.mu.Unlock()
		time.Sleep(10 * time.Millisecond)
		sc.mu.Lock()
		//DPrintf("term:%d,client:%d,me:%d,raftIndex:%d,lastIndex:%d", term, args.ClientId, kv.me, index, kv.lastAppliedIndex)
	}
	delete(sc.runningCommandMap, index)
	if op.ClientId != sc.lastAppliedOp.ClientId || op.Seq != sc.lastAppliedOp.Seq {
		reply.Err = ServerError
	} else {
		reply.Err = OK
		if args.Num == -1 || args.Num >= len(sc.configs) {
			reply.Config = sc.configs[len(sc.configs)-1]
		} else {
			reply.Config = sc.configs[args.Num]
		}
	}
	sc.mu.Unlock()
}

// the tester calls Kill() when a ShardCtrler instance won't
// be needed again. you are not required to do anything
// in Kill(), but it might be convenient to (for example)
// turn off debug output from this instance.
func (sc *ShardCtrler) Kill() {
	sc.rf.Kill()
	// Your code here, if desired.
}

// needed by shardkv tester
func (sc *ShardCtrler) Raft() *raft.Raft {
	return sc.rf
}

// servers[] contains the ports of the set of
// servers that will cooperate via Raft to
// form the fault-tolerant shardctrler service.
// me is the index of the current server in servers[].
func StartServer(servers []*labrpc.ClientEnd, me int, persister *raft.Persister) *ShardCtrler {
	sc := new(ShardCtrler)
	sc.me = me

	sc.configs = make([]Config, 1)
	sc.configs[0].Groups = map[int][]string{}

	labgob.Register(Op{})
	sc.applyCh = make(chan raft.ApplyMsg)
	sc.rf = raft.Make(servers, me, persister, sc.applyCh)

	// Your code here.
	sc.runningCommandMap = make(map[int]bool)
	sc.appliedSeq = map[int64]int32{}
	sc.lastAppliedIndex = 0
	go sc.applier()
	return sc
}
func (sc *ShardCtrler) applier() {
	for m := range sc.applyCh {
		//DPrintf("kvMe:%d,command:%+v", sc.me, m)
		if m.CommandValid {
			sc.applyCommand(&m)
		}
		//DPrintf("me:%d,apply end index:%d", sc.me, m.CommandIndex)
	}
}

func (sc *ShardCtrler) applyCommand(m *raft.ApplyMsg) {
	if sc.lastAppliedIndex+1 > m.CommandIndex {
		return
	} else if sc.lastAppliedIndex+1 < m.CommandIndex {

		//DPrintf("index:%d,lastSeq:%d,seq:%d,lastAppliedIndex:%d,commitIndex:%d", sc.me, sc.appliedSeq[op.ClientId], op.Seq, sc.lastAppliedIndex, m.CommandIndex)
	}
	sc.mu.Lock()
	defer sc.mu.Unlock()
	sc.lastAppliedIndex++
	op := m.Command.(Op)
	if sc.appliedSeq[op.ClientId]+1 == op.Seq {
		sc.appliedSeq[op.ClientId]++
		if op.Type != "Query" {
			var newConfig Config
			if op.Type == "Join" {
				newConfig = *(sc.applyJoin(op.JoinArgs.Servers))
			} else if op.Type == "Move" {
				newConfig = *sc.applyMove(op.MoveArgs.Shard, op.MoveArgs.GID)
			} else if op.Type == "Leave" {
				newConfig = *sc.applyLeave(op.LeaveArgs.GIDs)
			} else {
				panic(fmt.Sprintf("unSupportOperation:%s\n", op.Type))
			}
			sc.configs = append(sc.configs, newConfig)
		}
	}
	sc.lastAppliedOp = op
	//DPrintf("me:%d,commitIndex:%d,key:%s,value:%s", sc.me, sc.lastAppliedIndex, op.Key, sc.state[op.Key])
	for sc.runningCommandMap[sc.lastAppliedIndex] {
		sc.mu.Unlock()
		time.Sleep(10 * time.Millisecond)
		sc.mu.Lock()
	}
}
func (sc *ShardCtrler) applyJoin(servers map[int][]string) *Config {
	newConfig := sc.copyConfig()
	newConfig.Num++
	for gid, values := range servers {
		newConfig.Groups[gid] = make([]string, len(values))
		copy(newConfig.Groups[gid], values)
	}
	reBalance(newConfig)
	return newConfig
}
func (sc *ShardCtrler) applyMove(shard, gid int) *Config {
	newConfig := sc.copyConfig()
	newConfig.Num++
	newConfig.Shards[shard] = gid
	return newConfig
}
func (sc *ShardCtrler) applyLeave(gids []int) *Config {
	newConfig := sc.copyConfig()
	newConfig.Num++
	for _, gid := range gids {
		delete(newConfig.Groups, gid)
	}
	reBalance(newConfig)
	return newConfig
}
func (sc *ShardCtrler) copyConfig() *Config {
	oldConfig := sc.configs[len(sc.configs)-1]
	nc := Config{
		Num:    oldConfig.Num,
		Shards: [10]int{},
		Groups: map[int][]string{},
	}
	for k, v := range oldConfig.Groups {
		nc.Groups[k] = make([]string, len(v), len(v))
		copy(nc.Groups[k], v)
	}
	for i := 0; i < 10; i++ {
		nc.Shards[i] = oldConfig.Shards[i]
	}
	return &nc
}
func reBalance(config *Config) {
	if len(config.Groups) == 0 {
		for i := 0; i < 10; i++ {
			config.Shards[i] = 0
		}
		return
	}
	mod := NShards % len(config.Groups)
	min := NShards / len(config.Groups)
	count := make(map[int]int)
	for i := 0; i < 10; i++ {
		_, ok := config.Groups[config.Shards[i]]
		if ok {
			count[config.Shards[i]]++
		} else {
			config.Shards[i] = 0
		}
	}
	targets := make([]int, 0)
	for k, _ := range config.Groups {
		if count[k] < min {
			targets = append(targets, k)
		}
	}
	sort.Ints(targets)
	for i, j := 0, 0; i < 10 && len(targets) > 0; i++ {
		gid := config.Shards[i]
		if gid != 0 && count[gid] <= min {
			continue
		}
		if gid != 0 && count[gid] == min+1 && mod > 0 {
			mod--
			continue
		}
		config.Shards[i] = targets[j]
		count[targets[j]]++
		count[gid]--
		if count[targets[j]] >= min {
			j++
			j = j % len(targets)
		}
	}
}

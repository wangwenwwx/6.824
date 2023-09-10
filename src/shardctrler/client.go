package shardctrler

//
// Shardctrler clerk.
//

import (
	"6.5840/labrpc"
	"sync/atomic"
)
import "time"
import "crypto/rand"
import "math/big"

type Clerk struct {
	servers []*labrpc.ClientEnd
	// Your data here.
	ClientId int64
	Seq      int32
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
	// Your code here.
	ck.ClientId = nrand()
	ck.Seq = 0
	return ck
}

func (ck *Clerk) Query(num int) Config {
	seq := atomic.AddInt32(&ck.Seq, 1)
	args := &QueryArgs{Num: num, ClientId: ck.ClientId, Seq: seq}
	// Your code here.
	args.Num = num
	for {
		// try each known server.
		for _, srv := range ck.servers {
			var reply QueryReply
			ok := srv.Call("ShardCtrler.Query", args, &reply)
			if ok && reply.WrongLeader == false {
				return reply.Config
			}
		}
		time.Sleep(100 * time.Millisecond)
	}
}

func (ck *Clerk) Join(servers map[int][]string) {
	args := &JoinArgs{}
	seq := atomic.AddInt32(&ck.Seq, 1)
	// Your code here.
	args.Servers = servers
	args.Seq = seq
	args.ClientId = ck.ClientId

	for {
		// try each known server.
		for _, srv := range ck.servers {
			var reply JoinReply
			ok := srv.Call("ShardCtrler.Join", args, &reply)
			if ok && reply.WrongLeader == false {
				return
			}
		}
		time.Sleep(100 * time.Millisecond)
	}
}

func (ck *Clerk) Leave(gids []int) {
	args := &LeaveArgs{}
	seq := atomic.AddInt32(&ck.Seq, 1)
	// Your code here.
	args.GIDs = gids
	args.ClientId = ck.ClientId
	args.Seq = seq

	for {
		// try each known server.
		for _, srv := range ck.servers {
			var reply LeaveReply
			ok := srv.Call("ShardCtrler.Leave", args, &reply)
			if ok && reply.WrongLeader == false {
				return
			}
		}
		time.Sleep(100 * time.Millisecond)
	}
}

func (ck *Clerk) Move(shard int, gid int) {
	args := &MoveArgs{}
	// Your code here.

	seq := atomic.AddInt32(&ck.Seq, 1)
	args.Shard = shard
	args.GID = gid
	args.ClientId = ck.ClientId
	args.Seq = seq

	for {
		// try each known server.
		for _, srv := range ck.servers {
			var reply MoveReply
			ok := srv.Call("ShardCtrler.Move", args, &reply)
			if ok && reply.WrongLeader == false {
				return
			}
		}
		time.Sleep(100 * time.Millisecond)
	}
}

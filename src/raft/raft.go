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
	"6.824/labgob"
	"bytes"
	"log"
	"math/rand"
	"sort"

	//	"bytes"
	"sync"
	"sync/atomic"
	"time"

	//	"6.824/labgob"
	"6.824/labrpc"
)

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
	mu          sync.RWMutex        // Lock to protect shared access to this peer's state
	peers       []*labrpc.ClientEnd // RPC end points of all peers
	persister   *Persister          // Object to hold this peer's persisted state
	me          int                 // this peer's index into peers[]
	dead        int32               // set by Kill()
	term        int                 // 任期
	status      Status              // 服务状态
	heartbeats  chan bool
	votedFor    int
	timeout     int64
	logs        []Entry
	commitIndex int
	lastApplied int
	matchIndex  []int
	nextIndex   []int
	applyChan   chan ApplyMsg
	random      *rand.Rand
	// Your data here (2A, heartbeats, 2C).
	// Look at the paper's Figure 2 for a description of what
	// state a Raft server must maintain.

}
type Status int

const (
	Leader Status = iota
	Follower
	Candidate
)

// return currentTerm and whether this server
// believes it is the leader.
func (rf *Raft) GetState() (int, bool) {

	var term int
	var isleader bool
	//log.Printf("term:%d,leader:%t\n",term,isleader)
	rf.mu.RLock()
	term = rf.term
	isleader = rf.status == Leader
	rf.mu.RUnlock()
	return term, isleader
}

//
// save Raft's persistent state to stable storage,
// where it can later be retrieved after a crash and restart.
// see paper's Figure 2 for a description of what should be persistent.
//
func (rf *Raft) persist() {
	// Your code here (2C).
	// Example:
	w := new(bytes.Buffer)
	e := labgob.NewEncoder(w)
	e.Encode(rf.term)
	e.Encode(rf.votedFor)
	e.Encode(rf.logs)
	e.Encode(rf.commitIndex)
	data := w.Bytes()
	rf.persister.SaveRaftState(data)
}

//
// restore previously persisted state.
//
func (rf *Raft) readPersist(data []byte) {
	if data == nil || len(data) < 1 { // bootstrap without any state?
		return
	}
	// Your code here (2C).
	// Example:
	r := bytes.NewBuffer(data)
	d := labgob.NewDecoder(r)
	var term int
	var votedFor int
	var logs []Entry
	var commitIndex int
	if d.Decode(&term) != nil ||
		d.Decode(&votedFor) != nil ||
		d.Decode(&logs) != nil ||
		d.Decode(&commitIndex) != nil {
		log.Fatalln("error")
	} else {
		rf.term = term
		rf.votedFor = votedFor
		rf.logs = logs
		rf.commitIndex = commitIndex
	}
}

//
// A service wants to switch to snapshot.  Only do so if Raft hasn't
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
	// Your code here (2D).

}

//
// example RequestVote RPC arguments structure.
// field names must start with capital letters!
//
type RequestVoteArgs struct {
	Term         int
	CandidateId  int
	LastLogIndex int
	LastLogTerm  int
	// Your data here (2A, 2B).
}

//
// example RequestVote RPC reply structure.
// field names must start with capital letters!
//
type RequestVoteReply struct {
	Term        int
	VoteGranted bool
	// Your data here (2A).
}

//
// example RequestVote RPC handler.
//
func (rf *Raft) RequestVote(args *RequestVoteArgs, reply *RequestVoteReply) {
	rf.mu.Lock()
	if args.Term > rf.term {
		rf.term = args.Term
		rf.votedFor = -1
		if rf.status != Follower {
			rf.status = Follower
			go rf.ticker()
		}
		rf.persist()
	}
	if args.LastLogTerm > rf.logs[len(rf.logs)-1].Term || (args.LastLogTerm == rf.logs[len(rf.logs)-1].Term && args.LastLogIndex >= len(rf.logs)-1) {
		if rf.votedFor == -1 || rf.votedFor == args.CandidateId {
			reply.VoteGranted = true
			rf.votedFor = args.CandidateId
			rf.persist()
		} else {
			reply.VoteGranted = false
		}
	} else {
		reply.VoteGranted = false
	}
	reply.Term = rf.term
	rf.mu.Unlock()

	// Your code here (2A, 2B).
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
func (rf *Raft) sendRequestVote(server int, args *RequestVoteArgs, reply *RequestVoteReply, count *int32) bool {
	ok := rf.peers[server].Call("Raft.RequestVote", args, reply)
	if reply.VoteGranted {
		atomic.AddInt32(count, 1)
	}
	return ok
}

type AppendEntriesArgs struct {
	Term         int
	LeaderId     int
	PrevLogIndex int
	PrevLogTerm  int
	Entries      []Entry
	LeaderCommit int
}
type Entry struct {
	Command interface{}
	Term    int
}

type AppendEntriesReply struct {
	Term        int
	Success     bool
	FailedIndex int
	FailedTerm  int
}

func (rf *Raft) AppendEntries(args *AppendEntriesArgs, reply *AppendEntriesReply) {
	rf.mu.Lock()
	if args.Term < rf.term {
		//log.Printf("AppendEntries %v,%d", args, rf.term)
		reply.Success = false
		reply.Term = rf.term
	} else {
		if rf.status != Follower || args.Term > rf.term {
			rf.term = args.Term
			rf.votedFor = -1
			rf.persist()
			if rf.status != Follower {
				rf.status = Follower
				go rf.ticker()
			}
		}
		reply.Term = rf.term
		//log.Printf("AppendEntries %v,%d",args, len(rf.logs))
		if len(rf.logs) < args.PrevLogIndex+1 || rf.logs[args.PrevLogIndex].Term != args.PrevLogTerm {
			//log.Printf("AppendEntries %v,%d",args, len(rf.logs))
			reply.Success = false
			if len(rf.logs) < args.PrevLogIndex+1 {
				reply.FailedTerm = rf.logs[len(rf.logs)-1].Term
				reply.FailedIndex = len(rf.logs) - 1
				if reply.FailedIndex == 0 {
					reply.FailedIndex = 1
				}
			} else {
				reply.FailedTerm = rf.logs[args.PrevLogIndex].Term
				reply.FailedIndex = args.PrevLogIndex
				for rf.logs[reply.FailedIndex-1].Term == reply.FailedTerm {
					reply.FailedIndex--
				}
			}
		} else {
			reply.Success = true
			for i, j := args.PrevLogIndex+1, 0; i < len(rf.logs) && j < len(args.Entries); i, j = i+1, j+1 {
				if args.Entries[j].Term != rf.logs[i].Term {
					rf.logs = rf.logs[:i]
					break
				}
			}
			n := len(rf.logs) - 1 - args.PrevLogIndex
			//log.Printf("len:%d,args.PrevLogIndex:%d",len(rf.logs),args.PrevLogIndex)
			if n < len(args.Entries) {
				rf.logs = append(rf.logs, args.Entries[n:]...)
				rf.persist()
			}
			//log.Printf("%d,%d,%v",args.LeaderCommit,rf.commitIndex,reply)
			if args.LeaderCommit > rf.commitIndex {
				lastCommit := rf.commitIndex
				if args.LeaderCommit < len(rf.logs)-1 {
					rf.commitIndex = args.LeaderCommit
				} else {
					rf.commitIndex = len(rf.logs) - 1
				}
				for i := lastCommit + 1; i <= rf.commitIndex; i++ {
					apply := ApplyMsg{
						CommandValid:  true,
						Command:       rf.logs[i].Command,
						CommandIndex:  i,
						SnapshotValid: false,
						Snapshot:      nil,
						SnapshotTerm:  0,
						SnapshotIndex: 0,
					}
					rf.applyChan <- apply
				}
			}
		}
		//log.Printf("AppendEntries %v,%d",reply, len(rf.logs))
		select {
		case rf.heartbeats <- true:
		default:
			break
		}
	}
	rf.mu.Unlock()
}
func (rf *Raft) heartBeats() {
	for rf.killed() == false {
		rf.mu.RLock()
		term := rf.term
		if rf.status != Leader {
			rf.mu.RUnlock()
			return
		}
		for i := 0; i < len(rf.peers); i++ {
			if i != rf.me {
				args := AppendEntriesArgs{
					Term:         term,
					LeaderId:     rf.me,
					PrevLogIndex: rf.nextIndex[i] - 1,
					LeaderCommit: rf.commitIndex,
				}
				args.PrevLogTerm = rf.logs[args.PrevLogIndex].Term
				if rf.nextIndex[i] < len(rf.logs) {
					args.Entries = rf.logs[rf.nextIndex[i]:]
				} else {
					args.Entries = make([]Entry, 0)
				}
				//log.Printf("%d,%d",rf.nextIndex[i],len(rf.logs))
				go rf.sendAppendEntries(i, &args)
			}
		}
		rf.mu.RUnlock()
		time.Sleep(time.Millisecond * 50)
	}
}
func (rf *Raft) sendAppendEntries(index int, args *AppendEntriesArgs) {
	reply := AppendEntriesReply{}
	ok := rf.peers[index].Call("Raft.AppendEntries", args, &reply)
	if ok {
		rf.mu.Lock()
		if reply.Success {
			if args.PrevLogIndex+len(args.Entries) > rf.matchIndex[index] {
				rf.matchIndex[index] = args.PrevLogIndex + len(args.Entries)
				rf.nextIndex[index] = args.PrevLogIndex + len(args.Entries) + 1
				var arr = make([]int, len(rf.matchIndex))
				for i := 0; i < len(rf.matchIndex); i++ {
					arr[i] = rf.matchIndex[i]
				}
				sort.Slice(arr, func(i, j int) bool {
					return arr[i] > arr[j]
				})
				n := arr[len(arr)/2-1]
				//log.Printf("%v,%d,%d,%d", arr, rf.commitIndex, rf.matchIndex[index], index)
				if n > rf.commitIndex && rf.logs[n].Term == rf.term {
					lastCommit := rf.commitIndex
					rf.commitIndex = n
					for i := lastCommit + 1; i <= rf.commitIndex; i++ {
						apply := ApplyMsg{
							CommandValid:  true,
							Command:       rf.logs[i].Command,
							CommandIndex:  i,
							SnapshotValid: false,
							Snapshot:      nil,
							SnapshotTerm:  0,
							SnapshotIndex: 0,
						}
						rf.applyChan <- apply
					}
				}
			}
		} else {
			if rf.term < reply.Term {
				rf.status = Follower
				rf.votedFor = -1
				rf.term = reply.Term
				go rf.ticker()
				rf.persist()
			} else {
				//log.Printf("index:%d,%v,reply:%v,%d",index,args,reply,rf.nextIndex[index])
				if args.Term >= reply.Term && rf.nextIndex[index] > args.PrevLogIndex {
					rf.nextIndex[index] = reply.FailedIndex
				}
			}
		}
		rf.mu.Unlock()
	}
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
	rf.mu.Lock()
	defer rf.mu.Unlock()
	if rf.status != Leader {
		return -1, -1, false
	}
	entry := Entry{
		Command: command,
		Term:    rf.term,
	}
	rf.logs = append(rf.logs, entry)
	rf.persist()
	// Your code here (2B).

	return len(rf.logs) - 1, rf.term, true
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
}

func (rf *Raft) killed() bool {
	z := atomic.LoadInt32(&rf.dead)
	return z == 1
}

// The ticker go routine starts a new election if this peer hasn't received
// heartbeats recently.
func (rf *Raft) ticker() {

	for rf.killed() == false {
		rf.mu.RLock()
		if rf.status != Follower {
			rf.mu.RUnlock()
			return
		} else {
			rf.mu.RUnlock()
		}
		select {
		case <-rf.heartbeats:
			continue
		case <-time.After(time.Duration(rf.timeout) * time.Millisecond):
			rf.mu.Lock()
			if rf.status == Follower {
				rf.status = Candidate
				go rf.election()
			}
			rf.mu.Unlock()
			return
		}
	}
}
func (rf *Raft) election() {
	for rf.killed() == false {
		rf.mu.Lock()
		if rf.status != Candidate {
			rf.mu.Unlock()
			return
		}
		rf.term += 1
		term := rf.term
		rf.votedFor = rf.me
		rf.persist()
		rf.timeout = rf.random.Int63n(500) + 1000
		rf.mu.Unlock()
		//log.Printf("Candidate:%d,term:%d", rf.me, rf.term)
		startTime := time.Now().Add(time.Duration(rf.timeout) * time.Millisecond)
		var count int32
		count = 1
		for i := 0; i < len(rf.peers); i++ {
			if i != rf.me {
				args := RequestVoteArgs{
					Term:        term,
					CandidateId: rf.me,
				}
				args.LastLogIndex = len(rf.logs) - 1
				args.LastLogTerm = rf.logs[args.LastLogIndex].Term
				replay := RequestVoteReply{}
				go rf.sendRequestVote(i, &args, &replay, &count)
			}
		}
		for atomic.LoadInt32(&count) <= int32(len(rf.peers)/2) && startTime.After(time.Now()) {

		}
		//log.Printf("id: %d,vote: %d", rf.me, count)
		if atomic.LoadInt32(&count) > int32(len(rf.peers)/2) {
			rf.mu.Lock()
			if rf.status == Candidate {
				rf.status = Leader
				index := len(rf.logs)
				for i := 0; i < len(rf.nextIndex); i++ {
					rf.nextIndex[i] = index
				}
				//log.Printf("leader:%d,term:%d,count:%d", rf.me, rf.term, count)
				go rf.heartBeats()
			}
			rf.mu.Unlock()
			return
		}
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
	rf.applyChan = applyCh
	rf.peers = peers
	rf.persister = persister
	rf.me = me
	rf.status = Follower
	rf.votedFor = -1
	rf.term = 0
	rf.heartbeats = make(chan bool)
	rf.random = rand.New(rand.NewSource(int64(me)))
	rf.timeout = rf.random.Int63n(500) + 1000
	rf.logs = make([]Entry, 1)
	rf.commitIndex = 0
	rf.lastApplied = 0
	rf.matchIndex = make([]int, len(peers))
	rf.nextIndex = make([]int, len(peers))
	// Your initialization code here (2A, 2B, 2C).

	// initialize from state persisted before a crash
	rf.readPersist(persister.ReadRaftState())

	// start ticker goroutine to start elections
	go rf.ticker()

	return rf
}

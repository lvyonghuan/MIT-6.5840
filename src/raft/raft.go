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
	//	"bytes"
	"math/rand"
	"sync"
	"sync/atomic"
	"time"

	//	"6.5840/labgob"
	"6.5840/labrpc"
)

// as each Raft peer becomes aware that successive log entries are
// committed, the peer should send an ApplyMsg to the service (or
// tester) on the same server, via the applyCh passed to Make(). set
// CommandValid to true to indicate that the ApplyMsg contains a newly
// committed log entry.
//
// in part 3D you'll want to send other kinds of messages (e.g.,
// snapshots) on the applyCh, but set CommandValid to false for these
// other uses.
type ApplyMsg struct {
	CommandValid bool
	Command      interface{}
	CommandIndex int

	// For 3D:
	SnapshotValid bool
	Snapshot      []byte
	SnapshotTerm  int
	SnapshotIndex int
}

// A Go object implementing a single Raft peer.
type Raft struct {
	mu        sync.Mutex          // Lock to protect shared access to this peer's state
	peers     []*labrpc.ClientEnd // RPC end points of all peers(所有节点的RPC端点)
	persister *Persister          // Object to hold this peer's persisted state
	me        int                 // this peer's index into peers[](也就是自己的ID)
	dead      int32               // set by Kill()

	// Your data here (3A, 3B, 3C).

	//3A代码
	term        int  //当前任期
	isLeader    bool //是否是leader
	votedFor    int  //投票给谁(引用me)
	getVotedNum int  //候选人得票数
	//isGetHeart  bool       //当前任期是否收到领导人心跳
	doElection int        //选举控制-选举轮数,控制退避
	heartLock  sync.Mutex //心跳锁

	getHeartChan        chan struct{} //心跳接收器
	controlGetHeartChan chan struct{} //心跳接受器控制器

	// Look at the paper's Figure 2 for a description of what
	// state a Raft server must maintain.

}

// return currentTerm and whether this server
// believes it is the leader.
func (rf *Raft) GetState() (int, bool) {
	return rf.term, rf.isLeader
}

// save Raft's persistent state to stable storage,
// where it can later be retrieved after a crash and restart.
// see paper's Figure 2 for a description of what should be persistent.
// before you've implemented snapshots, you should pass nil as the
// second argument to persister.Save().
// after you've implemented snapshots, pass the current snapshot
// (or nil if there's not yet a snapshot).
func (rf *Raft) persist() {
	// Your code here (3C).
	// Example:
	// w := new(bytes.Buffer)
	// e := labgob.NewEncoder(w)
	// e.Encode(rf.xxx)
	// e.Encode(rf.yyy)
	// raftstate := w.Bytes()
	// rf.persister.Save(raftstate, nil)
}

// restore previously persisted state.
func (rf *Raft) readPersist(data []byte) {
	if data == nil || len(data) < 1 { // bootstrap without any state?
		return
	}
	// Your code here (3C).
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
}

// the service says it has created a snapshot that has
// all info up to and including index. this means the
// service no longer needs the log through (and including)
// that index. Raft should now trim its log as much as possible.
func (rf *Raft) Snapshot(index int, snapshot []byte) {
	// Your code here (3D).

}

// example RequestVote RPC arguments structure.
// field names must start with capital letters!
type RequestVoteArgs struct {
	// Your data here (3A, 3B).
	Term        int //候选人的任期
	CandidateId int //候选人ID
}

// example RequestVote RPC reply structure.
// field names must start with capital letters!
type RequestVoteReply struct {
	// Your data here (3A).
	Term        int  //当前任期
	VoteGranted bool //是否投票
}

// example RequestVote RPC handler.
func (rf *Raft) RequestVote(args *RequestVoteArgs, reply *RequestVoteReply) {
	// Your code here (3A, 3B).
	//若term < currentTerm返回false
	//log.Println(rf.me, " get vote request, term:", args.Term, " currentTerm:", rf.term)

	rf.heartLock.Lock()
	if args.Term < rf.term {
		rf.heartLock.Unlock()
		reply.Term = rf.term
		reply.VoteGranted = false
		return
	} else {
		rf.heartLock.Unlock()
	}
	//节点任期更新
	rf.term = args.Term

	//若还未投票，则投票给候选人
	if rf.votedFor == -1 {
		rf.votedFor = args.CandidateId
		reply.Term = rf.term
		reply.VoteGranted = true
		return
	} else {
		reply.Term = rf.term
		reply.VoteGranted = false
		return
	}
}

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
func (rf *Raft) sendRequestVote(server int, args *RequestVoteArgs, reply *RequestVoteReply) bool {
	if rf.doElection == 0 {
		return false
	}

	ok := rf.peers[server].Call("Raft.RequestVote", args, reply)
	return ok
}

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
func (rf *Raft) Start(command interface{}) (int, int, bool) {
	index := -1
	term := -1
	isLeader := true

	// Your code here (3B).

	return index, term, isLeader
}

// the tester doesn't halt goroutines created by Raft after each test,
// but it does call the Kill() method. your code can use killed() to
// check whether Kill() has been called. the use of atomic avoids the
// need for a lock.
//
// the issue is that long-running goroutines use memory and may chew
// up CPU time, perhaps causing later tests to fail and generating
// confusing debug output. any goroutine with a long-running loop
// should call killed() to check whether it should stop.
func (rf *Raft) Kill() {
	atomic.StoreInt32(&rf.dead, 1)
	// Your code here, if desired.
}

func (rf *Raft) killed() bool {
	z := atomic.LoadInt32(&rf.dead)
	return z == 1
}

func (rf *Raft) ticker() {
	//开始选举随机等待
	ms := rand.Int63() % 10
	time.Sleep(time.Duration(ms) * time.Millisecond)
	//初始化心跳控制管道
	rf.controlGetHeartChan <- struct{}{}

	for rf.killed() == false {
		// Your code here (3A)
		// Check if a leader election should be started.

		//如果当前节点不是leader
		select {
		case <-rf.getHeartChan:
			//重置心跳控制管道
			rf.controlGetHeartChan <- struct{}{}
			//log.Println(rf.me, " get heart")
		default:
			//log.Println(rf.me, " no heart")
			rf.heartLock.Lock()
			if rf.isLeader == false {
				//log.Println(rf.me, "start election")
				//如果当前任期内没有给其他节点投票
				if rf.votedFor == -1 {
					rf.heartLock.Unlock()
					rf.election()
				} else {
					rf.heartLock.Unlock()
				}
			} else {
				rf.heartLock.Unlock()
			}
		}

		//重置投票
		rf.votedFor = -1

		// pause for a random amount of time between 50 and 350
		// milliseconds.
		ms := 50 + (rand.Int63() % 300)
		time.Sleep(time.Duration(ms) * time.Millisecond)
	}
}

// the service or tester wants to create a Raft server. the ports
// of all the Raft servers (including this one) are in peers[]. this
// server's port is peers[me]. all the servers' peers[] arrays
// have the same order. persister is a place for this server to
// save its persistent state, and also initially holds the most
// recent saved state, if any. applyCh is a channel on which the
// tester or service expects Raft to send ApplyMsg messages.
// Make() must return quickly, so it should start goroutines
// for any long-running work.
func Make(peers []*labrpc.ClientEnd, me int,
	persister *Persister, applyCh chan ApplyMsg) *Raft {
	rf := &Raft{}
	rf.peers = peers
	rf.persister = persister
	rf.me = me

	// Your initialization code here (3A, 3B, 3C).

	//3A
	rf.term = 0
	rf.votedFor = -1
	//初始化心跳控制管道
	rf.getHeartChan = make(chan struct{}, 1)
	rf.controlGetHeartChan = make(chan struct{}, 1)

	// initialize from state persisted before a crash
	rf.readPersist(persister.ReadRaftState())

	// start ticker goroutine to start elections
	go rf.ticker()

	return rf
}

// 选举
func (rf *Raft) election() {
	//log.Println(rf.me, " start vote")
	//成为候选人，进行选举
	rf.term++           //任期自增
	rf.votedFor = rf.me //投票给自己
	rf.getVotedNum = 1  //得票数初始化为1

	//选举退避
	if rf.doElection > 1 {
		//log.Println(rf.me, "第", rf.doElection, "次选举")
		ms := rand.Int63() % int64(rf.doElection*20)
		if ms > 280 {
			ms = 280
		}
		time.Sleep(time.Duration(ms) * time.Millisecond)
	} else {
		rf.doElection = 1
	}

	//发送投票请求
	for i := 0; i < len(rf.peers); i++ {
		if rf.doElection == 0 {
			return
		}

		if i != rf.me {
			go func(i int) {
				args := RequestVoteArgs{}
				args.Term = rf.term
				args.CandidateId = rf.me
				reply := RequestVoteReply{}
				rf.sendRequestVote(i, &args, &reply)
				if reply.VoteGranted == true {
					rf.getVotedNum++
				}
			}(i)
		}
	}

	//等待投票结果
	time.Sleep(5 * time.Millisecond)

	//log.Println(rf.me, " get vote num:", rf.getVotedNum)
	//如果得票数大于半数，则成为leader
	if rf.getVotedNum > (len(rf.peers))/2 {
		//log.Println(rf.me, " become leader")
		rf.isLeader = true
		go rf.leaderSendHeart()
	} else {
		rf.doElection++ //退避
	}
}

// AppendEntries 心跳结构
type AppendEntries struct {
	Term     int
	LeaderId int
}

// AppendEntriesReplayStruct 心跳回复结构
type AppendEntriesReplayStruct struct {
	Term    int
	Success bool
}

func (rf *Raft) leaderSendHeart() {
	for {
		for i := 0; i < len(rf.peers); i++ {
			if !rf.isLeader || rf.killed() {
				//log.Println(rf.me, " stop leader")
				return
			}

			if i != rf.me {
				go func(i int) {
					rf.appendEntriesRequest(i)
				}(i)
			}
		}

		time.Sleep(20 * time.Millisecond)
	}
}

// 领导人发起日志复制请求
// 输入节点ID
func (rf *Raft) appendEntriesRequest(i int) bool {
	if !rf.isLeader {
		return false
	}

	args := AppendEntries{}
	args.Term = rf.term
	args.LeaderId = rf.me

	reply := AppendEntriesReplayStruct{}

	ok := rf.peers[i].Call("Raft.RequestAppendEntries", &args, &reply)

	//如果心跳回复的任期大于当前任期，则更新当前任期，并且不是leader
	if reply.Term > rf.term {
		rf.mu.Lock()
		rf.term = reply.Term
		rf.isLeader = false
		rf.mu.Lock()
	}

	return ok
}

// RequestAppendEntries 日志复制请求
func (rf *Raft) RequestAppendEntries(args *AppendEntries, reply *AppendEntriesReplayStruct) {
	reply.Term = rf.term

	//log.Println(rf.me, " get heart, term:", args.Term, " currentTerm:", rf.term)
	//检查任期
	if args.Term < rf.term {
		reply.Success = false
		return
	}
	//管道控制心跳
	select {
	case <-rf.controlGetHeartChan:
		rf.getHeartChan <- struct{}{} //在接收到心跳管道控制器的消息后，向心跳管道发送消息
		//log.Println(rf.me, " get a heart")
	default:
		//什么都不做
	}

	rf.heartLock.Lock()
	rf.term = args.Term
	rf.isLeader = false
	rf.doElection = 0
	rf.heartLock.Unlock()

	rf.votedFor = -1 //收到心跳后，重置投票
	reply.Success = true
}

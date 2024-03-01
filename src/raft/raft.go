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
// in part 2D you'll want to send other kinds of messages (e.g.,
// snapshots) on the applyCh, but set CommandValid to false for these
// other uses.
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

type LogEntry struct {
	Term    int
	Command interface{}
}

type ServerState int

const (
	Leader ServerState = iota
	Candidate
	Follower
)

// A Go object implementing a single Raft peer.
type Raft struct {
	mu        sync.Mutex          // Lock to protect shared access to this peer's state
	peers     []*labrpc.ClientEnd // RPC end points of all peers
	persister *Persister          // Object to hold this peer's persisted state
	me        int                 // this peer's index into peers[]
	dead      int32               // set by Kill()

	// Your data here (2A, 2B, 2C).
	// Look at the paper's Figure 2 for a description of what
	// state a Raft server must maintain.
	applyCh chan ApplyMsg

	state       ServerState
	lastReceive time.Time

	currentTerm int
	votedFor    int
	log         []LogEntry

	commitIndex int
	lastApplied int

	nextIndex  []int
	matchIndex []int
}

// return currentTerm and whether this server
// believes it is the leader.
func (rf *Raft) GetState() (int, bool) {

	var term int
	var isleader bool
	// Your code here (2A).
	rf.mu.Lock()
	defer rf.mu.Unlock()
	term = rf.currentTerm
	isleader = rf.state == Leader
	return term, isleader
}

// save Raft's persistent state to stable storage,
// where it can later be retrieved after a crash and restart.
// see paper's Figure 2 for a description of what should be persistent.
// before you've implemented snapshots, you should pass nil as the
// second argument to persister.Save().
// after you've implemented snapshots, pass the current snapshot
// (or nil if there's not yet a snapshot).
func (rf *Raft) persist() {
	// Your code here (2C).
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
	// Your code here (2C).
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
	// Your code here (2D).

}

type AppendType int

type AppendEntriesArgs struct {
	Type         AppendType
	Term         int
	LeaderId     int
	PrevLogIndex int
	PrevLogTerm  int
	Entries      []LogEntry
	LeaderCommit int
}

const (
	Heartbeat AppendType = iota
	Replicate
	Commit
)

type AppendEntriesReply struct {
	Term    int
	Success bool
}

func (rf *Raft) AppendEntries(args *AppendEntriesArgs, reply *AppendEntriesReply) {
	rf.mu.Lock()
	defer rf.mu.Unlock()

	rf.lastReceive = time.Now()
	if args.Term < rf.currentTerm {
		reply.Success = false
		reply.Term = rf.currentTerm
		return
	}

	if args.Term >= rf.currentTerm {
		if rf.state == Leader {
			DPrintf("%d back to follower by AppendEntries", rf.me)
		}
		convertToFollower(rf, args.Term)
	}

	// if args.Term >= rf.currentTerm {
	// 	rf.currentTerm = args.Term
	// }

	if args.Type == Heartbeat {
		// DPrintf("%d got heartbeat from %d", rf.me, args.LeaderId)
		reply.Success = true
		reply.Term = rf.currentTerm
		return
	}

	if args.Type == Replicate {
		term, index := getLastEntryInfo(rf)
		if term == -1 && index == -1 && args.PrevLogIndex == -1 && args.PrevLogTerm == -1 {
			// initial log (I set dummy log when starting the raft, so this if code is not needed)
			reply.Success = true
			rf.log = append(rf.log, args.Entries...)
			return
		}
		if args.PrevLogIndex > index {
			reply.Success = false
			return
		}
		if args.PrevLogIndex == index && args.PrevLogTerm != term {
			rf.log = rf.log[:len(rf.log)-1]
			reply.Success = false
			return
		}
		// there can't be a scenario that args.PrevLogIndex < index
		reply.Success = true
		rf.log = append(rf.log, args.Entries...)
		return
	}

	if args.Type == Commit {
		if rf.commitIndex >= args.LeaderCommit {
			reply.Success = false
			return
		}
		rf.commitIndex = args.LeaderCommit
		DPrintf("%d rf.commitIndex: %d", rf.me, rf.commitIndex)
		go rf.applyLocalMachine(rf.commitIndex)
		return
	}
}

func (rf *Raft) applyLocalMachine(currCommitIndex int) {
	rf.mu.Lock()
	DPrintf("%d, %d", currCommitIndex, rf.commitIndex)
	if currCommitIndex < rf.commitIndex {
		rf.mu.Unlock()
		return
	}
	applyMsg := ApplyMsg{
		CommandValid: true,
		Command:      rf.log[rf.lastApplied+1].Command,
		CommandIndex: rf.lastApplied + 1,
	}
	rf.lastApplied++
	rf.mu.Unlock()
	rf.applyCh <- applyMsg
	rf.mu.Lock()
	if currCommitIndex < rf.commitIndex {
		rf.mu.Unlock()
		return
	}
	if rf.lastApplied < rf.commitIndex {
		go rf.applyLocalMachine(currCommitIndex)
	}
	rf.mu.Unlock()
}

func (rf *Raft) sendAppendEntries(server int, args *AppendEntriesArgs, reply *AppendEntriesReply) bool {
	ok := rf.peers[server].Call("Raft.AppendEntries", args, reply)
	return ok
}

// example RequestVote RPC arguments structure.
// field names must start with capital letters!
type RequestVoteArgs struct {
	// Your data here (2A, 2B).
	Term         int
	CandidateId  int
	LastLogIndex int
	LastLogTerm  int
}

// example RequestVote RPC reply structure.
// field names must start with capital letters!
type RequestVoteReply struct {
	// Your data here (2A).
	Term         int
	VotedGranted bool
}

// example RequestVote RPC handler.
func (rf *Raft) RequestVote(args *RequestVoteArgs, reply *RequestVoteReply) {
	// Your code here (2A, 2B).
	rf.mu.Lock()
	defer rf.mu.Unlock()
	lastTerm, lastIndex := getLastEntryInfo(rf)
	isLogUpToDate := args.LastLogTerm > lastTerm || (args.LastLogTerm == lastTerm && args.LastLogIndex >= lastIndex)
	// isVoted := args.Term > rf.currentTerm || args.Term == rf.currentTerm && (rf.votedFor == args.CandidateId)
	// isVoted := args.Term >= rf.currentTerm && (rf.votedFor ==-1 || rf.votedFor == args.CandidateId)

	rf.lastReceive = time.Now()
	if args.Term > rf.currentTerm {
		if rf.state == Leader {
			DPrintf("%d back to follower : args.Term : %d, args.CandidateId : %d, rf.currentTerm : %d", rf.me, args.Term, args.CandidateId, rf.currentTerm)
		}
		convertToFollower(rf, args.Term)
	}

	isVoted := args.Term >= rf.currentTerm && (rf.votedFor == -1 || rf.votedFor == args.CandidateId)

	if isVoted && isLogUpToDate {
		reply.VotedGranted = true
		rf.votedFor = args.CandidateId
	} else {
		reply.VotedGranted = false
	}
}

func getLastEntryInfo(rf *Raft) (int, int) {
	lastIndex := len(rf.log) - 1
	term := -1
	if lastIndex >= 0 {
		term = rf.log[lastIndex].Term
	}
	return term, lastIndex
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

	// Your code here (2B).
	rf.mu.Lock()
	defer rf.mu.Unlock()
	isLeader = rf.state == Leader
	if !isLeader {
		return -1, -1, isLeader
	}
	DPrintf("%d starts new command", rf.me)
	logEntry := LogEntry{Term: rf.currentTerm, Command: command}
	rf.log = append(rf.log, logEntry)
	_, index = getLastEntryInfo(rf)

	for i := 0; i < len(rf.peers); i++ {
		if rf.me == i {
			continue
		}
		go rf.appendEntriesToFollower(i, rf.currentTerm, index)
	}
	return index, term, true
}

func (rf *Raft) appendEntriesToFollower(followerId int, requestTerm int, requestIndex int) {
	ms := 25
	for {
		rf.mu.Lock()
		_, lastLogIndex := getLastEntryInfo(rf)
		// fmt.Println("-----------------")
		// fmt.Println(rf.state)
		// fmt.Println(rf.currentTerm)
		// fmt.Println(requestTerm)
		// fmt.Println(lastLogIndex)
		// fmt.Println(requestIndex)
		if rf.state != Leader || rf.currentTerm != requestTerm || lastLogIndex != requestIndex {
			rf.mu.Unlock()
			break
		}
		DPrintf("appendEntriesToFOllowers followerId: %d, requestTerm: %d, requestIndex: %d", followerId, requestTerm, requestIndex)
		startIndex := rf.nextIndex[followerId]
		entries := rf.log[startIndex:]
		prevLogIndex := startIndex - 1
		prevLogTerm := -1
		if prevLogIndex >= 0 {
			prevLogTerm = rf.log[prevLogIndex].Term
		}

		args := AppendEntriesArgs{
			Type:         Replicate,
			Term:         rf.currentTerm,
			LeaderId:     rf.me,
			PrevLogIndex: prevLogIndex,
			PrevLogTerm:  prevLogTerm,
			Entries:      entries,
		}
		reply := AppendEntriesReply{}
		ok := rf.sendAppendEntries(followerId, &args, &reply)
		if !ok {
			rf.mu.Unlock()
			time.Sleep(time.Duration(ms) * time.Millisecond)
			continue
		}
		if reply.Success {
			rf.nextIndex[followerId] = len(rf.log)
			rf.matchIndex[followerId] = len(rf.log) - 1
			go rf.triggerCommit(followerId, rf.matchIndex[followerId])
			rf.mu.Unlock()
			break
		} else {
			if rf.nextIndex[followerId] > 0 {
				rf.nextIndex[followerId]--
			}
		}
		rf.mu.Unlock()
		time.Sleep(time.Duration(ms) * time.Millisecond)
	}
}

func (rf *Raft) triggerCommit(followerId int, index int) {
	rf.mu.Lock()
	DPrintf("%d trigger commit at term : %d, index : %d", rf.me, rf.currentTerm, index)

	if rf.commitIndex >= index {
		rf.mu.Unlock()
		return
	}
	numCommit := 1
	for i := 0; i < len(rf.peers); i++ {
		if rf.me == i {
			continue
		}
		if rf.matchIndex[i] >= index {
			numCommit++
		}
	}
	DPrintf("%d numCommit: %d", rf.me, numCommit)
	if numCommit <= len(rf.peers)/2 {
		rf.mu.Unlock()
		return
	}
	rf.commitIndex = index
	go rf.applyLocalMachine(rf.commitIndex)
	for i := 0; i < len(rf.peers); i++ {
		if rf.me == i {
			continue
		}
		go rf.commitToFollower(i, index)
	}
	rf.mu.Unlock()
}

func (rf *Raft) commitToFollower(followerId int, index int) {
	ms := 25
	for {
		rf.mu.Lock()
		DPrintf("commitToFollower rf.commitIndex: %d, index: %d, followerId: %d", rf.commitIndex, index, followerId)
		if rf.commitIndex != index {
			rf.mu.Unlock()
			break
		}
		args := AppendEntriesArgs{
			Type:         Commit,
			Term:         rf.currentTerm,
			LeaderId:     rf.me,
			LeaderCommit: index,
		}
		reply := AppendEntriesReply{}
		DPrintf("%d : send commit to followerId : %d", rf.me, followerId)
		ok := rf.sendAppendEntries(followerId, &args, &reply)
		if ok {
			rf.mu.Unlock()
			break
		}
		DPrintf("commitToFollower failed rf.commitIndex: %d, index: %d, followerId: %d", rf.commitIndex, index, followerId)
		rf.mu.Unlock()
		time.Sleep(time.Duration(ms) * time.Millisecond)
	}
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
	for rf.killed() == false {

		// Your code here (2A)
		// Check if a leader election should be started.
		electionTimeout := 150 + (rand.Int63() % 150)
		rf.mu.Lock()
		tnow := time.Now()
		if rf.lastReceive.Add(time.Duration(electionTimeout)*time.Millisecond).Before(tnow) && rf.state != Leader {
			DPrintf("%d kicks off election", rf.me)
			DPrintf("%s, %s", tnow, rf.lastReceive)
			go rf.KickoffElection()
		}
		rf.mu.Unlock()
		// pause for a random amount of time between 50 and 350
		// milliseconds.
		ms := 50 + (rand.Int63() % 300)
		time.Sleep(time.Duration(ms) * time.Millisecond)
	}
}

func convertToFollower(rf *Raft, term int) {
	rf.state = Follower
	rf.currentTerm = term
	rf.votedFor = -1
	rf.lastReceive = time.Now()
}

func (rf *Raft) KickoffElection() {
	rf.mu.Lock()
	rf.state = Candidate
	rf.votedFor = rf.me
	rf.currentTerm++
	rf.lastReceive = time.Now()
	term, lastIndex := getLastEntryInfo(rf)
	args := RequestVoteArgs{
		Term:         rf.currentTerm,
		CandidateId:  rf.me,
		LastLogIndex: lastIndex,
		LastLogTerm:  term,
	}
	numVote := 1
	finishedCnt := 1
	rf.mu.Unlock()
	cond := sync.NewCond(&rf.mu)
	for i := 0; i < len(rf.peers); i++ {
		if i == rf.me {
			continue
		}
		go func(p int) {
			reply := RequestVoteReply{}
			ok := rf.sendRequestVote(p, &args, &reply)
			rf.mu.Lock()
			defer rf.mu.Unlock()
			defer cond.Broadcast()
			finishedCnt++
			if !ok {
				return
			}
			if reply.Term > rf.currentTerm {
				convertToFollower(rf, reply.Term)
				return
			}
			if reply.VotedGranted {
				numVote++
			}
		}(i)
	}
	rf.mu.Lock()
	defer rf.mu.Unlock()
	for numVote <= len(rf.peers)/2 && finishedCnt < len(rf.peers) {
		cond.Wait()
		if rf.state == Follower {
			break
		}
	}
	DPrintf("%d request result at term : %d - numVote : %d", rf.me, rf.currentTerm, numVote)
	if numVote > len(rf.peers)/2 && rf.state == Candidate {
		DPrintf("%d is a new leader at term : %d", rf.me, rf.currentTerm)
		rf.state = Leader
		rf.lastReceive = time.Now()
		go rf.sendHeartbeat()
	}
}

func (rf *Raft) sendHeartbeat() {
	for rf.state == Leader {
		rf.mu.Lock()
		args := AppendEntriesArgs{
			Type:     Heartbeat,
			Term:     rf.currentTerm,
			LeaderId: rf.me,
		}
		// DPrintf("%d sending heartbeat", rf.me)
		for i := 0; i < len(rf.peers); i++ {
			if i == rf.me {
				continue
			}
			go func(p int) {
				rf.mu.Lock()
				defer rf.mu.Unlock()
				reply := AppendEntriesReply{}
				ok := rf.sendAppendEntries(p, &args, &reply)
				if !ok {
					return
				}
				if reply.Term > rf.currentTerm {
					DPrintf("%d found bigger Term", rf.me)
					convertToFollower(rf, reply.Term)
				}
			}(i)
		}
		rf.mu.Unlock()
		ms := 25
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

	// Your initialization code here (2A, 2B, 2C).
	rf.state = Follower
	rf.currentTerm = 0
	rf.votedFor = -1
	rf.lastReceive = time.Now()
	rf.applyCh = applyCh

	rf.log = append(rf.log, LogEntry{Term: 0, Command: nil})

	rf.commitIndex = 0
	rf.lastApplied = 0

	nextIndex := make([]int, len(peers))
	matchIndex := make([]int, len(peers))
	for i := range matchIndex {
		nextIndex[i] = 1
	}
	for i := range matchIndex {
		matchIndex[i] = 0
	}

	rf.nextIndex = nextIndex
	rf.matchIndex = matchIndex

	// initialize from state persisted before a crash
	rf.readPersist(persister.ReadRaftState())

	// start ticker goroutine to start elections
	go rf.ticker()

	return rf
}

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
	"bytes"
	"math/rand"
	"sync"
	"sync/atomic"
	"time"

	"6.5840/labgob"
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

type RaftState string

const (
	Leader    RaftState = "Leader"
	Candidate RaftState = "Candidate"
	Follower  RaftState = "Follower"
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
	state       RaftState
	currentTerm int
	votedFor    int
	votes       int

	log []LogEntry

	commitIndex int // this is not inclusive
	lastApplied int

	nextIndex  []int
	matchIndex []int

	heartbeatInterval int
	electionInterval  int

	applyCh     chan ApplyMsg
	leaderCh    chan bool
	followerCh  chan bool
	voteCh      chan bool
	heartbeatCh chan bool
}

// return currentTerm and whether this server
// believes it is the leader.
func (rf *Raft) GetState() (int, bool) {

	rf.mu.Lock()
	defer rf.mu.Unlock()
	// Your code here (2A).
	return rf.currentTerm, rf.state == Leader
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
	w := new(bytes.Buffer)
	e := labgob.NewEncoder(w)

	if e.Encode(rf.currentTerm) != nil ||
		e.Encode(rf.votedFor) != nil ||
		e.Encode(rf.log) != nil {
		panic("encode failed")
	}

	raftstate := w.Bytes()
	rf.persister.Save(raftstate, nil)
	Debug(dPersist, "S%d: (%d) persisted", rf.me, rf.currentTerm)
}

// restore previously persisted state.
func (rf *Raft) readPersist(data []byte) {
	if data == nil || len(data) < 1 { // bootstrap without any state?
		return
	}
	// Your code here (2C).
	// Example:
	r := bytes.NewBuffer(data)
	d := labgob.NewDecoder(r)

	var persistTerm int
	var persistVoted int
	var persistLog []LogEntry

	if d.Decode(&persistTerm) != nil ||
		d.Decode(&persistVoted) != nil ||
		d.Decode(&persistLog) != nil {
		panic("readPersist failed")
	}
	rf.currentTerm = persistTerm
	rf.votedFor = persistVoted
	rf.log = persistLog
	Debug(dPersist, "S%d: (%d) read from persist save", rf.me, rf.currentTerm)
}

// the service says it has created a snapshot that has
// all info up to and including index. this means the
// service no longer needs the log through (and including)
// that index. Raft should now trim its log as much as possible.
func (rf *Raft) Snapshot(index int, snapshot []byte) {
	// Your code here (2D).

}

/////////////////////////////////////////////////////////////////////
/////////////////////////////////////////////////////////////////////
/////////////////////                            ////////////////////
/////////////////////////////////////////////////////////////////////
/////////////////////////////////////////////////////////////////////

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
	Term        int
	VoteGranted bool
}

func (rf *Raft) checkLogIfMoreUpdated(candidateLastLogIndex int, candidateLastLogTerm int) bool {
	myLastLogIndex := rf.getLastLogIndex()
	myLastLogTerm := rf.getLastLogTerm()

	if myLastLogTerm < candidateLastLogTerm ||
		(myLastLogTerm == candidateLastLogTerm &&
			myLastLogIndex <= candidateLastLogIndex) {
		return true
	}

	return false
}

// example RequestVote RPC handler.
func (rf *Raft) RequestVote(args *RequestVoteArgs, reply *RequestVoteReply) {
	// Your code here (2A, 2B).
	rf.mu.Lock()
	defer rf.mu.Unlock()
	// defer rf.persist()

	// THIS FUNCTION RUNS WHEN SOMEONE STARTED ELECTION
	// DECIDES WHETHER TO VOTE FOR SOME MACHINE OR NOT

	// this part is chekcing if the machine term is higher than current machine

	if args.Term < rf.currentTerm {
		// request term is lower than this machine, so REJECT
		Debug(dVote, "S%d: no vote. S%d term lower than self. (%d<%d)",
			rf.me, args.CandidateId, args.Term, rf.currentTerm)
		reply.VoteGranted = false
		reply.Term = rf.currentTerm
		return
	} else if args.Term > rf.currentTerm {
		if rf.state != Follower {
			Debug(dInfo, "S%d: Since S%d higher term, change to follower. (%d>%d)",
				rf.me, args.CandidateId, args.Term, rf.currentTerm)
		}

		// JUST BECAUSE TERM IS HIGHER doesn't mean automatically will vote
		// need to check log as well

		// change RaftState to Follower
		rf.becomeFollower(args.Term)
	}

	// configure reply

	if (rf.votedFor == -1 || rf.votedFor == args.CandidateId) &&
		rf.checkLogIfMoreUpdated(args.LastLogIndex, args.LastLogTerm) {

		// only AFTER CHECKING LOGS can this machine decide to vote

		rf.votedFor = args.CandidateId
		reply.VoteGranted = true
		reply.Term = rf.currentTerm

		Debug(dVote, "S%d: No problem with S%d's logs, voted.", rf.me, args.CandidateId)

		rf.persist()

		rf.addToChan(rf.voteCh, true)

	} else {
		reply.VoteGranted = false
		reply.Term = rf.currentTerm

		Debug(dVote, "S%d: S%d's logs are outdated, not voted.", rf.me, args.CandidateId)

		rf.addToChan(rf.voteCh, false)
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
	ok := rf.peers[server].Call("Raft.RequestVote", args, reply)
	if !ok {
		return false
	}

	rf.mu.Lock()
	defer rf.mu.Unlock()

	// request invalid when cur machine is not a candidate
	// or if term has updated already since RequestVote call
	//		cur is candidate => sendRequestVote
	//		=> cur gets RequestVote from someone else
	//		=> updates term => gets reply for own sendRequestVote
	//		=> already too late, so invalid Request
	if rf.state == Leader {
		Debug(dLeader, "S%d: Already LEADER.", rf.me)
		return ok
	} else if rf.state == Follower {
		Debug(dLeader, "S%d: Is a FOLLOWER, not a CANDIDATE.", rf.me)
		return ok
	} else if rf.currentTerm != args.Term || rf.currentTerm > reply.Term {
		Debug(dInfo, "S%d: New term began, no longer valid candidate!", rf.me)
		return ok
	}

	Debug(dVote, "S%d: Got reply... 0_0", rf.me)

	// reply can bring new information
	// e.g. leader has been elected
	if reply.Term > rf.currentTerm {
		Debug(dTerm, "S%d: New leader detected from S%d vote, no longer candidate.",
			rf.me)
		rf.becomeFollower(reply.Term)
		// rf.persist()
		return ok
	}

	// if got vote from the call, check # of votes
	if reply.VoteGranted {
		rf.votes += 1
		votesRequired := len(rf.peers)/2 + 1
		Debug(dVote, "S%d: Got vote, cur %d, need %d.",
			rf.me, rf.votes, votesRequired)

		// check if enough votes
		if rf.votes >= votesRequired {
			// Debug(dLeader, "S%d: enough votes, NEW LEADER.", rf.me)
			// rf.state = Leader
			// rf.heartbeatTime = time.Now()
			rf.addToChan(rf.leaderCh, true)
		}
	}

	return ok
}

func (rf *Raft) raiseElection() {

	rf.mu.Lock()
	defer rf.mu.Unlock()

	// race condition check
	if rf.state != Candidate {
		return
	}

	args := &RequestVoteArgs{
		Term:         rf.currentTerm,
		CandidateId:  rf.me,
		LastLogIndex: rf.getLastLogIndex(),
		LastLogTerm:  rf.getLastLogTerm(),
	}

	for peer := range rf.peers {
		if peer == rf.me {
			continue
		}
		go rf.sendRequestVote(peer, args, &RequestVoteReply{})
	}
}

/////////////////////////////////////////////////////////////////////
/////////////////////                            ////////////////////
/////////////////////////////////////////////////////////////////////
/////////////////////                            ////////////////////
/////////////////////////////////////////////////////////////////////

type AppendEntriesArgs struct {
	Term         int
	LeaderId     int
	PrevLogIndex int
	PrevLogTerm  int
	Entries      []LogEntry
	LeaderCommit int
}

type AppendEntriesReply struct {
	Term          int
	Success       bool
	ConflictIndex int
	ConflictTerm  int
}

// (2A) only heartbeat, so just check whether if leader's term is not behind, do nothing else
func (rf *Raft) AppendEntries(args *AppendEntriesArgs, reply *AppendEntriesReply) {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	defer rf.persist()

	// if "leader" is behind this machine's term
	if args.Term < rf.currentTerm {
		reply.Term = rf.currentTerm
		reply.Success = false
		reply.ConflictIndex = -1
		reply.ConflictTerm = -1
		return
	}

	// if leader is ahead of this machine's term
	if args.Term > rf.currentTerm {
		rf.becomeFollower(args.Term)
	}

	rf.addToChan(rf.heartbeatCh, true)

	myLastLogIndex := rf.getLastLogIndex()

	// *** prevLogIndex := rf.nextIndex[peer] - 1 ***

	// leader log is longer that log of current machine
	if args.PrevLogIndex > myLastLogIndex {
		reply.Term = rf.currentTerm
		reply.Success = false
		reply.ConflictIndex = myLastLogIndex + 1
		reply.ConflictTerm = -1
		Debug(dLog, "S%d: PrevLogIndex longer than this machine's log size (%d>%d)",
			rf.me, args.PrevLogIndex, len(rf.log))
		return
	}

	// first need to check if the log at LastLogIndex is correct
	// if not, can't proceed with append entries
	myTerm := rf.log[args.PrevLogIndex].Term
	if myTerm != args.PrevLogTerm {
		// we have a conflict!!
		Debug(dLog2, "S%d: Conflicting terms, myTerm(%d) != %d.", rf.me, myTerm, args.PrevLogTerm)
		reply.Success = false
		reply.Term = rf.currentTerm
		reply.ConflictTerm = myTerm

		index := args.PrevLogIndex
		// back up to the first log with term == myTerm
		for index >= 0 && rf.log[index].Term == myTerm {
			reply.ConflictIndex = index
			index -= 1
		}

		return
	}

	// at this point, see if truncating is necessary
	i := args.PrevLogIndex + 1
	j := 0
	for i < myLastLogIndex+1 && j < len(args.Entries) {
		if rf.log[i].Term != args.Entries[j].Term {
			Debug(dLog2, "S%d: Bad log entry at %d, dropping", rf.me, i)
			break
		}
		i += 1
		j += 1
	}

	// append correct logs
	rf.log = rf.log[:i]             // truncate until wrong index
	args.Entries = args.Entries[j:] // truncate from wrong index
	Debug(dLog, "S%d: Logs good until %d.", rf.me, i)
	rf.log = append(rf.log, args.Entries...)
	if len(args.Entries) != 0 {
		Debug(dLog, "S%d: Appended from index %d-%d", rf.me, i, i+len(args.Entries)-1)
	} else {
		Debug(dHeart, "S%d: (%d) Got heartbeat.", rf.me, rf.currentTerm)
		Debug(dHeart, "S%d: (%d) Log size %d, last log term %d.", rf.me, rf.currentTerm, rf.getLastLogIndex()+1, rf.getLastLogTerm())
	}

	reply.Success = true
	reply.Term = rf.currentTerm
	reply.ConflictIndex = -1
	reply.ConflictTerm = -1

	// update commitIndex if leader has commited more than this machine
	if rf.commitIndex < args.LeaderCommit {
		// get this again since it has changed after append
		myLastLogIndex = rf.getLastLogIndex()
		// what to update to depends on if this machine has enough entries
		// to catch up to leader's commitIndex
		var newCommitIndex int
		if myLastLogIndex < args.LeaderCommit {
			newCommitIndex = myLastLogIndex
		} else {
			newCommitIndex = args.LeaderCommit
		}
		go rf.commitLogs(newCommitIndex)
	}

}

// calls AppendEntries from the target machine
func (rf *Raft) sendAppendEntries(server int, args *AppendEntriesArgs, reply *AppendEntriesReply) {
	ok := rf.peers[server].Call("Raft.AppendEntries", args, reply)
	if !ok {
		return
	}

	rf.mu.Lock()
	defer rf.mu.Unlock()
	defer rf.persist()

	//////////////////////////////////////////////
	// race condition check
	if rf.state != Leader {
		Debug(dInfo, "S%d: No longer LEADER, sAE invalid.", rf.me)
		return
	} else if rf.currentTerm != args.Term || rf.currentTerm > reply.Term {
		Debug(dInfo, "S%d: New term began, no longer LEADER!", rf.me)
		Debug(dInfo, "S%d: Follow up: either (%d != %d) or (%d > %d).",
			rf.me, rf.currentTerm, args.Term, rf.currentTerm, reply.Term)
		return
	}

	// reply can give info on whether this machine is outdated
	if reply.Term > rf.currentTerm {
		// step down to follower
		Debug(dInfo, "S%d: New term began indicated by AE reply, no longer LEADER!", rf.me)
		rf.becomeFollower(reply.Term)
		return
	}

	if reply.Success {
		// this is when the AppendEntries is successful
		// so update matchIndex[server]
		// -> AS THE LEADER, this server has matched w/ me up to index ___
		match := args.PrevLogIndex + len(args.Entries)
		if match > rf.matchIndex[server] {
			rf.matchIndex[server] = match
		}
		rf.nextIndex[server] = rf.matchIndex[server] + 1
	} else if reply.ConflictTerm < 0 {
		// conflict stays in the same term
		rf.nextIndex[server] = reply.ConflictIndex
		rf.matchIndex[server] = rf.nextIndex[server] - 1
	} else {
		// conflict across different terms
		// see if leader has logs from that term
		index := rf.getLastLogIndex()
		for ; index >= 0; index-- {
			if rf.log[index].Term == reply.ConflictTerm {
				break
			}
		}
		if index > 0 {
			// if it does, correct logs from there
			rf.nextIndex[server] = index
		} else {
			// if it doesn't, just go with conflict index
			rf.nextIndex[server] = reply.ConflictIndex
		}
		rf.matchIndex[server] = rf.nextIndex[server] - 1
	}

	////////////////////////////////////////////////////////////
	// COMMIT LOGS

	for n := rf.getLastLogIndex(); n >= rf.commitIndex; n-- {
		logCount := 1
		if rf.log[n].Term == rf.currentTerm {
			for i, peer := range rf.matchIndex {
				if i == rf.me {
					continue
				}
				if peer >= n {
					logCount += 1
				}
			}
		}

		if logCount > len(rf.peers)/2 {
			Debug(dCommit, "S%d: Commiting from %d-%d", rf.me, rf.commitIndex, n)
			go rf.commitLogs(n)

			// here is the assumption that, if a log is updated
			// to at least half the machines, EVERYTHING BEFORE
			// IS ALSO CORRECTLY UPDATED, hence no need to check
			break
		}
	}
}

func (rf *Raft) sendHeartbeat() {
	rf.mu.Lock()
	defer rf.mu.Unlock()

	// race condition check
	if rf.state != Leader {
		return
	}

	for peer := range rf.peers {
		if peer == rf.me {
			continue
		}

		prevLogIndex := rf.nextIndex[peer] - 1
		prevLogTerm := rf.log[prevLogIndex].Term

		// making a deep copy here
		slicedEntries := rf.log[rf.nextIndex[peer]:]
		entries := make([]LogEntry, len(slicedEntries))
		copy(entries, slicedEntries)

		args := &AppendEntriesArgs{
			Term:         rf.currentTerm,
			LeaderId:     rf.me,
			PrevLogIndex: prevLogIndex,
			PrevLogTerm:  prevLogTerm,
			LeaderCommit: rf.commitIndex,
			Entries:      entries,
		}
		go rf.sendAppendEntries(peer, args, &AppendEntriesReply{})
	}
}

func (rf *Raft) commitLogs(newCommitIndex int) {
	rf.mu.Lock()
	defer rf.mu.Unlock()

	hasCurrentTermLog := false
	for i := rf.commitIndex + 1; i <= newCommitIndex; i++ {
		if rf.log[i].Term == rf.currentTerm {
			hasCurrentTermLog = true
			break
		}
	}

	if hasCurrentTermLog {
		rf.commitIndex = newCommitIndex

		for i := rf.lastApplied + 1; i <= rf.commitIndex; i++ {
			rf.applyCh <- ApplyMsg{
				CommandValid: true,
				Command:      rf.log[i].Command,
				CommandIndex: i,
			}
			rf.lastApplied = i
		}
	}
}

/////////////////////////////////////////////////////////////////////
/////////////////////                            ////////////////////
/////////////////////////////////////////////////////////////////////
///////////                   ///////////                   /////////
/////////////////////////////////////////////////////////////////////

func (rf *Raft) addToChan(c chan bool, value bool) {
	select {
	case c <- value:
	default:
	}
}

func (rf *Raft) becomeFollower(term int) {
	state := rf.state
	rf.state = Follower
	rf.currentTerm = term
	rf.votedFor = -1
	rf.votes = 0
	rf.persist()
	if state != Follower {
		rf.addToChan(rf.followerCh, true)
	}
}

func (rf *Raft) becomeLeader() {
	rf.mu.Lock()
	defer rf.mu.Unlock()

	// race condition check
	if rf.state != Candidate {
		return
	}

	rf.state = Leader
	rf.nextIndex = make([]int, len(rf.peers))
	rf.matchIndex = make([]int, len(rf.peers))
	rf.persist()

	// reset all channels
	rf.leaderCh = make(chan bool)
	rf.followerCh = make(chan bool)
	rf.voteCh = make(chan bool)
	rf.heartbeatCh = make(chan bool)

	nextIndex := len(rf.log)
	for i := range rf.peers {
		rf.nextIndex[i] = nextIndex
	}
}

func (rf *Raft) becomeCandidate(s RaftState) {
	rf.mu.Lock()
	defer rf.mu.Unlock()

	// race condition check
	if rf.state != s {
		return
	}

	rf.state = Candidate
	rf.currentTerm += 1
	rf.votedFor = rf.me
	rf.votes = 1
	rf.persist()

	// reset all channels
	rf.leaderCh = make(chan bool)
	rf.followerCh = make(chan bool)
	rf.voteCh = make(chan bool)
	rf.heartbeatCh = make(chan bool)

}

func (rf *Raft) getElectionTime(electionTime int) time.Duration {
	nextElection := electionTime + (rand.Int() % 100)
	return time.Duration(nextElection) * time.Millisecond
}

func (rf *Raft) getLastLogIndex() int {
	return len(rf.log) - 1
}

func (rf *Raft) getLastLogTerm() int {
	return rf.log[rf.getLastLogIndex()].Term
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
	// Your code here (2B).
	rf.mu.Lock()
	defer rf.mu.Unlock()

	if rf.state != Leader {
		return -1, rf.currentTerm, false
	}

	term := rf.currentTerm
	Debug(dLog, "S%d: LEADER appended log index %d.", rf.me, len(rf.log))
	rf.log = append(rf.log, LogEntry{term, command})
	rf.persist()

	return len(rf.log) - 1, term, true
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
	for !rf.killed() {

		// Your code here (2A)
		// Check if a leader election should be started.
		rf.mu.Lock()
		state := rf.state
		rf.mu.Unlock()

		switch state {
		case Leader:
			select {
			case <-rf.followerCh:
				Debug(dInfo, "S%d: Stepping down from LEADER to FOLLOWER.", rf.me)
			case <-time.After(time.Duration(rf.heartbeatInterval) * time.Millisecond):
				rf.sendHeartbeat()
				Debug(dHeart, "S%d: (%d) Sending heartbeat.", rf.me, rf.currentTerm)
			}
		case Candidate:
			select {
			case <-rf.followerCh:
				Debug(dInfo, "S%d: Stepping down from CANDIDATE to FOLLOWER.", rf.me)
			case <-rf.leaderCh:
				Debug(dLeader, "S%d: Enough votes received, NEW LEADER.", rf.me)
				rf.becomeLeader()
				Debug(dHeart, "S%d: Sending FIRST heartbeat, (TERM=%d).", rf.me, rf.currentTerm)
				rf.sendHeartbeat()
			case <-time.After(rf.getElectionTime(rf.electionInterval)):
				Debug(dTimer, "S%d: Election timeout, raising election as CANDIDATE AGAIN", rf.me)
				rf.becomeCandidate(Candidate)
				rf.raiseElection()
			}
		case Follower:
			select {
			case <-rf.voteCh:
			case <-rf.heartbeatCh:
			case <-time.After(rf.getElectionTime(rf.electionInterval)):
				Debug(dTimer, "S%d: Election timeout, FOLLOWER raising election now CANDIDATE ", rf.me)
				rf.becomeCandidate(Follower)
				rf.raiseElection()
			}
		}
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
	rf.votes = 0

	rf.log = append(rf.log, LogEntry{Term: 0})

	rf.commitIndex = 0
	rf.lastApplied = 0

	rf.heartbeatInterval = 100
	rf.electionInterval = 250

	rf.applyCh = applyCh
	rf.leaderCh = make(chan bool)
	rf.followerCh = make(chan bool)
	rf.voteCh = make(chan bool)
	rf.heartbeatCh = make(chan bool)

	Debug(dClient, "Created client %d", rf.me)

	// initialize from state persisted before a crash
	rf.readPersist(persister.ReadRaftState())

	// start ticker goroutine to start elections
	go rf.ticker()

	return rf
}

/////////////////////////////////////////////////////

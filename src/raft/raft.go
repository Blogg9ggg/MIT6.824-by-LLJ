package raft

import (
	"bytes"
	"sync"
	"sync/atomic"

	"6.824/labgob"
	"6.824/labrpc"

	"time"
	"math/rand"
	"fmt"
)

// constant 
const (
	leader 					int = 0
	candidate 				int = 1
	follower 				int = 2

	election_timeout		int32 = 150	// 150 - 300
	append_entries_timeout	int32 = 100
)

// for apply
type ApplyMsg struct {
	CommandValid bool
	Command      interface{}
	CommandIndex int

	// for snap shot
	SnapshotValid bool
	Snapshot      []byte
	SnapshotTerm  int
	SnapshotIndex int
}

// for store log
type LogEntry struct {
	Term 	int
	Data	interface{}
}

type Raft struct {
	mu        sync.Mutex          // Lock to protect shared access to this peer's state
	peers     []*labrpc.ClientEnd // RPC end points of all peers
	persister *Persister          // Object to hold this peer's persisted state
	me        int                 // this peer's index into peers[]
	dead      int32               // set by Kill()

	currentTerm	int
	log			[]LogEntry
	state 		int 	// 0, leader; 1, candidate; 2, follower
	
	timer				*time.Timer
	applyChan			chan ApplyMsg
	updateAppliedChan 	chan int

	// for leader election
	votedFor	int

	// for apply
	commitIndex	int
	lastApplied	int
	
	// for leader
	nextIndex	[]int
	matchIndex	[]int

	// for candidate
	yes_vote	int

	// for snap shot
	snapShotIndex int
	snapShotTerm int
}

// util
func min(a int, b int) int {
	if a < b {
		return a
	}
	return b
}
func max(a int, b int) int {
	if a > b {
		return a
	}
	return b
}
// clear the chan of timer
func (rf *Raft) clearTimerC() {
	if !rf.timer.Stop() && len(rf.timer.C)>0 {
		<- rf.timer.C
	}
}
// a goroutine for updating applied chan
func (rf *Raft) updateAppliedObj() {
	var tmpInd int
	for {
		tmpInd = <- rf.updateAppliedChan

		rf.mu.Lock()			
		if tmpInd != rf.lastApplied + 1 ||
			rf.indInLog(tmpInd) < 0 {
			// DPrintf("sync error")
			rf.mu.Unlock()
			continue
		} else {
			rf.lastApplied++
		}

		tmpApplyMsg := ApplyMsg{
			CommandValid: 	true,
			Command:		rf.log[rf.indInLog(tmpInd)].Data,
			CommandIndex:	tmpInd,
		}
		rf.mu.Unlock()
		
		rf.applyChan <- tmpApplyMsg
	}
}
func (rf *Raft) updateCommitIndex() {
	var tmp int
	var nextCommit int = 0
	for {
		rf.mu.Lock()
		if nextCommit <= rf.commitIndex {
			nextCommit = rf.commitIndex + 1
		}
		
		for nextCommit < rf.lenOfLog() &&
		rf.log[rf.indInLog(nextCommit)].Term < rf.currentTerm {
			nextCommit++
		}

		if rf.state == leader && nextCommit < rf.lenOfLog() {
			cnt := 1
			for i := 0; i < len(rf.peers); i++ {
				if i != rf.me && rf.matchIndex[i] >= nextCommit {
					cnt++
					if cnt > len(rf.peers)/2 {
						rf.commitIndex = nextCommit
						break
					}
				}
			}
		}

		if rf.commitIndex <= rf.lastApplied {
			rf.mu.Unlock()
			time.Sleep(time.Duration(100) * time.Millisecond)
			continue
		}
		tmp = rf.lastApplied + 1
		rf.mu.Unlock()
		rf.updateAppliedChan <- tmp
	}
}

// debug
const debug bool = true
func (rf *Raft) allInfo(pos string, to int, res bool, entries_len int) {
	if debug {
		DPrintf("=============== @%s ===============\nID: %d\nTerm: %d\nState: %d\nCommitInd: %d\nTo/from: %d\nResult: %v\nlastApplied: %d\nentries_len: %d\nsnapShotInd: %d\nsnapShotTerm: %d\nlogLen: %d\n", 
		pos, rf.me, rf.currentTerm, rf.state, rf.commitIndex, to, res,rf.lastApplied, entries_len, rf.snapShotIndex, rf.snapShotTerm, rf.lenOfLog())
		
		fmt.Printf("Log: ")
		for i := 0; i < len(rf.log); i++ {
			fmt.Printf("(%d,%d) ", rf.log[i].Term, rf.log[i].Data)
		}
		if rf.state == leader {
			fmt.Printf("\nnextIndex: ")
			for _, val := range rf.nextIndex {
				fmt.Printf("%d ", val)
			}
			fmt.Printf("\nmatchIndex: ")
			for _, val := range rf.matchIndex {
				fmt.Printf("%d ", val)
			}
		}
		
		fmt.Println("\n==============================")
	}
}


// change state
// if currentTerm change, votedFor must change (before the function)
func (rf *Raft) turn2Follower(vot int, cur int) {
	// W
	
	rf.mu.Lock()
	if cur == rf.currentTerm {
		rf.state = follower
		if rf.votedFor == -1 {
			rf.votedFor = vot
		}
	} else if cur > rf.currentTerm {
		rf.state = follower
		rf.votedFor = vot
		rf.currentTerm = cur
	}
	rf.persist()
	rf.mu.Unlock()

	rf.clearTimerC()
	rf.timer.Reset(time.Duration(election_timeout+rand.Int31() % 151) * time.Millisecond)
}

func (rf *Raft) turn2Candidate() {
	
	rf.mu.Lock()
	rf.state = candidate
	// Increment currentTerm
	rf.currentTerm ++
	// Vote for self
	rf.yes_vote = 1
	rf.votedFor = rf.me
	rf.persist()
	rf.mu.Unlock()

	// Reset election timer
	rf.clearTimerC()
	rf.timer.Reset(time.Duration(election_timeout+rand.Int31() % 151) * time.Millisecond)

	
	for i := 0; i < len(rf.peers); i++ {
		if i != rf.me {
			rf.mu.Lock()
			args := RequestVoteArgs{
				Term: 			rf.currentTerm,
				CandidateId: 	rf.me,
				LastLogIndex:	rf.snapShotIndex+len(rf.log)-1,
				LastLogTerm:	rf.log[len(rf.log)-1].Term,
			}
			reply := RequestVoteReply{}

			rf.mu.Unlock()

			go rf.sendRequestVote(i, &args, &reply)
		}
	}
}

func (rf *Raft) turn2Leader() {
	
	rf.mu.Lock()
	if rf.state != candidate {
		rf.mu.Unlock()
		return
	}

	rf.state = leader
	for i := 0; i < len(rf.peers); i++ {
		rf.nextIndex[i] = rf.lenOfLog()
		rf.matchIndex[i] = 0
	}
	rf.mu.Unlock()

	rf.clearTimerC()
	rf.timer.Reset(time.Duration(append_entries_timeout) * time.Millisecond)

	for i := 0; i < len(rf.peers); i++ {
		if i != rf.me {
			go func(ind int) {
				rf.mu.Lock()
				args := AppendEntriesArgs{
					Term: 			rf.currentTerm,
					LeaderId:		rf.me,
					PrevLogIndex:	rf.nextIndex[ind] - 1,
					PrevLogTerm:	rf.log[rf.indInLog(rf.nextIndex[ind]) - 1].Term,
					Entries:		[]LogEntry{},
					LeaderCommit:	min(rf.commitIndex, rf.matchIndex[ind]),
				}
				reply := AppendEntriesReply{}
				rf.mu.Unlock()
	
				rf.sendAppendEntries(ind, &args, &reply)
			}(i)
		}
	}
}

func (rf *Raft) indInLog(ind int) int {
	return ind - rf.snapShotIndex
}
func (rf *Raft) lenOfLog() int {
	return rf.snapShotIndex + len(rf.log)
}

// return currentTerm and whether this server
// believes it is the leader.
func (rf *Raft) GetState() (int, bool) {
	var term int
	var isleader bool

	// Your code here (2A).
	// R
	rf.mu.Lock()
	term = rf.currentTerm	
	isleader = (rf.state == leader)
	rf.mu.Unlock()
	return term, isleader
}

func (rf *Raft) encodeState() []byte {

	buf := new(bytes.Buffer)
	enc := labgob.NewEncoder(buf)
	enc.Encode(rf.currentTerm)
	enc.Encode(rf.votedFor)
	enc.Encode(rf.log)
	return buf.Bytes()
}
//
// save Raft's persistent state to stable storage,
// where it can later be retrieved after a crash and restart.
// see paper's Figure 2 for a description of what should be persistent.
//
func (rf *Raft) persist() {
	// Your code here (2C).
	// Example:
	// w := new(bytes.Buffer)
	// e := labgob.NewEncoder(w)
	// e.Encode(rf.xxx)
	// e.Encode(rf.yyy)
	// data := w.Bytes()
	// rf.persister.SaveRaftState(data)

	rf.persister.SaveRaftState(rf.encodeState())
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

	buf := bytes.NewBuffer(data)
	dec := labgob.NewDecoder(buf)
	var cTerm int
	var vFor int
	var lg []LogEntry

	if dec.Decode(&cTerm) != nil ||
	dec.Decode(&vFor) != nil ||
	dec.Decode(&lg) != nil {
	// SOMETHING ERROR
	} else {
		rf.currentTerm, rf.votedFor, rf.log = cTerm, vFor, lg
	}
}

type InstallSnapshotArgs struct {
	Term				int
	LeaderId 			int
	LastIncludedIndex	int
	LastIncludedTerm	int
	Data				[]byte
}
type InstallSnapshotReply struct {
	Term				int
}

func (rf *Raft) InstallSnapshot(args *InstallSnapshotArgs, reply *InstallSnapshotReply) {
	// pos string, to int, res bool, entries_len int
	rf.allInfo("InstallSnapshot", args.LeaderId, true, args.LastIncludedIndex)

	rf.mu.Lock()
	reply.Term = rf.currentTerm
	// illegal leader
	if args.Term < rf.currentTerm {
		rf.mu.Unlock()
		return
	}
	rf.mu.Unlock()

	if args.Term > rf.currentTerm {
		rf.turn2Follower(-1, args.Term)
	} else {// args.Term == rf.currentTerm
		switch rf.state {
		case leader:
			// assert(args.Term != rf.currentTerm)
			// because one Term just have one leader
			// SOMETHING ERROR
		case candidate:
			tmpVotedFor := rf.me
			rf.turn2Follower(tmpVotedFor, args.Term)
		case follower:
			rf.clearTimerC()
			rf.timer.Reset(time.Duration(election_timeout+rand.Int31() % 151) * time.Millisecond)
		}
	}

	rf.mu.Lock()
	rf.persist()

	// outdated snapshot
	if args.LastIncludedIndex <= rf.snapShotIndex {
		rf.mu.Unlock()
		return
	}
	rf.mu.Unlock()

	rf.applyChan <- ApplyMsg{
		SnapshotValid: 	true,
		Snapshot: 		args.Data,
		SnapshotTerm: 	args.LastIncludedTerm,
		SnapshotIndex: 	args.LastIncludedIndex,
	}

	rf.mu.Lock()
	if rf.lenOfLog() - 1 >= args.LastIncludedIndex {
		rf.log = rf.log[rf.indInLog(args.LastIncludedIndex):]
	} else {
		rf.log = rf.log[:1]
	}
	rf.log[0] = LogEntry{}
	// rf.log[0].Data = nil
	// rf.log[0].Term = args.LastIncludedTerm
	rf.shrinkLogsArray()

	rf.snapShotIndex = args.LastIncludedIndex
	rf.snapShotTerm = args.LastIncludedTerm

	if args.LastIncludedIndex > rf.commitIndex {
		rf.commitIndex = args.LastIncludedIndex
	}
	if args.LastIncludedIndex > rf.lastApplied {
		// DPrintf("(SERVER #%d)lastApplied = %d\n", rf.me, rf.lastApplied)
		rf.lastApplied = args.LastIncludedIndex
		// DPrintf("(SERVER #%d)lastApplied = %d\n", rf.me, rf.lastApplied)
	}
	
	rf.mu.Unlock()

	rf.allInfo("InstallSnapshot(after)", args.LeaderId, true, args.LastIncludedIndex)
}

func (rf *Raft) sendInstallSnapshot(server int, args *InstallSnapshotArgs, reply *InstallSnapshotReply) {
	ok := rf.peers[server].Call("Raft.InstallSnapshot", args, reply)
	
	// DPrintf("sendInstallSnapshot(server #%d) -- lastApplied: %d, snapShotIndex: %d, snapShotTerm: %d\n", rf.me, rf.lastApplied, rf.snapShotIndex, rf.snapShotTerm)
	
	if !ok {
		reply = &InstallSnapshotReply{}
		go rf.sendInstallSnapshot(server, args, reply)
		return
	}
	// pos string, to int, res bool, entries_len int
	rf.allInfo("sendInstallSnapshot", server, true, args.LastIncludedIndex)

	rf.mu.Lock()
	if reply.Term > rf.currentTerm {
		rf.mu.Unlock()
		rf.turn2Follower(-1, reply.Term)
		return
	}
	rf.nextIndex[server] = max(rf.nextIndex[server], args.LastIncludedIndex + 1)
	rf.mu.Unlock()
}


//
// A service wants to switch to snapshot.  Only do so if Raft hasn't
// have more recent info since it communicate the snapshot on applyCh.
//
func (rf *Raft) CondInstallSnapshot(lastIncludedTerm int, lastIncludedIndex int, snapshot []byte) bool {

	// Your code here (2D).

	return true
}

func (rf *Raft) shrinkLogsArray() {
	const lenMultiple = 2
	if len(rf.log) == 0 {
		rf.log = nil
	} else if len(rf.log)*lenMultiple < cap(rf.log) {
		newLogs := make([]LogEntry, len(rf.log))
		copy(newLogs, rf.log)
		rf.log = newLogs
	}
}

// the service says it has created a snapshot that has
// all info up to and including index. this means the
// service no longer needs the log through (and including)
// that index. Raft should now trim its log as much as possible.
func (rf *Raft) Snapshot(index int, snapshot []byte) {

	// Your code here (2D).
	if rf.killed() {
		return
	}

	rf.mu.Lock()
	defer rf.mu.Unlock()

	if index <= rf.snapShotIndex {
		DPrintf("#SERVER %v: rejects replacing log with snapshotIndex %v as current snapshotIndex %v is larger in term %v", rf.me, index, rf.snapShotIndex, rf.currentTerm)
		return
	}
	if index > rf.commitIndex {
		DPrintf("#SERVER %v: log #%v hasn't committed", rf.me, index)
		return
	}
	if index >= rf.lenOfLog() {
		DPrintf("#SERVER %v: snapshot out of range(%d/%d)", rf.me, index, rf.lenOfLog())
	}

	tmpLog := make([]LogEntry, 0)
	tmpLog = append(tmpLog, LogEntry{})
	for i := rf.indInLog(index) + 1; i < len(rf.log); i++ {
		tmpLog = append(tmpLog, rf.log[i])
	}
	
	rf.snapShotTerm = rf.log[rf.indInLog(index)].Term
	rf.snapShotIndex = index

	rf.log = tmpLog
	if index > rf.commitIndex {
		rf.commitIndex = index
	}
	if index > rf.lastApplied {
		rf.lastApplied = index
	}

	// rf.log = rf.log[rf.indInLog(index):]
	// rf.log[0].Data = nil
	// rf.shrinkLogsArray()
	// rf.snapShotIndex = index

	rf.persister.SaveStateAndSnapshot(rf.encodeState(), snapshot)
	// DPrintf("Snapshot(%d)\n", index)
	// rf.allInfo("Snapshot(after)", -1, true, -1)
}

//
// example RequestVote RPC arguments structure.
// field names must start with capital letters!
//
type RequestVoteArgs struct {
	// Your data here (2A, 2B).
	Term			int
	CandidateId		int
	LastLogIndex	int
	LastLogTerm		int
}
//
// example RequestVote RPC reply structure.
// field names must start with capital letters!
//
type RequestVoteReply struct {
	// Your data here (2A).
	Term			int
	VoteGranted		bool
}

// check if sender's log is up-to-date ?
func (rf *Raft) isUpToDate(lastLogIndex int, lastLogTerm int) (bool) {
	rf.mu.Lock()
	defer rf.mu.Unlock()

	rfLastTerm := rf.log[len(rf.log)-1].Term
	if lastLogTerm > rfLastTerm {
		return true
	}

	if lastLogTerm == rfLastTerm && 
	lastLogIndex >= rf.lenOfLog() - 1 {
		return true
	} 

	return false
}

func (rf *Raft) RequestVote(args *RequestVoteArgs, reply *RequestVoteReply) {
	// Your code here (2A, 2B).

	if rf.killed() {
		reply.Term = -1
		reply.VoteGranted = false
		return
	}
	
	// Reply false if term < currentTerm (&5.1)
	// R
	rf.mu.Lock()
	if args.Term < rf.currentTerm {
		reply.Term = rf.currentTerm
		reply.VoteGranted = false
		rf.mu.Unlock()
		return
	}
	rf.mu.Unlock()

	// If votedFor is null or candidatedId, and candidate's log is at
	// least as up-to-date as receiver's log, grant vote (&5.2, &5.4)
	is_up_to_date := rf.isUpToDate(args.LastLogIndex, args.LastLogTerm)

	// args.Term >= rf.currentTerm
	rf.mu.Lock()
	switch rf.state {
	case leader:
		if args.Term > rf.currentTerm {
			rf.mu.Unlock()
			reply.Term = args.Term
			if is_up_to_date {
				reply.VoteGranted = true
				rf.turn2Follower(args.CandidateId, args.Term)
			} else {
				reply.VoteGranted = false
				rf.turn2Follower(-1, args.Term)
			}
		} else {
			reply.Term = rf.currentTerm
			rf.mu.Unlock()
			reply.VoteGranted = false
		}
	case candidate:
		if args.Term > rf.currentTerm {
			rf.mu.Unlock()
			reply.Term = args.Term
			if is_up_to_date {
				reply.VoteGranted = true
				rf.turn2Follower(args.CandidateId, args.Term)
			} else {
				reply.VoteGranted = false
				rf.turn2Follower(-1, args.Term)
			}
		} else {
			reply.Term = rf.currentTerm
			rf.mu.Unlock()
			reply.VoteGranted = false
		}
	case follower:
		if args.Term > rf.currentTerm {
			rf.mu.Unlock()
			reply.Term = args.Term
			if is_up_to_date {
				reply.VoteGranted = true
				
				rf.turn2Follower(args.CandidateId, args.Term)
			} else {
				reply.VoteGranted = false
				
				rf.turn2Follower(-1, args.Term)
			}
		} else if args.Term == rf.currentTerm {
			reply.Term = args.Term
			if rf.votedFor < 0 {
				rf.votedFor = args.CandidateId
				rf.persist()
				rf.mu.Unlock()
				reply.VoteGranted = true
			} else {
				rf.mu.Unlock()
				reply.VoteGranted = false
			}

			rf.clearTimerC()
			rf.timer.Reset(time.Duration(election_timeout+rand.Int31() % 151) * time.Millisecond)
		} else {
			reply.Term = rf.currentTerm
			rf.mu.Unlock()
			// SOMETHING ERROR
		}
	}
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
func (rf *Raft) sendRequestVote(server int, args *RequestVoteArgs, reply *RequestVoteReply) {
	if rf.killed() {
		reply.Term = -1
		reply.VoteGranted = false
		return
	}
	ok := rf.peers[server].Call("Raft.RequestVote", args, reply)
	
	if !ok {
		return
	}

	rf.mu.Lock()
	if reply.Term > rf.currentTerm {// reply.Success must be false
		rf.mu.Unlock()
		rf.turn2Follower(-1, reply.Term)
		return
	}
	rf.mu.Unlock()

	if reply.VoteGranted {
		rf.mu.Lock()
		rf.yes_vote++
		if rf.yes_vote > len(rf.peers)/2 {
			rf.mu.Unlock()
			rf.turn2Leader()
			return
		}
		rf.mu.Unlock()
	} 
}

type AppendEntriesArgs struct {
	Term			int
	LeaderId		int

	PrevLogIndex 	int
	PrevLogTerm		int
	Entries			[]LogEntry	

	LeaderCommit	int
}
type AppendEntriesReply struct {
	Term	int
	Success	bool
}

func (rf *Raft) AppendEntries(args *AppendEntriesArgs, reply *AppendEntriesReply) {
	reply.Success = false
	if rf.killed() {
		reply.Term = -1
		return
	}
	// DPrintf("AppendEntries(server #%d) -- lastApplied: %d, snapShotIndex: %d, snapShotTerm: %d\n", rf.me, rf.lastApplied, rf.snapShotIndex, rf.snapShotTerm)
	// rf.allInfo("AE", args.LeaderId, false, len(args.Entries))

	rf.mu.Lock()
	if args.Term < rf.currentTerm {
		reply.Term = rf.currentTerm
		rf.mu.Unlock()
		return
	}
	rf.mu.Unlock()

	// args.Term >= rf.currentTerm
	switch rf.state {
	case leader:
		// assert(args.Term != rf.currentTerm)
		// because one Term just have one leader
		if args.Term > rf.currentTerm {
			rf.turn2Follower(-1, args.Term)
		} // else SOMETHING ERROR
	case candidate:
		tmpVotedFor := rf.me
		if args.Term > rf.currentTerm {
			tmpVotedFor = -1
		}
		rf.turn2Follower(tmpVotedFor, args.Term)
	case follower:
		rf.mu.Lock()
		if args.Term > rf.currentTerm {
			rf.currentTerm = args.Term
			rf.votedFor = -1
			rf.persist()
		}
		rf.mu.Unlock()

		rf.clearTimerC()
		rf.timer.Reset(time.Duration(election_timeout+rand.Int31() % 151) * time.Millisecond)
	}
	
	// try to append entries
	reply.Term = args.Term
	if len(args.Entries) == 0 {
		
		rf.mu.Lock()
		if args.LeaderCommit > rf.commitIndex {
			rf.commitIndex = min(args.LeaderCommit, rf.lenOfLog() - 1)
		}
		if args.PrevLogIndex < rf.lenOfLog() && rf.indInLog(args.PrevLogIndex) >= 0 &&
		rf.log[rf.indInLog(args.PrevLogIndex)].Term == args.PrevLogTerm {
			reply.Success = true
		}
		rf.mu.Unlock()

		return
	}
	rf.mu.Lock()
	if args.PrevLogIndex < rf.lenOfLog() && 
	rf.indInLog(args.PrevLogIndex) >= 0 &&
	rf.log[rf.indInLog(args.PrevLogIndex)].Term == args.PrevLogTerm {
		for i := 0; i < len(args.Entries); i++ {
			if args.PrevLogIndex + 1 + i < rf.lenOfLog() {
				rf.log[rf.indInLog(args.PrevLogIndex + 1 + i)] = args.Entries[i]
			} else {
				rf.log = append(rf.log, args.Entries[i])
			}
		}
		rf.persist()
		reply.Success = true
	}
	
	if args.LeaderCommit > rf.commitIndex {
		rf.commitIndex = min(args.LeaderCommit, rf.lenOfLog() - 1)
	}
	rf.mu.Unlock()

	
}

func (rf *Raft) sendAppendEntries(server int, args *AppendEntriesArgs, reply *AppendEntriesReply) {
	if rf.killed() {
		reply.Term = -1
		reply.Success = false
		return
	}
	
	ok := rf.peers[server].Call("Raft.AppendEntries", args, reply)
	// DPrintf("ok: %v\n", ok)
	// rf.allInfo("sendAE", server, reply.Success, len(args.Entries))


	if !ok {
		// communication error, just retry
		reply = &AppendEntriesReply{}
		go rf.sendAppendEntries(server, args, reply)
		
		return
	}
	rf.mu.Lock()
	if reply.Term > rf.currentTerm {// reply.Success must be false
		rf.mu.Unlock()
		rf.turn2Follower(-1, reply.Term)
		return
	}
	rf.mu.Unlock()

	// if len(args.Entries) == 0 {// heart beat
	// 	return
	// }

	rf.mu.Lock()
	if rf.state != leader {
		rf.mu.Unlock()
		return
	}
	rf.mu.Unlock()

	// rf.currentTerm >= reply.Term && len(args.Entries) != 0
	if reply.Success {
		// R & W
		rf.mu.Lock()
		rf.matchIndex[server] = max(args.PrevLogIndex+len(args.Entries), rf.matchIndex[server])
		rf.nextIndex[server] = max(rf.matchIndex[server]+1, rf.nextIndex[server])
		rf.mu.Unlock()
	} else {
		// R & W
		rf.mu.Lock()
		if args.PrevLogIndex <= rf.snapShotIndex {
			sargs := InstallSnapshotArgs {
				Term: 				rf.currentTerm,
				LeaderId: 			rf.me,
				LastIncludedIndex: 	rf.snapShotIndex,
				LastIncludedTerm:	rf.snapShotTerm,
				Data:				rf.persister.ReadSnapshot(),
			}
			sreply := InstallSnapshotReply{}
			rf.mu.Unlock()

			go rf.sendInstallSnapshot(server, &sargs, &sreply)
			return
		}
		
		// rf.nextIndex[server] > rf.snapShotIndex
		rf.nextIndex[server] = min(rf.nextIndex[server], args.PrevLogIndex)
		if rf.nextIndex[server] <= rf.matchIndex[server] {
			rf.nextIndex[server] = rf.matchIndex[server] + 1
		}

		if rf.log[len(rf.log) - 1].Term != rf.currentTerm {
			rf.mu.Unlock()
			return
		}
		
		args = &AppendEntriesArgs{
			Term: 			rf.currentTerm,
			LeaderId:		rf.me,
			PrevLogIndex:	0,
			PrevLogTerm:	0,
			Entries:		[]LogEntry{},
			LeaderCommit:	min(rf.commitIndex, rf.matchIndex[server]),
		}
		reply = &AppendEntriesReply{}

		// rf.log[len(rf.log) - 1].Term == rf.currentTerm 
		args.PrevLogIndex = rf.nextIndex[server] - 1
		args.PrevLogTerm = rf.log[rf.indInLog(args.PrevLogIndex)].Term
		for i := rf.nextIndex[server]; i < rf.lenOfLog(); i++ {
			args.Entries = append(args.Entries, rf.log[rf.indInLog(i)])
		}
		
		rf.mu.Unlock()
		
		go rf.sendAppendEntries(server, args, reply)
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
	// Your code here (2B).

	if rf.killed() {
		return -1, -1, false
	}
	
	rf.mu.Lock()
	index := rf.lenOfLog()
	term := rf.currentTerm
	isLeader := (rf.state == 0)

	if isLeader {
		newLogEntry := LogEntry{
			Term:	rf.currentTerm,
			Data:	command,
		}
		rf.log = append(rf.log, newLogEntry)
		rf.persist()
	}
	rf.mu.Unlock()
	
	
	return index, term, isLeader
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
// heartsbeats recently.
func (rf *Raft) ticker() {
	rand.Seed(makeSeed())

	for rf.killed() == false {
		// Your code here to check if a leader election should
		// be started and to randomize sleeping time using
		// time.Sleep().
		<- rf.timer.C

		rf.mu.Lock()
		switch rf.state {
		case leader:
			rf.mu.Unlock()
			rf.timer.Reset(time.Duration(append_entries_timeout) * time.Millisecond)
			
			for i := 0; i < len(rf.peers); i++ {
				if i != rf.me {
					rf.mu.Lock()
					if rf.nextIndex[i] - 1 < rf.snapShotIndex {
						args := InstallSnapshotArgs {
							Term: 				rf.currentTerm,
							LeaderId: 			rf.me,
							LastIncludedIndex: 	rf.snapShotIndex,
							LastIncludedTerm:	rf.snapShotTerm,
							Data:				rf.persister.ReadSnapshot(),
						}
						reply := InstallSnapshotReply{}
						rf.mu.Unlock()
						
						go rf.sendInstallSnapshot(i, &args, &reply)
					} else {
						args := AppendEntriesArgs{
							Term: 			rf.currentTerm,
							LeaderId:		rf.me,
							PrevLogIndex:	rf.nextIndex[i] - 1,
							PrevLogTerm:	rf.log[rf.indInLog(rf.nextIndex[i])-1].Term,	
							Entries:		[]LogEntry{},
							LeaderCommit:	min(rf.commitIndex, rf.matchIndex[i]),
						}
						reply := AppendEntriesReply{}
						if rf.nextIndex[i] < rf.lenOfLog() && 
						rf.log[len(rf.log)-1].Term == rf.currentTerm {
							args.PrevLogIndex = rf.nextIndex[i] - 1
							args.PrevLogTerm = rf.log[rf.indInLog(args.PrevLogIndex)].Term
							for j := rf.nextIndex[i]; j < rf.lenOfLog(); j++ {
								args.Entries = append(args.Entries, rf.log[rf.indInLog(j)])
							}
						}
						rf.mu.Unlock()
						go func(ind int, args *AppendEntriesArgs, reply *AppendEntriesReply) {
							rf.sendAppendEntries(ind, args, reply)
						}(i, &args, &reply)
					}
				}
			}
			// wg.Wait()
		case candidate:
			rf.mu.Unlock()
			rf.turn2Candidate()
		case follower:
			rf.mu.Unlock()
			rf.turn2Candidate()
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

	rf.peers = peers
	rf.persister = persister
	rf.me = me

	// Your initialization code here (2A, 2B, 2C).
	rf.currentTerm = 0
	rf.votedFor = -1
	rf.log = append([]LogEntry{}, LogEntry{})
	
	rf.commitIndex = 0
	rf.lastApplied = 0
	rf.applyChan = applyCh

	rf.state = follower
	rf.timer = time.NewTimer(time.Duration(election_timeout+rand.Int31() % 151) * time.Millisecond)

	rf.nextIndex = make([]int, len(rf.peers))
	rf.matchIndex = make([]int, len(rf.peers))

	rf.updateAppliedChan = make(chan int)

	rf.snapShotIndex = 0
	rf.snapShotTerm = 0
	
	// initialize from state persisted before a crash
	rf.readPersist(persister.ReadRaftState())

	// start ticker goroutine to start elections

	go rf.updateCommitIndex()
	go rf.updateAppliedObj()
	go rf.ticker()

	return rf
}

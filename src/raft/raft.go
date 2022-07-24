package raft

import (
	"bytes"
	"sync"
	"sync/atomic"

	"6.824/labgob"
	"6.824/labrpc"

	"time"
	"math/rand"
	// "fmt"
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
type logAPI struct {
	log				[]LogEntry
	// headIndex == rf.snapShotIndex
	headIndex	int
}
func (la *logAPI) findFirst(term int) int {
	l, r := la.headIndex, la.tailIndex()
	for l < r {
		m := (l + r) / 2
		if la.at(m).Term > term {
			r = m - 1
		} else if la.at(m).Term < term {
			l = m + 1
		} else {
			r = m
		}
	}
	if l == r && la.at(l).Term == term {
		return l
	}
	return -1
}
func (la *logAPI) findLast(term int) int {
	l, r, ret := la.headIndex, la.tailIndex(), -1
	for l < r {
		m := (l + r) / 2
		if la.at(m).Term > term {
			r = m - 1
		} else if la.at(m).Term < term {
			l = m + 1
		} else {
			ret, l = m, m + 1	
		}
	}
	return ret
}
func (la *logAPI) at(ind int) LogEntry {
	if ind < la.headIndex {
		DPrintf("ind(%d) < la.headIndex(%d)", ind, la.headIndex)
		panic("logAPI.at(error): too old log.")
	}
	return la.log[ind - la.headIndex]
}
func (la *logAPI) index(ind int) int {
	if ind < la.headIndex {
		DPrintf("ind(%d) < la.headIndex(%d)", ind, la.headIndex)
		panic("logAPI.at(error): too old log.")
	}
	return ind - la.headIndex
}
func (la *logAPI) tailIndex() int {
	return la.headIndex + len(la.log) - 1
}
func (la *logAPI) tail() LogEntry {
	return la.log[len(la.log)-1]
}
func (la *logAPI) shrinkLogsArray() {
	const lenMultiple = 2
	if len(la.log) == 0 {
		la.log = nil
	} else if len(la.log)*lenMultiple < cap(la.log) {
		newLogs := make([]LogEntry, len(la.log))
		copy(newLogs, la.log)
		la.log = newLogs
	}
}
func (la *logAPI) logCompression(ind int) {
	la.log = append([]LogEntry{}, la.log[la.index(ind):]...)
	la.log[0].Data = nil
	la.headIndex = ind
	la.shrinkLogsArray()
}
func (la *logAPI) installSnapshot(lastIncludedTerm int, lastIncludedIndex int) {
	if lastIncludedIndex <= la.tailIndex() && 
	lastIncludedTerm == la.at(lastIncludedIndex).Term {
		la.logCompression(lastIncludedIndex)
	} else {
		la.log = make([]LogEntry, 1)
		la.log[0].Term, la.headIndex = lastIncludedTerm, lastIncludedIndex
		la.shrinkLogsArray()
	}
}

type Raft struct {
	mu        sync.Mutex          // Lock to protect shared access to this peer's state
	peers     []*labrpc.ClientEnd // RPC end points of all peers
	persister *Persister          // Object to hold this peer's persisted state
	me        int                 // this peer's index into peers[]
	dead      int32               // set by Kill()

	currentTerm	int
	logapi		logAPI
	state 		int 	// 0, leader; 1, candidate; 2, follower
	
	timer				*time.Timer
	applyChan			chan ApplyMsg

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
	snapShotIndex 	int
	snapShotTerm 	int

	applyCond	*sync.Cond
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
// 这个 clear 其实没什么用，因为 timer 的很多操作都是原子性的
func (rf *Raft) clearTimerC() {
	if !rf.timer.Stop() && len(rf.timer.C)>0 {
		<- rf.timer.C
	}
}

func (rf *Raft) applier() {
	for rf.killed() == false {
		rf.mu.Lock()
		for rf.lastApplied >= rf.commitIndex {
			rf.applyCond.Wait()
		}
		rf.lastApplied++
		tmpApplyMsg := ApplyMsg{
			CommandValid: 	true,
			Command:		rf.logapi.at(rf.lastApplied).Data,
			CommandIndex:	rf.lastApplied,
		}
		rf.mu.Unlock()
		rf.applyChan <- tmpApplyMsg
	}
}

// for updating commitIndex
func (rf *Raft) updateCommitIndex(newComInd int) {
	if newComInd != -1 {
		if newComInd > rf.commitIndex {
			rf.commitIndex = newComInd
		}
	}
	if rf.state == leader {
		cnt := 1
		flag := true
		for flag {
			flag = false
			for i := 0; i < len(rf.peers); i++ {
				if i != rf.me && rf.matchIndex[i] > rf.commitIndex {
					cnt++
					if cnt > len(rf.peers)/2 {
						rf.commitIndex++
						flag = true
						break
					}
				}
			}
		}
	}
	rf.applyCond.Signal()
}



// change state (before the function)
// if currentTerm change, votedFor must change
func (rf *Raft) turn2Follower(vot int, cur int) {
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
	rf.mu.Unlock()

	// Reset election timer
	rf.clearTimerC()
	rf.timer.Reset(time.Duration(election_timeout+rand.Int31() % 151) * time.Millisecond)

	for i := 0; i < len(rf.peers); i++ {
		if i != rf.me {
			go func(ind int) {
				rf.mu.Lock()
				args := RequestVoteArgs{
					Term: 			rf.currentTerm,
					CandidateId: 	rf.me,
					LastLogIndex:	rf.logapi.tailIndex(),
					LastLogTerm:	rf.logapi.tail().Term,
				}
				reply := RequestVoteReply{}
	
				rf.mu.Unlock()
				rf.sendRequestVote(ind, &args, &reply)
			}(i)
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
	nextIndex_ := rf.logapi.tailIndex() + 1
	for i := 0; i < len(rf.peers); i++ {
		rf.nextIndex[i] = nextIndex_
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
					PrevLogTerm:	rf.logapi.at(rf.nextIndex[ind] - 1).Term,
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



// return currentTerm and whether this server
// believes it is the leader.
func (rf *Raft) GetState() (int, bool) {
	var term int
	var isleader bool
	
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
	enc.Encode(rf.logapi.log)
	enc.Encode(rf.votedFor)
	enc.Encode(rf.snapShotIndex)
	enc.Encode(rf.snapShotTerm)

	return buf.Bytes()
}

func (rf *Raft) persist() {
	rf.persister.SaveRaftState(rf.encodeState())
}

func (rf *Raft) readPersist(data []byte) {
	if data == nil || len(data) < 1 {
		return
	}

	buf := bytes.NewBuffer(data)
	dec := labgob.NewDecoder(buf)
	var currentTerm_ int
	var log_ []LogEntry
	var votedFor_ int
	var snapShotIndex_ int
	var snapShotTerm_ int
	

	if dec.Decode(&currentTerm_) != nil ||
		dec.Decode(&log_) != nil ||
		dec.Decode(&votedFor_) != nil ||
		dec.Decode(&snapShotIndex_) != nil ||
		dec.Decode(&snapShotTerm_) != nil {
	// SOMETHING ERROR
	} else {
		rf.currentTerm, rf.logapi.log, rf.votedFor, rf.snapShotIndex, rf.snapShotTerm = 
			currentTerm_, log_, votedFor_, snapShotIndex_, snapShotTerm_
		rf.logapi.headIndex = rf.snapShotIndex
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
	DPrintf("### SERVER %d: InstallSnapshot ...\n", rf.me)

	rf.mu.Lock()
	reply.Term = rf.currentTerm
	if args.Term < rf.currentTerm {
		rf.mu.Unlock()
		return
	}
	rf.mu.Unlock()
	// 保证 args.Term >= rf.currentTerm

	// 
	// 改变 state
	// 
	if args.Term > rf.currentTerm {
		rf.turn2Follower(-1, args.Term)
	} else {// args.Term == rf.currentTerm
		switch rf.state {
		case leader:
			// Election Safety 性质保证 args.Term != rf.currentTerm
			// 故流程不应该走到这里
		case candidate:
			rf.turn2Follower(rf.me, args.Term)
		case follower:
			rf.clearTimerC()
			rf.timer.Reset(time.Duration(election_timeout+rand.Int31() % 151) * time.Millisecond)
		}
	}

	rf.mu.Lock()
	// 丢弃过时的快照
	if args.LastIncludedIndex <= rf.commitIndex {
		rf.mu.Unlock()
		return
	}
	rf.mu.Unlock()

	// 将快照放到 applyChan 管道中, 等上层的 service 调用 CondInstallSnapshot 将快照执行到本机的状态机上
	rf.applyChan <- ApplyMsg{
		SnapshotValid: 	true,
		Snapshot: 		args.Data,
		SnapshotTerm: 	args.LastIncludedTerm,
		SnapshotIndex: 	args.LastIncludedIndex,
	}
}

func (rf *Raft) sendInstallSnapshot(server int, args *InstallSnapshotArgs, reply *InstallSnapshotReply) {
	ok := rf.peers[server].Call("Raft.InstallSnapshot", args, reply)
	
	if !ok {
		reply = &InstallSnapshotReply{}
		go rf.sendInstallSnapshot(server, args, reply)
		return
	}
	DPrintf("### SERVER %d: sendInstallSnapshot is ok.\n", rf.me)

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
	DPrintf("### SERVER %d: CondInstallSnapshot ...\n", rf.me)
	rf.mu.Lock()
	defer rf.mu.Unlock()

	if lastIncludedIndex <= rf.commitIndex ||
	lastIncludedIndex <= rf.snapShotIndex {
		return false
	}

	rf.logapi.installSnapshot(lastIncludedTerm, lastIncludedIndex)

	rf.commitIndex, rf.lastApplied = lastIncludedIndex, lastIncludedIndex
	rf.snapShotIndex, rf.snapShotTerm = lastIncludedIndex, lastIncludedTerm

	rf.persister.SaveStateAndSnapshot(rf.encodeState(), snapshot)

	return true
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

	if index > rf.commitIndex || 
	index <= rf.snapShotIndex {
		return
	}

	rf.snapShotTerm, rf.snapShotIndex = rf.logapi.at(index).Term, index
	rf.logapi.logCompression(rf.snapShotIndex)

	rf.persister.SaveStateAndSnapshot(rf.encodeState(), snapshot)

	DPrintf("### SERVER %d: Snapshot(%d) is ok.\n", rf.me, index)
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

	rfLastTerm := rf.logapi.tail().Term
	if lastLogTerm > rfLastTerm {
		return true
	}

	if lastLogTerm == rfLastTerm && 
	lastLogIndex >= rf.logapi.tailIndex() {
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
	DPrintf("=====================================================================\n")
	DPrintf("RequestVote(from: %d, to: %d)\n",args.CandidateId,rf.me)
	DPrintf("me.state: %d, me.term: %d, args.term: %d\n", rf.state, rf.currentTerm, args.Term)
	DPrintf("log: %v\n",rf.logapi.log)
	
	// Reply false if term < currentTerm (&5.1)
	rf.mu.Lock()
	if args.Term < rf.currentTerm {
		reply.Term = rf.currentTerm
		reply.VoteGranted = false
		rf.persist()
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
			reply.VoteGranted = false
			rf.mu.Unlock()
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
			reply.VoteGranted = false
			rf.mu.Unlock()
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
			if rf.votedFor < 0 && is_up_to_date {
				rf.votedFor = args.CandidateId
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
	rf.mu.Lock()
	rf.persist()
	rf.mu.Unlock()
}



func (rf *Raft) sendRequestVote(server int, args *RequestVoteArgs, reply *RequestVoteReply) {
	if rf.killed() {
		reply.Term = -1
		reply.VoteGranted = false
		return
	}
	rf.mu.Lock()
	if rf.state != candidate {
		rf.mu.Unlock()
		return
	}
	rf.persist()
	rf.mu.Unlock()
	ok := rf.peers[server].Call("Raft.RequestVote", args, reply)

	DPrintf("=====================================================================\n")
	DPrintf("sendRequestVote(from: %d, to: %d, result: %v)\n",rf.me,server,reply.VoteGranted)
	DPrintf("me.term: %d\n", rf.currentTerm)
	DPrintf("log: %v\n",rf.logapi.log)

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

	if args.Term == rf.currentTerm && reply.VoteGranted {
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


// 
// 日志复制
// 
type AppendEntriesArgs struct {
	Term			int
	LeaderId		int

	PrevLogIndex 	int
	PrevLogTerm		int
	Entries			[]LogEntry	

	LeaderCommit	int
}
type AppendEntriesReply struct {
	Term			int
	Success			bool
	ConflictIndex 	int
	ConflictTerm	int
}
func (rf *Raft) AppendEntries(args *AppendEntriesArgs, reply *AppendEntriesReply) {
	reply.Success = false
	rf.mu.Lock()
	reply.Term = rf.currentTerm
	rf.mu.Unlock()

	if rf.killed() {
		return
	}
	
	DPrintf("=====================================================================\n")
	DPrintf("AppendEntries(from: %d, to: %d(me))\n", args.LeaderId, rf.me)
	DPrintf("args.Term: %d, me.term: %d\n", args.Term, rf.currentTerm)
	DPrintf("snapshot index: %d, logapi.headIndex: %d\n", rf.snapShotIndex, rf.logapi.headIndex)
	DPrintf("log: %v\n", rf.logapi.log)
	
	if args.Term < reply.Term {
		return
	}

	// 
	// 改变 state
	// 
	rf.mu.Lock()
	switch rf.state {
	// 保证: args.Term >= rf.currentTerm
	case leader:
		// Election Safety 性质保证此处 args.Term > rf.currentTerm
		if args.Term > rf.currentTerm {
			rf.mu.Unlock()
			rf.turn2Follower(-1, args.Term)
		} // else SOMETHING ERROR
	case candidate:
		tmpVotedFor := rf.me
		if args.Term > rf.currentTerm {
			tmpVotedFor = -1
		}
		rf.mu.Unlock()
		rf.turn2Follower(tmpVotedFor, args.Term)
	case follower:
		if args.Term > rf.currentTerm {
			rf.currentTerm = args.Term
			rf.votedFor = -1
		}
		rf.mu.Unlock()

		rf.clearTimerC()
		rf.timer.Reset(time.Duration(election_timeout+rand.Int31() % 151) * time.Millisecond)
	}

	// 
	// 复制日志
	// 
	rf.mu.Lock()
	// 利用 ConflictTerm, ConflictIndex 这 2 个字段做 accelerated log backtracking optimization
	if rf.logapi.headIndex > args.PrevLogIndex ||
		args.PrevLogIndex > rf.logapi.tailIndex() {
		reply.Success = false
		reply.ConflictTerm = -1
		reply.ConflictIndex = rf.logapi.tailIndex() + 1
	} else if rf.logapi.at(args.PrevLogIndex).Term != args.PrevLogTerm {
		reply.Success = false
		reply.ConflictTerm = rf.logapi.at(args.PrevLogIndex).Term
		reply.ConflictIndex = rf.logapi.findFirst(reply.ConflictTerm)
	} else {
		reply.Success = true
		for i := 0; i < len(args.Entries); i++ {
			if args.PrevLogIndex + 1 + i <= rf.logapi.tailIndex() {
				tmpInd := rf.logapi.index(args.PrevLogIndex+1+i)
				rf.logapi.log[tmpInd] = args.Entries[i]
			} else {
				rf.logapi.log = append(rf.logapi.log, args.Entries[i])
			}
		}
	}

	rf.persist()
	if args.LeaderCommit > rf.commitIndex {
		rf.updateCommitIndex(min(args.LeaderCommit, rf.logapi.tailIndex()))
		rf.mu.Unlock()

		return
	}
	rf.mu.Unlock()
}

func (rf *Raft) sendAppendEntries(server int, args *AppendEntriesArgs, reply *AppendEntriesReply) {
	if rf.killed() {
		reply.Term = -1
		reply.Success = false
		return
	}
	
	rf.mu.Lock()
	if rf.state != leader {
		rf.mu.Unlock()
		return
	}
	rf.persist()
	rf.mu.Unlock()
	ok := rf.peers[server].Call("Raft.AppendEntries", args, reply)
	
	if !ok {
		// rpc 失败, 直接重试即可
		reply = &AppendEntriesReply{}
		rf.mu.Lock()
		rf.persist()
		rf.mu.Unlock()
		go rf.sendAppendEntries(server, args, reply)
		
		return
	}
	DPrintf("=====================================================================\n")
	DPrintf("sendAppendEntries(from: %d(me), to: %d, PrevLogIndex: %d, commitIndex: %d, result: %v)\n", rf.me, server, args.PrevLogIndex, rf.commitIndex, reply.Success)
	DPrintf("me.state: %d, me.term: %d, reply.term: %d, args.term: %d\n", rf.state, rf.currentTerm, reply.Term, args.Term)
	DPrintf("ConflictIndex: %d, ConflictTerm: %d\n", reply.ConflictIndex, reply.ConflictTerm)
	// DPrintf("snapshot index: %d, logapi.headIndex: %d\n", rf.snapShotIndex, rf.logapi.headIndex)
	DPrintf("log: %v\n", rf.logapi.log)
	DPrintf("entries: %v\n", args.Entries)
	DPrintf("nextIndex: %v\n", rf.nextIndex)
	DPrintf("matchIndex: %v\n", rf.matchIndex)

	rf.mu.Lock()
	if reply.Term > rf.currentTerm {
		// reply.Success 一定是 false
		rf.mu.Unlock()
		rf.turn2Follower(-1, reply.Term)
		return
	}
	// 验证身份和任期
	if rf.state != leader || args.Term != rf.currentTerm {
		rf.mu.Unlock()
		return
	}

	if reply.Success {
		rf.matchIndex[server] = max(args.PrevLogIndex+len(args.Entries), rf.matchIndex[server])
		rf.nextIndex[server] = max(rf.matchIndex[server]+1, rf.nextIndex[server])
		rf.updateCommitIndex(-1)
		rf.mu.Unlock()
	} else {
		tmpInd := rf.logapi.findLast(reply.ConflictTerm)
		if tmpInd == -1 {
			rf.nextIndex[server] = min(reply.ConflictIndex, rf.logapi.tailIndex() + 1)
		} else {
			rf.nextIndex[server] = min(tmpInd + 1, rf.logapi.tailIndex() + 1)
		}
		
		if rf.nextIndex[server] - 1 < rf.snapShotIndex {
			sargs := InstallSnapshotArgs {
				Term: 				rf.currentTerm,
				LeaderId: 			rf.me,
				LastIncludedIndex: 	rf.snapShotIndex,
				LastIncludedTerm:	rf.snapShotTerm,
				Data:				rf.persister.ReadSnapshot(),
			}
			sreply := InstallSnapshotReply{}
			rf.persist()
			rf.mu.Unlock()
			
			go rf.sendInstallSnapshot(server, &sargs, &sreply)
		} else {
			args = &AppendEntriesArgs{
				Term: 			rf.currentTerm,
				LeaderId:		rf.me,
				PrevLogIndex:	rf.nextIndex[server] - 1,
				PrevLogTerm:	rf.logapi.at(rf.nextIndex[server]-1).Term,
				Entries:		[]LogEntry{},
				LeaderCommit:	min(rf.commitIndex, rf.matchIndex[server]),
			}
			reply = &AppendEntriesReply{}
			if rf.logapi.tail().Term == rf.currentTerm {
				for j := rf.nextIndex[server]; j <= rf.logapi.tailIndex(); j++ {
					args.Entries = append(args.Entries, rf.logapi.at(j))
				}
			}
			rf.persist()
			rf.mu.Unlock()
			go rf.sendAppendEntries(server, args, reply)
		}
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
	index := rf.logapi.tailIndex() + 1
	term := rf.currentTerm
	isLeader := (rf.state == 0)

	if isLeader {
		newLogEntry := LogEntry{
			Term:	rf.currentTerm,
			Data:	command,
		}
		rf.logapi.log = append(rf.logapi.log, newLogEntry)
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
						rf.persist()
						rf.mu.Unlock()
						
						go rf.sendInstallSnapshot(i, &args, &reply)
					} else {
						args := AppendEntriesArgs{
							Term: 			rf.currentTerm,
							LeaderId:		rf.me,
							PrevLogIndex:	rf.nextIndex[i] - 1,
							PrevLogTerm:	rf.logapi.at(rf.nextIndex[i]-1).Term,
							Entries:		[]LogEntry{},
							LeaderCommit:	min(rf.commitIndex, rf.matchIndex[i]),
						}
						reply := AppendEntriesReply{}
						if rf.nextIndex[i] <= rf.logapi.tailIndex() && 
							rf.logapi.tail().Term == rf.currentTerm {
							args.PrevLogIndex = rf.nextIndex[i] - 1
							args.PrevLogTerm = rf.logapi.at(args.PrevLogIndex).Term
							for j := rf.nextIndex[i]; j <= rf.logapi.tailIndex(); j++ {
								args.Entries = append(args.Entries, rf.logapi.at(j))
							}
						}
						rf.mu.Unlock()
						go rf.sendAppendEntries(i, &args, &reply)
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
	rf := &Raft{
		peers: peers,
		persister: persister,
		me:	me,

		currentTerm: 0,
		state: follower,
		
		timer: time.NewTimer(time.Duration(election_timeout+rand.Int31() % 151) * time.Millisecond),
		applyChan: applyCh,

		votedFor: -1,

		commitIndex: 0,
		lastApplied: 0,

		nextIndex: make([]int, len(peers)),
		matchIndex: make([]int, len(peers)),

		yes_vote: 0,

		snapShotIndex: 0,
		snapShotTerm: 0,
	}
	rf.logapi = logAPI{}
	rf.logapi.log = make([]LogEntry, 1)
	rf.applyCond = sync.NewCond(&rf.mu)
	
	// initialize from state persisted before a crash
	rf.readPersist(persister.ReadRaftState())

	rf.commitIndex, rf.lastApplied = rf.snapShotIndex, rf.snapShotIndex

	// start ticker goroutine to start elections

	go rf.applier()
	go rf.ticker()

	return rf
}

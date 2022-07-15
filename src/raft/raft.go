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
// apply Msg at normal cases
func (rf *Raft) normalApply() {
	for {
		rf.mu.Lock()
		if rf.lastApplied >= rf.commitIndex {
			rf.mu.Unlock()
			return
		}

		rf.lastApplied++
		tmpApplyMsg := ApplyMsg{
			CommandValid: 	true,
			Command:		rf.log[rf.indInLog(rf.lastApplied)].Data,
			CommandIndex:	rf.lastApplied,
		}
		rf.mu.Unlock()
		DPrintf("SERVER #%d: apply [%d](%v)\n", rf.me, tmpApplyMsg.CommandIndex, tmpApplyMsg.Command)

		rf.applyChan <- tmpApplyMsg
	}
}
// for updating commitIndex
func (rf *Raft) updateCommitIndex(newComInd int) {
	if newComInd != -1 {
		if newComInd > rf.commitIndex {
			rf.commitIndex = newComInd
		}
	} else if rf.state == leader {
		cnt := 1
		for i := 0; i < len(rf.peers); i++ {
			if i != rf.me && rf.matchIndex[i] > rf.commitIndex {
				cnt++
				if cnt > len(rf.peers)/2 {
					rf.commitIndex++
					break
				}
			}
		}
	}
}
// === code review ending ===

// debug
const debug bool = false
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


// change state (before the function)
// if currentTerm change, votedFor must change
func (rf *Raft) turn2Follower(vot int, cur int) {
	rf.allInfo("turn2Follower", -1, true, -1)
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
			go func(ind int) {
				rf.mu.Lock()
				args := RequestVoteArgs{
					Term: 			rf.currentTerm,
					CandidateId: 	rf.me,
					LastLogIndex:	rf.lenOfLog()-1,
					LastLogTerm:	rf.log[len(rf.log)-1].Term,
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
	enc.Encode(rf.log)
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
	var currentTerm0 int
	var log1 []LogEntry
	var votedFor2 int
	var snapShotIndex3 int
	var snapShotTerm4 int
	

	if dec.Decode(&currentTerm0) != nil ||
		dec.Decode(&log1) != nil ||
		dec.Decode(&votedFor2) != nil ||
		dec.Decode(&snapShotIndex3) != nil ||
		dec.Decode(&snapShotTerm4) != nil {
	// SOMETHING ERROR
	} else {
		rf.currentTerm, rf.log, rf.votedFor, rf.snapShotIndex, rf.snapShotTerm = 
			currentTerm0, log1, votedFor2, snapShotIndex3, snapShotTerm4
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
			rf.turn2Follower(rf.me, args.Term)
			
		case follower:
			rf.clearTimerC()
			rf.timer.Reset(time.Duration(election_timeout+rand.Int31() % 151) * time.Millisecond)
		}
	}

	rf.mu.Lock()
	
	// outdated snapshot
	if args.LastIncludedIndex <= rf.lastApplied {
		rf.persist()
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
	tmpLog := make([]LogEntry, 0)
	tmpLog = append(tmpLog, LogEntry{})
	for i := rf.indInLog(args.LastIncludedIndex) + 1; i < len(rf.log); i++ {
		tmpLog = append(tmpLog, rf.log[i])
	}
	rf.log = tmpLog
	rf.updateCommitIndex(args.LastIncludedIndex)
	if args.LastIncludedIndex > rf.lastApplied {
		rf.lastApplied = args.LastIncludedIndex
	}
	rf.snapShotTerm = args.LastIncludedTerm
	// rf.snapShotIndex = args.LastIncludedIndex
	rf.changeSnapShotInd("InstallSnapshot", args.LastIncludedIndex)
	rf.log[0].Term = rf.snapShotTerm
	rf.persist()
	rf.mu.Unlock()

	// rf.allInfo("InstallSnapshot(after)", args.LeaderId, true, args.LastIncludedIndex)
}

func (rf *Raft) changeSnapShotInd(pos string, val int) {
	DPrintf("changeSnapShotInd(#SERVER %d@%s): %d -> %d\n", rf.me, pos, rf.snapShotIndex, val)
	rf.snapShotIndex = val
}

func (rf *Raft) sendInstallSnapshot(server int, args *InstallSnapshotArgs, reply *InstallSnapshotReply) {
	ok := rf.peers[server].Call("Raft.InstallSnapshot", args, reply)
	
	if !ok {
		reply = &InstallSnapshotReply{}
		go rf.sendInstallSnapshot(server, args, reply)
		return
	}
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
	
	rf.allInfo("Snapshot", -1, true, -1)

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

	
	rf.snapShotTerm = rf.log[rf.indInLog(index)].Term
	tmpLog := make([]LogEntry, 0)
	tmpLog = append(tmpLog, LogEntry{})
	for i := rf.indInLog(index) + 1; i < len(rf.log); i++ {
		tmpLog = append(tmpLog, rf.log[i])
	}
	rf.log = tmpLog
	rf.updateCommitIndex(index)
	if index > rf.lastApplied {
		rf.lastApplied = index
	}
	// rf.snapShotIndex = index
	rf.changeSnapShotInd("Snapshot", index)
	rf.log[0].Term = rf.snapShotTerm

	rf.persister.SaveStateAndSnapshot(rf.encodeState(), snapshot)
	
	
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
	
	defer rf.allInfo("RequestVote", args.CandidateId, reply.VoteGranted, -1)
	
	// Reply false if term < currentTerm (&5.1)
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
	DPrintf("VoteGranted: %v\n",reply.VoteGranted)
}



func (rf *Raft) sendRequestVote(server int, args *RequestVoteArgs, reply *RequestVoteReply) {
	if rf.killed() {
		reply.Term = -1
		reply.VoteGranted = false
		return
	}
	ok := rf.peers[server].Call("Raft.RequestVote", args, reply)

	rf.allInfo("sendRequestVote", server, reply.VoteGranted, -1)
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
	rf.allInfo("AE", args.LeaderId, false, len(args.Entries))

	rf.mu.Lock()
	if args.Term < rf.currentTerm {
		reply.Term = rf.currentTerm
		rf.mu.Unlock()
		return
	}
	rf.mu.Unlock()

	switch rf.state {
		// args.Term >= rf.currentTerm
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
		}
		rf.mu.Unlock()

		rf.clearTimerC()
		rf.timer.Reset(time.Duration(election_timeout+rand.Int31() % 151) * time.Millisecond)
	}

	rf.mu.Lock()
	// try to append entries
	reply.Term = rf.currentTerm
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
		
		reply.Success = true
	}
	
	rf.persist()
	if args.LeaderCommit > rf.commitIndex {
		rf.updateCommitIndex(min(args.LeaderCommit, rf.lenOfLog() - 1))
		rf.mu.Unlock()
		rf.normalApply()

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
	
	ok := rf.peers[server].Call("Raft.AppendEntries", args, reply)
	DPrintf("ok: %v\n", ok)
	rf.allInfo("sendAE", server, reply.Success, len(args.Entries))


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

	if rf.state != leader {
		rf.mu.Unlock()
		return
	}
	rf.mu.Unlock()

	// rf.currentTerm >= reply.Term && len(args.Entries) != 0
	if reply.Success {
		rf.mu.Lock()
		rf.matchIndex[server] = max(args.PrevLogIndex+len(args.Entries), rf.matchIndex[server])
		rf.nextIndex[server] = max(rf.matchIndex[server]+1, rf.nextIndex[server])
		rf.updateCommitIndex(-1)
		rf.mu.Unlock()
		
		rf.normalApply()
	} else {
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
	
	rf.allInfo("Start", -1, true, -1)
	
	rf.mu.Lock()
	index := rf.lenOfLog()
	term := rf.currentTerm
	isLeader := (rf.state == 0)

	if isLeader {
		newLogEntry := LogEntry{
			Term:	rf.currentTerm,
			Data:	command,
		}
		DPrintf("Start -- command: %v, newLogEntry: %v\n", command, newLogEntry)
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
		log: make([]LogEntry, 1),
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
	
	// initialize from state persisted before a crash
	rf.readPersist(persister.ReadRaftState())

	rf.commitIndex, rf.lastApplied = rf.snapShotIndex, rf.snapShotIndex

	rf.allInfo("Make", -1, true, -1)
	// start ticker goroutine to start elections

	go rf.ticker()

	return rf
}

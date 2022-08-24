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

	// election_timeout		int32 = 100
	// append_entries_timeout	int32 = 70
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
func (la *logAPI) logDebug(peer int) {
	// DPrintf("LOG #%d: log(%v), headIndex(%d)\n", peer, la.log, la.headIndex)
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
		panic("logAPI.at(error): too old log.")
	}
	return la.log[ind - la.headIndex]
}
func (la *logAPI) index(ind int) int {
	if ind < la.headIndex {
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
func (la *logAPI) cutLogsArray(ind int) {
	la.log[la.index(ind + 1)] = LogEntry{}
	la.log = la.log[:la.index(ind + 1)]
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

	// for replicator
	replicatorCond 	[]*sync.Cond // used to signal replicator goroutine to batch replicating entries

	needPersist		bool
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
		// Debug(dCommit, "S%d(T:%d), apply(log[%d] = %v)\n", rf.me, rf.currentTerm, tmpApplyMsg.CommandIndex, tmpApplyMsg.Command)
		// DPrintf("SERVER #%d: apply(log[%d] = (%v))\n", rf.me, tmpApplyMsg.CommandIndex, tmpApplyMsg.Command)
		rf.mu.Unlock()
		rf.applyChan <- tmpApplyMsg
	}
}

// for updating commitIndex
// 注：更新 commitIndex 时一定要保证该 index 对应的 log 是当前任期的，不然会破坏性质4: Leader Completeness 
func (rf *Raft) updateCommitIndex(newComInd int) {
	if newComInd <= rf.commitIndex {
		return
	}

	if rf.state != leader {
		rf.commitIndex = newComInd
	} else {
		check := func(ind int) bool {
			cnt := 1
			for i := 0; i < len(rf.peers); i++ {
				if i == rf.me {
					continue
				}
				if rf.matchIndex[i] >= ind {
					cnt++
				}
				if cnt > len(rf.peers)/2 {
					return true
				}
			}
			return false
		}
		l, r := Max(rf.commitIndex + 1, rf.snapShotIndex + 1), rf.logapi.tailIndex()
		for l <= r {
			m := (l + r) / 2
			// if rf.logapi.at(m).Term < rf.currentTerm {
			// 	l = m + 1
			// } else 
			if !check(m) {
				r = m - 1
			} else {
				rf.commitIndex = Max(rf.commitIndex, m)
				l = m + 1
			}
		}
		// DPrintf("SERVER #%d(T:%d): (matchIndex = %v), (commitIndex = %d), (log_len = %d)\n", rf.me, rf.currentTerm, rf.matchIndex, rf.commitIndex, len(rf.logapi.log))
	}

	rf.applyCond.Signal()
}



// change state (before the function)
// if currentTerm change, votedFor must change
func (rf *Raft) turn2Follower(vot int, cur int, resetTimer bool) {
	if resetTimer {
		Debug(dTimer, "S%d become follower. timer(150-300)\n", rf.me)
		rf.timer.Reset(time.Duration(election_timeout+rand.Int31() % 151) * time.Millisecond)
	}

	if cur == rf.currentTerm {
		rf.state = follower
		if rf.votedFor == -1 {
			rf.votedFor = vot
			rf.needPersist = true
		}
	} else if cur > rf.currentTerm {
		rf.state = follower
		rf.votedFor = vot
		rf.currentTerm = cur
		rf.needPersist = true
	}
}

func (rf *Raft) turn2Candidate() {
	rf.state = candidate
	// Increment currentTerm
	rf.currentTerm ++
	// Vote for self
	rf.yes_vote = 1
	rf.votedFor = rf.me
	rf.needPersist = true

	// Reset election timer
	Debug(dTimer, "S%d become candidate. timer(150-300)\n", rf.me)
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
	Debug(dVote, "S%d(T:%d) get %d votes, become leader.\n", rf.me, rf.currentTerm, rf.yes_vote)
	rf.state = leader
	nextIndex_ := rf.logapi.tailIndex() + 1
	for i := 0; i < len(rf.peers); i++ {
		rf.nextIndex[i] = nextIndex_
		rf.matchIndex[i] = 0
	}

	// rf.timer.Reset(time.Duration(append_entries_timeout) * time.Millisecond)
	rf.broadcastEntries(true)
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

// 必须上锁
func (rf *Raft) persist() {
	if rf.needPersist {
		rf.persister.SaveRaftState(rf.encodeState())
		rf.needPersist = false
	}
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
		rf.needPersist = true
	}
}

func (rf *Raft) InstallSnapshot(args *InstallSnapshotArgs, reply *InstallSnapshotReply) {
	rf.mu.Lock()
	reply.Term = rf.currentTerm
	if args.Term < rf.currentTerm {
		rf.persist()
		rf.mu.Unlock()
		return
	}
	rf.mu.Unlock()
	// 保证 args.Term >= rf.currentTerm

	// 
	// 改变 state
	// 
	rf.mu.Lock()
	if args.Term > rf.currentTerm {
		rf.turn2Follower(-1, args.Term, true)
	} else {// args.Term == rf.currentTerm
		switch rf.state {
		case leader:
			// Election Safety 性质保证 args.Term != rf.currentTerm
			// 故流程不应该走到这里
		case candidate:
			rf.turn2Follower(rf.me, args.Term, true)
		case follower:
			Debug(dTimer, "S%d become follower because snapshot. timer(150-300)\n", rf.me)
			rf.timer.Reset(time.Duration(election_timeout+rand.Int31() % 151) * time.Millisecond)
		}
	}

	rf.persist()
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
	// 发送 rpc 之前必须检查 state, currentTerm 和调用 persist
	rf.mu.Lock()
	if rf.state != leader || 
	rf.currentTerm != args.Term {
		rf.mu.Unlock()
		return
	}
	rf.persist()
	rf.mu.Unlock()
	ok := rf.peers[server].Call("Raft.InstallSnapshot", args, reply)
	if !ok {
		return
	}

	rf.mu.Lock()
	defer rf.mu.Unlock()
	if reply.Term > rf.currentTerm {
		rf.turn2Follower(-1, reply.Term, true)
		return
	}

	// rpc 返回后, 修改状态之前, 必须检查 currentTerm 和 state
	if args.Term == rf.currentTerm && 
	rf.state == leader {
		rf.nextIndex[server] = Max(rf.nextIndex[server], args.LastIncludedIndex + 1)
	}
}


//
// A service wants to switch to snapshot.  Only do so if Raft hasn't
// have more recent info since it communicate the snapshot on applyCh.
//
func (rf *Raft) CondInstallSnapshot(lastIncludedTerm int, lastIncludedIndex int, snapshot []byte) bool {
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
}




// check if sender's log is up-to-date ?
func (rf *Raft) isUpToDate(lastLogIndex int, lastLogTerm int) (bool) {
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
	if rf.killed() {
		reply.Term = -1
		reply.VoteGranted = false
		return
	}

	Debug(dVote, "S%d(T:%d) is asked vote by S%d(T:%d)\n", rf.me, rf.currentTerm, args.CandidateId, args.Term)
	
	// Reply false if term < currentTerm (&5.1)
	rf.mu.Lock()
	defer rf.mu.Unlock()
	if args.Term < rf.currentTerm {
		reply.Term = rf.currentTerm
		reply.VoteGranted = false
		rf.persist()
		
		return
	}
	
	// If votedFor is null or candidatedId, and candidate's log is at
	// least as up-to-date as receiver's log, grant vote (&5.2, &5.4)
	is_up_to_date := rf.isUpToDate(args.LastLogIndex, args.LastLogTerm)
	
	// args.Term >= rf.currentTerm
	switch rf.state {
	case leader:
		if args.Term > rf.currentTerm {
			reply.Term = args.Term
			if is_up_to_date {
				reply.VoteGranted = true
				rf.turn2Follower(args.CandidateId, args.Term, true)
			} else {
				reply.VoteGranted = false
				rf.turn2Follower(-1, args.Term, true)	// 争议
			}
		} else {
			reply.Term = rf.currentTerm
			reply.VoteGranted = false
		}
	case candidate:
		if args.Term > rf.currentTerm {
			reply.Term = args.Term
			if is_up_to_date {
				reply.VoteGranted = true
				rf.turn2Follower(args.CandidateId, args.Term, true)
			} else {
				reply.VoteGranted = false
				rf.turn2Follower(-1, args.Term, true)	// 争议
			}
		} else {
			reply.Term = rf.currentTerm
			reply.VoteGranted = false
		}
	case follower:
		if args.Term > rf.currentTerm {
			reply.Term = args.Term
			if is_up_to_date {
				reply.VoteGranted = true
				rf.turn2Follower(args.CandidateId, args.Term, true)
			} else {
				reply.VoteGranted = false
				rf.turn2Follower(-1, args.Term, true)	// 争议
			}
		} else if args.Term == rf.currentTerm {
			reply.Term = args.Term
			if rf.votedFor < 0 && is_up_to_date {
				rf.votedFor = args.CandidateId
				rf.needPersist = true
				reply.VoteGranted = true
				Debug(dTimer, "S%d request vote. timer(150-300)\n", rf.me)
				rf.timer.Reset(time.Duration(election_timeout+rand.Int31() % 151) * time.Millisecond)
			} else {
				reply.VoteGranted = false
			}
			
		} else {
			reply.Term = rf.currentTerm
			// SOMETHING ERROR
		}
	}
	rf.persist()
}



func (rf *Raft) sendRequestVote(server int, args *RequestVoteArgs, reply *RequestVoteReply) {
	// if rf.killed() {
	// 	reply.Term = -1
	// 	reply.VoteGranted = false
	// 	return
	// }

	// 发送 rpc 之前必须检查 state, currentTerm 和调用 persist
	rf.mu.Lock()
	if rf.state != candidate || 
	rf.currentTerm != args.Term {
		rf.mu.Unlock()
		return
	}
	rf.persist()
	rf.mu.Unlock()

	if !rf.peers[server].Call("Raft.RequestVote", args, reply) {
		// 内置重试
		reply = &RequestVoteReply{}
		go rf.sendRequestVote(server, args, reply)
		return
	}
	
	Debug(dVote, "S%d(T:%d) voted by S%d? %v.\n", rf.me, rf.currentTerm, server, reply.VoteGranted)

	
	rf.mu.Lock()
	defer rf.mu.Unlock()

	if reply.Term > rf.currentTerm {// reply.Success must be false
		rf.turn2Follower(-1, reply.Term, true)
		return
	}

	// rpc 返回后, 修改状态之前, 必须检查 currentTerm 和 state
	if args.Term == rf.currentTerm && 
	rf.state == candidate && 
	reply.VoteGranted {
		rf.yes_vote++
		if rf.yes_vote > len(rf.peers)/2 {
			rf.turn2Leader()
			return
		}
	} 
}


// 
// 日志复制
// 
func (rf *Raft) AppendEntries(args *AppendEntriesArgs, reply *AppendEntriesReply) {
	rf.mu.Lock()
	defer rf.mu.Unlock()

	reply.Success = false
	reply.Term = rf.currentTerm

	if len(args.Entries) > 0 {
		Debug(dLog2, "S%d(T:%d)(state:%d) recv [%v] from S%d(T:%d)\n", rf.me, rf.currentTerm, rf.state, args.Entries, args.LeaderId, args.Term)
	}
	defer rf.logapi.logDebug(rf.me)

	if args.Term < rf.currentTerm {
		rf.persist()
		return
	}

	// 
	// 改变 state
	// 

	switch rf.state {
	// 保证: args.Term >= rf.currentTerm
	case leader:
		// Election Safety 性质保证此处 args.Term > rf.currentTerm
		if args.Term > rf.currentTerm {
			rf.turn2Follower(-1, args.Term, true)
		} // else SOMETHING ERROR
	case candidate:
		tmpVotedFor := rf.votedFor
		if args.Term > rf.currentTerm {
			tmpVotedFor = -1
		}
		rf.turn2Follower(tmpVotedFor, args.Term, true)
	case follower:
		if args.Term > rf.currentTerm {
			rf.currentTerm = args.Term
			rf.votedFor = args.LeaderId
			rf.needPersist = true
		}
		Debug(dTimer, "S%d. timer(150-300)\n", rf.me)
		rf.timer.Reset(time.Duration(election_timeout+rand.Int31() % 151) * time.Millisecond)
	}

	// 
	// 复制日志
	// 
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
		needCut := false
		for i := 0; i < len(args.Entries); i++ {
			if args.PrevLogIndex + 1 + i <= rf.logapi.tailIndex() {
				tmpInd := rf.logapi.index(args.PrevLogIndex+1+i)
				if rf.logapi.log[tmpInd] != args.Entries[i] {
					rf.logapi.log[tmpInd] = args.Entries[i]
					needCut = true
				}
			} else {
				rf.logapi.log = append(rf.logapi.log, args.Entries[i])
				needCut = true
			}
		}
		// 一定要把后面多余的日志删掉！
		if args.PrevLogIndex + len(args.Entries) < rf.logapi.tailIndex() && needCut {
			rf.logapi.cutLogsArray(args.PrevLogIndex + len(args.Entries))
		}
		
		rf.needPersist = true
	}

	rf.persist()
	if args.LeaderCommit > rf.commitIndex {
		rf.updateCommitIndex(Min(args.LeaderCommit, rf.logapi.tailIndex()))
		return
	}
}

func (rf *Raft) sendAppendEntries(server int, args *AppendEntriesArgs, reply *AppendEntriesReply) {
	// if rf.killed() {
	// 	reply.Term = -1
	// 	reply.Success = false
	// 	return
	// }

	// 发送 rpc 之前必须检查 state, currentTerm 和调用 persist
	rf.mu.Lock()
	if rf.state != leader || 
	rf.currentTerm != args.Term {
		rf.mu.Unlock()
		return
	}
	rf.persist()
	rf.mu.Unlock()

	
	if !rf.peers[server].Call("Raft.AppendEntries", args, reply) {
		Debug(dDrop, "S%d -> S%d is fail.\n", rf.me, server);
		
		return
	}
	
	rf.mu.Lock()
	defer rf.mu.Unlock()

	if reply.Term > rf.currentTerm {
		// reply.Success 一定是 false
		rf.turn2Follower(-1, reply.Term, true)
		return
	}

	// rpc 返回后, 修改状态之前, 必须检查 currentTerm 和 state
	if rf.state != leader || 
	args.Term != rf.currentTerm {
		return
	}

	if len(args.Entries) > 0 {
		Debug(dLog, "S%d(T:%d)(leader) send [%v] to S%d\n", rf.me, rf.currentTerm, args.Entries, server)
	}
	rf.logapi.logDebug(rf.me)

	if reply.Success {
		rf.matchIndex[server] = Max(args.PrevLogIndex+len(args.Entries), rf.matchIndex[server])
		rf.nextIndex[server] = Max(rf.matchIndex[server]+1, rf.nextIndex[server])
		rf.updateCommitIndex(rf.matchIndex[server])
	} else {
		tmpInd := rf.logapi.findLast(reply.ConflictTerm)
		if tmpInd == -1 {
			rf.nextIndex[server] = Min(reply.ConflictIndex, rf.logapi.tailIndex() + 1)
		} else {
			rf.nextIndex[server] = Min(tmpInd + 1, rf.logapi.tailIndex() + 1)
		}
		// Debug(dTest, "S%d [%d] signal\n", rf.me, server)
		// rf.replicatorCond[server].Signal()
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
	if rf.killed() {
		return -1, -1, false
	}
	
	rf.mu.Lock()
	index := rf.logapi.tailIndex() + 1
	term := rf.currentTerm
	isLeader := (rf.state == leader)

	if isLeader {
		newLogEntry := LogEntry{
			Term:	rf.currentTerm,
			Data:	command,
		}
		rf.logapi.log = append(rf.logapi.log, newLogEntry)
		rf.needPersist = true
		Debug(dLog, "S%d(T:%d)(leader) start(%v)\n", rf.me, rf.currentTerm, newLogEntry.Data)
		rf.broadcastEntries(false)
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

func (rf *Raft) genInstallSnapshotArgs() InstallSnapshotArgs {
	return InstallSnapshotArgs {
		Term: 				rf.currentTerm,
		LeaderId: 			rf.me,
		LastIncludedIndex: 	rf.snapShotIndex,
		LastIncludedTerm:	rf.snapShotTerm,
		Data:				rf.persister.ReadSnapshot(),
	}
}
func (rf *Raft) genInitAppendEntriesArgs(index int) AppendEntriesArgs {
	return AppendEntriesArgs{
		Term: 			rf.currentTerm,
		LeaderId:		rf.me,
		PrevLogIndex:	rf.nextIndex[index] - 1,
		PrevLogTerm:	rf.logapi.at(rf.nextIndex[index]-1).Term,
		Entries:		[]LogEntry{},
		LeaderCommit:	Min(rf.commitIndex, rf.matchIndex[index]),
	}
}

// The ticker go routine starts a new election if this peer hasn't received
// heartsbeats recently.
func (rf *Raft) ticker() {
	for rf.killed() == false {
		// Your code here to check if a leader election should
		// be started and to randomize sleeping time using
		// time.Sleep().
		<- rf.timer.C

		rf.mu.Lock()
		switch rf.state {
		case leader:
			// rf.timer.Reset(time.Duration(append_entries_timeout) * time.Millisecond)
			// DPrintf("#SERVER %d: heartbeat", rf.me)
			rf.broadcastEntries(true)
		case candidate:
			rf.turn2Candidate()
		case follower:
			rf.turn2Candidate()
		}
		rf.mu.Unlock()
	}
}

func (rf *Raft) needAppend(peer int) bool {
	rf.mu.Lock()
	defer rf.mu.Unlock()

	if (rf.state == leader && rf.nextIndex[peer] <= rf.logapi.tailIndex()) {
		return true
	}
	return false
}
func (rf *Raft) replicator(peer int) {
	rf.replicatorCond[peer].L.Lock()
	defer rf.replicatorCond[peer].L.Unlock()
	for rf.killed() == false {
		for !rf.needAppend(peer) {
			Debug(dTest, "S%d [%d] wait\n", rf.me, peer)
			rf.replicatorCond[peer].Wait()
		}
		
		Debug(dTimer, "S%d replicateEntries to S%d at replicator\n", rf.me, peer)
		rf.replicateEntries(peer)
	}
}

func (rf *Raft) replicateEntries(peer int) {
	// DPrintf("replicateEntries(%d)\n", peer)
	rf.mu.Lock()
	if rf.nextIndex[peer] - 1 < rf.snapShotIndex {
		args := rf.genInstallSnapshotArgs()
		reply := InstallSnapshotReply{}
		rf.mu.Unlock()

		// 注！这里不能开协程！
		// 这里如果开协程, replicator 外层的循环会导致很多没有必要的重复 rpc
		rf.sendInstallSnapshot(peer, &args, &reply)
	} else {
		args := rf.genInitAppendEntriesArgs(peer)
		reply := AppendEntriesReply{}
		if rf.nextIndex[peer] <= rf.logapi.tailIndex() && 
		rf.logapi.tail().Term == rf.currentTerm {
			args.PrevLogIndex = rf.nextIndex[peer] - 1
			args.PrevLogTerm = rf.logapi.at(args.PrevLogIndex).Term
			for j := rf.nextIndex[peer]; j <= rf.logapi.tailIndex(); j++ {
				args.Entries = append(args.Entries, rf.logapi.at(j))
			}
		}
		rf.mu.Unlock()
		rf.sendAppendEntries(peer, &args, &reply)
	}
}
func (rf *Raft) broadcastEntries(isHeartBeat bool) {
	// TODO: 现在要解决的问题就是 "幽灵复现" 的问题？ 研究好 test-1.err 中的 FAIL: TestCount2B
	Debug(dTimer, "S%d(state:%d) broadcastEntries(%v). timer(100)\n", rf.me, rf.state, isHeartBeat)
	rf.timer.Reset(time.Duration(append_entries_timeout) * time.Millisecond)
	
	for i := 0; i < len(rf.peers); i++ {
		if i == rf.me {
			continue
		}
		if isHeartBeat {
			Debug(dTimer, "S%d replicateEntries to S%d at heartbeat\n", rf.me, i)
			go rf.replicateEntries(i)
		} else {
			Debug(dTest, "S%d [%d] signal\n", rf.me, i)
			rf.replicatorCond[i].Signal()
		}
	}
}

func (rf *Raft) test_timer() {
	test_timer := time.NewTimer(100 * time.Millisecond)
	for rf.killed() == false {
		<- test_timer.C
		Debug(dInfo, "S%d: test timer.\n", rf.me)
		test_timer.Reset(100 * time.Millisecond)
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
	
	// TODO: 1. 修改定时器(必做); 2. 加入 no-op 功能
	DebugInit()
	rand.Seed(makeSeed())

	rf := &Raft{
		peers: peers,
		persister: persister,
		me: me,

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

		replicatorCond: make([]*sync.Cond, len(peers)),

		needPersist: false,
	}
	rf.logapi = logAPI{}
	rf.logapi.log = make([]LogEntry, 1)
	rf.applyCond = sync.NewCond(&rf.mu)
	
	// initialize from state persisted before a crash
	rf.readPersist(persister.ReadRaftState())

	rf.commitIndex, rf.lastApplied = rf.snapShotIndex, rf.snapShotIndex
	
	for i, nextInd := 0, rf.logapi.tailIndex() + 1; i < len(rf.peers); i++ {
		rf.matchIndex[i], rf.nextIndex[i] = 0, nextInd
		if i != rf.me {
			rf.replicatorCond[i] = sync.NewCond(&sync.Mutex{})
			go rf.replicator(i)
		}
	}

	// start ticker goroutine to start elections
	go rf.applier()
	go rf.ticker()

	return rf
}

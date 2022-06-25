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
	// "bytes"
	"sync"
	"sync/atomic"

	// "6.824/labgob"
	"6.824/labrpc"

	"time"
	"math/rand"
	"fmt"
)

// constant
const (
	leader 				int = 0
	candidate 			int = 1
	follower 			int = 2

	election_timeout		int32 = 150	// 150 - 300
	append_entries_timeout	int32 = 100
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

	// Your data here (2A, 2B, 2C).
	// Look at the paper's Figure 2 for a description of what
	// state a Raft server must maintain.
	currentTerm	int
	votedFor	int
	log			[]LogEntry

	commitIndex	int
	lastApplied	int

	// commitNum	int
	// commitRec	map[int]bool	
	
	// for leader
	nextIndex	[]int
	matchIndex	[]int

	// for candidate
	yes_vote	int

	state 		int 	// 0, leader; 1, candidate; 2, follower
	timer		*time.Timer
	applyChan	chan ApplyMsg
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
func (rf *Raft) clearTimerC() {
	if !rf.timer.Stop() && len(rf.timer.C)>0 {
		<- rf.timer.C
	}
}
func (rf *Raft) updateApplied() {
	rf.mu.Lock()
	for rf.commitIndex > rf.lastApplied {
		rf.lastApplied++
		rf.applyChan <- ApplyMsg{
			CommandValid: 	true,
			Command:		rf.log[rf.lastApplied].Data,
			CommandIndex:	rf.lastApplied,
		}
	}
	rf.mu.Unlock()
}
func (rf *Raft) updateCommitIndex() {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	if rf.state == leader {
		// for leader
		cnt := 1
		for i := 0; i < len(rf.peers); i++ {
			if i != rf.me && rf.matchIndex[i] > rf.commitIndex {
				cnt++
				if cnt > len(rf.peers)/2 {
					rf.commitIndex++
					return
				}
			}
		}
	}
}

// debug
const debug bool = false
func (rf *Raft) allInfo(pos string, to int, res bool, reply_term int, args_prev_log_index int, entries_len int) {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	if debug {
		fmt.Printf("=============== @%s ===============\n", pos)
		fmt.Printf("ID: %d\nTerm: %d\nState: %d\nCommitInd: %d\n", rf.me, rf.currentTerm, rf.state, rf.commitIndex)
		fmt.Printf("To: %d\nResult: %v\nreply.Term: %d\nargs.PrevLogIndex: %d\n", to, res, reply_term, args_prev_log_index)
		fmt.Printf("entries_len: %d\n", entries_len)
		fmt.Printf("Log: ")
		for i := 0; i < len(rf.log); i++ {
			fmt.Printf("(%d,%d) ", rf.log[i].Term, rf.log[i].Data)
		}
		fmt.Println("\n==============================")
	}
}
func (rf *Raft) senderInfo(to int, result bool, entries_len int) {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	if debug {
		fmt.Printf("=============== @sendAppendEntries ===============\n")
		fmt.Printf("ID: %d\nTerm: %d\nState: %d\nCommitInd: %d\n", rf.me, rf.currentTerm, rf.state, rf.commitIndex)
		fmt.Printf("To: %d\nResult: %v\n", to, result)
		fmt.Printf("entries_len: %d\n", entries_len)
		fmt.Printf("Log: ")
		for i := 0; i < len(rf.log); i++ {
			fmt.Printf("(%d,%d) ", rf.log[i].Term, rf.log[i].Data)
		}
		fmt.Println("\nnextIndex: ")
		for i := 0; i < len(rf.peers); i++ {
			fmt.Printf("%d ", rf.nextIndex[i])
		}
		fmt.Println("\nmatchIndex: ")
		for i := 0; i < len(rf.peers); i++ {
			fmt.Printf("%d ", rf.matchIndex[i])
		}
		fmt.Println("\n==============================")
	}
}
func (rf *Raft) receiverInfo(leaderCommit int) {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	if debug {
		fmt.Printf("=============== @AppendEntries ===============\n")
		fmt.Printf("ID: %d\nTerm: %d\nState: %d\nCommitInd: %d\n", rf.me, rf.currentTerm, 
			rf.state, rf.commitIndex)
		fmt.Printf("LeaderCommit: %d\n", leaderCommit)
		fmt.Printf("Log: ")
		for i := 0; i < len(rf.log); i++ {
			fmt.Printf("(%d,%d) ", rf.log[i].Term, rf.log[i].Data)
		}
		fmt.Println("\n==============================")
	}
}

// change state
func (rf *Raft) turn2Follower(vot int, cur int) {
	rf.state = follower
	rf.votedFor = vot
	rf.currentTerm = cur
	rf.clearTimerC()
	rf.timer.Reset(time.Duration(election_timeout+rand.Int31() % 151) * time.Millisecond)
}
func (rf *Raft) turn2Candidate() {
	var tmpTerm int

	rf.mu.Lock()

	rf.state = candidate
	// Increment currentTerm
	rf.currentTerm ++
	// Vote for self
	rf.yes_vote = 1
	rf.votedFor = rf.me
	// Reset election timer
	rf.clearTimerC()
	rf.timer.Reset(time.Duration(election_timeout) * time.Millisecond)

	tmpTerm = rf.currentTerm

	rf.mu.Unlock()

	for i := 0; i < len(rf.peers); i++ {
		if i != rf.me {
			args := RequestVoteArgs{
				Term: 			tmpTerm,
				CandidateId: 	rf.me,
				LastLogIndex:	0,
				LastLogTerm:	0,
			}
			reply := RequestVoteReply{}
			go rf.sendRequestVote(i, &args, &reply)
		}
	}
}
func (rf *Raft) turn2Leader() {
	var tmpTerm int
	var tmpCommitInd int

	rf.mu.Lock()

	rf.state = leader
	for i := 0; i < len(rf.peers); i++ {
		rf.nextIndex[i] = len(rf.log)
		rf.matchIndex[i] = 0
	}
	rf.clearTimerC()
	rf.timer.Reset(time.Duration(append_entries_timeout) * time.Millisecond)

	tmpTerm = rf.currentTerm
	tmpCommitInd = rf.commitIndex

	rf.mu.Unlock()

	for i := 0; i < len(rf.peers); i++ {
		if i != rf.me {
			args := AppendEntriesArgs{
				Term: 			tmpTerm,
				LeaderId:		rf.me,
				PrevLogIndex:	-1,
				PrevLogTerm:	-1,
				Entries:		[]LogEntry{},
				LeaderCommit:	tmpCommitInd,
			}
			reply := AppendEntriesReply{}
			go rf.sendAppendEntries(i, &args, &reply)
		}
	}
}

// return currentTerm and whether this server
// believes it is the leader.
func (rf *Raft) GetState() (int, bool) {
	var term int
	var isleader bool

	// Your code here (2A).
	rf.mu.Lock()
	term = rf.currentTerm
	isleader = (rf.state == leader)
	rf.mu.Unlock()

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
	// w := new(bytes.Buffer)
	// e := labgob.NewEncoder(w)
	// e.Encode(rf.xxx)
	// e.Encode(rf.yyy)
	// data := w.Bytes()
	// rf.persister.SaveRaftState(data)
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
	if lastLogTerm > rf.currentTerm {
		return true
	}

	if lastLogTerm == rf.currentTerm && 
	lastLogIndex >= len(rf.log) - 1 {
		return true
	} 

	return false
}

func (rf *Raft) RequestVote(args *RequestVoteArgs, reply *RequestVoteReply) {
	// Your code here (2A, 2B).
	
	// Reply false if term < currentTerm (&5.1)
	if args.Term < rf.currentTerm {
		rf.mu.Lock()
		reply.Term = rf.currentTerm
		reply.VoteGranted = false
		rf.mu.Unlock()
		return
	}

	// If votedFor is null or candidatedId, and candidate's log is at
	// least as up-to-date as receiver's log, grant vote (&5.2, &5.4)
	is_up_to_date := rf.isUpToDate(args.LastLogIndex, args.LastLogTerm)

	rf.mu.Lock()
	// args.Term >= rf.currentTerm
	switch rf.state {
	case leader:
		if args.Term > rf.currentTerm {
			if is_up_to_date {
				rf.turn2Follower(args.CandidateId, args.Term)
				reply.VoteGranted = true
			} else {
				rf.turn2Follower(-1, args.Term)
				reply.VoteGranted = false
			}
		} else {
			reply.VoteGranted = false
		}
		reply.Term = rf.currentTerm
	case candidate:
		if args.Term > rf.currentTerm {
			if is_up_to_date {
				rf.turn2Follower(args.CandidateId, args.Term)
				reply.VoteGranted = true
			} else {
				rf.turn2Follower(-1, args.Term)
				reply.VoteGranted = false
			}
		} else {
			reply.VoteGranted = false
		}
		reply.Term = rf.currentTerm
	case follower:
		if args.Term > rf.currentTerm {
			if is_up_to_date {
				rf.turn2Follower(args.CandidateId, args.Term)
				reply.VoteGranted = true
			} else {
				rf.turn2Follower(-1, args.Term)
				reply.VoteGranted = false
			}
		} else if args.Term == rf.currentTerm {
			rf.clearTimerC()
			rf.timer.Reset(time.Duration(election_timeout+rand.Int31() % 151) * time.Millisecond)

			if rf.votedFor < 0 {
				rf.votedFor = args.CandidateId
				reply.VoteGranted = true
			} else {
				reply.VoteGranted = false
			}
		} else {
			// SOMETHING ERROR
		}
		reply.Term = rf.currentTerm
	}
	rf.mu.Unlock()
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
	var got_it bool = false
	ok := rf.peers[server].Call("Raft.RequestVote", args, reply)

	if ok {
		// just for candidates
		if rf.state == candidate {
			if reply.VoteGranted {
				rf.mu.Lock()
				rf.yes_vote++
				if rf.yes_vote > len(rf.peers)/2 && rf.state == candidate {
					got_it = true
				}
				rf.mu.Unlock()

				if got_it {
					rf.turn2Leader()
				}
			} else if reply.Term > rf.currentTerm {
				rf.mu.Lock()
				rf.turn2Follower(-1, reply.Term)
				rf.mu.Unlock()
			}
		}
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
	if rf.killed() {
		reply.Term = -1
		reply.Success = false
		return
	}

	rf.mu.Lock()
	switch rf.state {
	case leader:
		rf.currentTerm = max(args.Term, rf.currentTerm)
		rf.turn2Follower(-1, rf.currentTerm)

		reply.Success = false
		reply.Term = rf.currentTerm
	case candidate:
		rf.currentTerm = max(args.Term, rf.currentTerm)
		rf.turn2Follower(-1, rf.currentTerm)

		reply.Success = false
		reply.Term = rf.currentTerm
	case follower:
		if args.Term < rf.currentTerm {
			reply.Success = false
		} else {
			rf.turn2Follower(args.LeaderId, args.Term)
			if args.PrevLogIndex == -1 {
				// heart beat
				reply.Success = true
			} else {
				if args.PrevLogIndex < len(rf.log) && 
					rf.log[args.PrevLogIndex].Term == args.PrevLogTerm {
					for i := 0; i < len(args.Entries); i++ {
						if args.PrevLogIndex + 1 + i < len(rf.log) {
							rf.log[args.PrevLogIndex + 1 + i] = args.Entries[i]
						} else {
							rf.log = append(rf.log, args.Entries[i])
						}
					}
					reply.Success = true
					// rf.receiverInfo(args.LeaderCommit)
				} else {
					reply.Success = false
				}
			}

			if args.LeaderCommit > rf.commitIndex {
				rf.commitIndex = min(args.LeaderCommit, len(rf.log) - 1)
			}
		}
		reply.Term = rf.currentTerm
	}
	rf.mu.Unlock()
	
	rf.updateApplied()
}

func (rf *Raft) sendAppendEntries(server int, args *AppendEntriesArgs, reply *AppendEntriesReply) {
	if rf.state != leader {
		return
	}
	ok := rf.peers[server].Call("Raft.AppendEntries", args, reply)
	rf.senderInfo(server, reply.Success, len(args.Entries))
	// if args.PrevLogIndex < rf.matchIndex[server] {
	// 	// this RPC is obsolete.
	// 	return
	// }

	if !ok {
		// communication error, just retry
		reply = &AppendEntriesReply{}
		go rf.sendAppendEntries(server, args, reply)
	} else if rf.state == leader {
		// just for leader
		if reply.Term > rf.currentTerm {
			// reply.Success must be false
			rf.turn2Follower(-1, reply.Term)
		} else {
			if args.PrevLogIndex == -1 {
				// heart beat
			} else {
				if reply.Success {
					rf.mu.Lock()
					rf.matchIndex[server] = max(args.PrevLogIndex+len(args.Entries), rf.matchIndex[server])
					rf.nextIndex[server] = max(rf.matchIndex[server]+1, rf.nextIndex[server])
					rf.mu.Unlock()
					
					rf.updateCommitIndex()
					rf.updateApplied()
					
				} else {
					rf.mu.Lock()
					rf.nextIndex[server] = min(rf.nextIndex[server], args.PrevLogIndex)
					args = &AppendEntriesArgs{
						Term: 			rf.currentTerm,
						LeaderId:		rf.me,
						PrevLogIndex:	-1,
						PrevLogTerm:	-1,
						Entries:		[]LogEntry{},
						LeaderCommit:	rf.commitIndex,
					}
					reply = &AppendEntriesReply{}

					args.PrevLogIndex = rf.nextIndex[server] - 1
					args.PrevLogTerm = rf.log[args.PrevLogIndex].Term
					for i := rf.nextIndex[server]; i < len(rf.log); i++ {
						args.Entries = append(args.Entries, rf.log[i])
					}
					rf.mu.Unlock()

					go rf.sendAppendEntries(server, args, reply)
				}
			}
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
	rf.mu.Lock()
	defer rf.mu.Unlock()

	if rf.killed() {
		return -1, -1, false
	}
	
	index := len(rf.log)
	term := rf.currentTerm
	isLeader := (rf.state == 0)

	if isLeader {
		newLogEntry := LogEntry{
			Term:	rf.currentTerm,
			Data:	command,
		}

		rf.log = append(rf.log, newLogEntry)
	}
	
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
		
		switch rf.state {
		case leader:
			if len(rf.timer.C) > 0 {
				rf.mu.Lock()
				// append entries timeout
				<- rf.timer.C
				rf.timer.Reset(time.Duration(append_entries_timeout) * time.Millisecond)
				rf.mu.Unlock()

				for i := 0; i < len(rf.peers); i++ {
					if i != rf.me {
						rf.mu.Lock()
						args := AppendEntriesArgs{
							Term: 			rf.currentTerm,
							LeaderId:		rf.me,
							PrevLogIndex:	-1,
							PrevLogTerm:	-1,
							Entries:		[]LogEntry{},
							LeaderCommit:	rf.commitIndex,
						}
						reply := AppendEntriesReply{}

						if rf.nextIndex[i] < len(rf.log) {
							args.PrevLogIndex = rf.nextIndex[i] - 1
							args.PrevLogTerm = rf.log[args.PrevLogIndex].Term
							for j := rf.nextIndex[i]; j < len(rf.log); j++ {
								args.Entries = append(args.Entries, rf.log[j])
							}
						}
						rf.mu.Unlock()
						
						go rf.sendAppendEntries(i, &args, &reply)
					}
				}
			}
		case candidate:
			if rf.yes_vote > len(rf.peers)/2 {
				rf.turn2Leader()
			} else if len(rf.timer.C) > 0 {
				// election time out
				<- rf.timer.C

				rf.turn2Candidate()
			}
		case follower:
			if len(rf.timer.C) > 0 {
				// election timeout
				<- rf.timer.C

				rf.turn2Candidate()
			}
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
	rf.mu.Lock()
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


	// initialize from state persisted before a crash
	rf.readPersist(persister.ReadRaftState())
	rf.mu.Unlock()

	// start ticker goroutine to start elections
	go rf.ticker()

	return rf
}

package raft

// The file raftapi/raft.go defines the interface that raft must
// expose to servers (or the tester), but see comments below for each
// of these functions for more details.
//
// Make() creates a new raft peer that implements the raft interface.

import (
	//	"bytes"
	"math/rand"
	"sync"
	"sync/atomic"
	"time"

	//	"6.5840/labgob"
	"6.5840/labrpc"
	"6.5840/raftapi"
	tester "6.5840/tester1"
)

type LogEntry struct {
	Command interface{}
	Term int
}

// A Go object implementing a single Raft peer.
type Raft struct {
	mu        sync.Mutex          // Lock to protect shared access to this peer's state
	peers     []*labrpc.ClientEnd // RPC end points of all peers
	persister *tester.Persister   // Object to hold this peer's persisted state
	me        int                 // this peer's index into peers[]
	dead      int32               // set by Kill()

	// Your data here (3A, 3B, 3C).
	// Look at the paper's Figure 2 for a description of what
	// state a Raft server must maintain.
	CurrentTerm int
	VotedFor    int
	log         []LogEntry
	State       int

	// timeStamp
	lastHeartbeat time.Time
	// re-set as new rand value at new term
	electionTimeOut time.Duration

	// volatile state on all servers
	CommitIndex int
	LastApplied int

	// volatile state on leaders
	NextIndex  []int
	MatchIndex []int

	applyCh chan raftapi.ApplyMsg
}

// return currentTerm and whether this server
// believes it is the leader.
func (rf *Raft) GetState() (int, bool) {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	return rf.CurrentTerm, rf.State == LEADER
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

// how many bytes in Raft's persisted log?
func (rf *Raft) PersistBytes() int {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	return rf.persister.RaftStateSize()
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
type AppendEntriesArgs struct {
	Term int
	LeaderId int
	PrevLogIndex int
	PrevLogTerm int
	Entries []LogEntry
	LeaderCommit int
}

type AppendEntriesReply struct {
	Term int
	Success bool
}

func (rf *Raft) AppendEntries(args *AppendEntriesArgs, reply *AppendEntriesReply) {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	currentTerm := rf.CurrentTerm
	
	// reply false if term < currentTerm
	if args.Term < currentTerm {
		reply.Term = currentTerm
		reply.Success = false
		return
	}

	if args.Term == currentTerm {
		rf.State = FOLLOWER
	}

	// only reset voteFor when received bigger term
	if args.Term > currentTerm {
		rf.CurrentTerm = args.Term
		rf.State = FOLLOWER
		rf.VotedFor = -1
		rf.lastHeartbeat = time.Now()
		rf.electionTimeOut = NewElectionTimeOut()
	}


	// check prevLogIndex and term
	// 1. log too short
	// 2. unmatched prefix
	if args.PrevLogIndex >= len(rf.log) || 
	   args.PrevLogTerm != rf.log[args.PrevLogIndex].Term {
		reply.Term = rf.CurrentTerm
		reply.Success = false
		return
	}

	// deal with conflicts
	if len(args.Entries) > 0 {
		for i, e := range args.Entries {
			idx := args.PrevLogIndex + i + 1

			if idx >= len(rf.log) {
				rf.log = append(rf.log, args.Entries[i:]...)
            	break
			} 

			if rf.log[idx].Term != e.Term {
				rf.log = rf.log[:idx]
				rf.log = append(rf.log, args.Entries[i:]...)
            	break
			}
		}
	}

	if args.LeaderCommit > rf.CommitIndex {
		rf.CommitIndex = min(args.LeaderCommit, len(rf.log) - 1)
	}

	reply.Term = rf.CurrentTerm
	reply.Success = true
}

func (rf *Raft) sendAppendEntries(server int, args *AppendEntriesArgs, reply *AppendEntriesReply) bool {
	ok := rf.peers[server].Call("Raft.AppendEntries", args, reply)
	return ok
}

type RequestVoteArgs struct {
	// Your data here (3A, 3B).
	Term         int
	CandidateId  int
	LastLogIndex int
	LastLogTerm  int
}

// example RequestVote RPC reply structure.
// field names must start with capital letters!
type RequestVoteReply struct {
	// Your data here (3A).
	Term        int
	VoteGranted bool
}

// example RequestVote RPC handler.
func (rf *Raft) RequestVote(args *RequestVoteArgs, reply *RequestVoteReply) {
	// Your code here (3A, 3B).
	rf.mu.Lock()
	defer rf.mu.Unlock()

	// if received smaller term, reply false
	// each RPC reply should include its current term
	if args.Term < rf.CurrentTerm {
		reply.Term = rf.CurrentTerm
		reply.VoteGranted = false
		return
	}

	if args.Term > rf.CurrentTerm {
		rf.CurrentTerm = args.Term
		rf.State = FOLLOWER
		rf.VotedFor = -1
	}


	reply.Term = rf.CurrentTerm
	lastIdx := len(rf.log) - 1
	lastTerm := rf.log[lastIdx].Term

	isLogUpToDate := args.LastLogTerm > lastTerm || 
		(args.LastLogTerm == lastTerm && args.LastLogIndex >= lastIdx)

	// candidate's log is at least as up-to-date as receiver's log
	if (rf.VotedFor == -1 || rf.VotedFor == args.CandidateId) && isLogUpToDate{
		// reset timer only when granted vote
		rf.VotedFor = args.CandidateId
		rf.lastHeartbeat = time.Now()
		rf.electionTimeOut = NewElectionTimeOut()
		rf.State = FOLLOWER
		reply.VoteGranted = true
	} else {
		reply.VoteGranted = false
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
	// Your code here (3B).
	rf.mu.Lock()
	defer rf.mu.Unlock()
	if rf.State != LEADER {
		return 0, 0, false
	}

	term := rf.CurrentTerm

	newLog := LogEntry{
		Command: command,
		Term: term,
	}

	rf.log = append(rf.log, newLog)
	index := len(rf.log) - 1
	rf.NextIndex[rf.me] = index
	rf.MatchIndex[rf.me] = index
	

	return index, term, true
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

func (rf *Raft) startElection() {
	rf.mu.Lock()
	// term++
	rf.CurrentTerm++
	electionTerm := rf.CurrentTerm
	// reset ElectionTimeOut at each new Term
	rf.electionTimeOut = NewElectionTimeOut()
	// state transition
	rf.State = CANDIDATE
	// vote for itself
	rf.VotedFor = rf.me
	lastIdx :=len(rf.log) - 1
	args := &RequestVoteArgs{
		Term: electionTerm,
		CandidateId: rf.me,
		LastLogIndex: lastIdx,
		LastLogTerm: rf.log[lastIdx].Term,
	}
	rf.mu.Unlock()

	// include vote from itself
	voteCounts := 1

	for i := range rf.peers {
		if i == rf.me {
			continue
		}

		go func (i int)  {
			reply := &RequestVoteReply{}
			ok := rf.sendRequestVote(i, args, reply)

			if !ok {
				return
			}

			rf.mu.Lock()

			if electionTerm < reply.Term {
				rf.CurrentTerm = reply.Term
				rf.State = FOLLOWER
				rf.VotedFor = -1
				rf.mu.Unlock()
				return
			}
			
			// Ensure the term hasn't changed since starting this election
			if rf.CurrentTerm != electionTerm {
				rf.mu.Unlock()
				return
			}

			// Ensure this server is still a candidate before processing the vote
			if rf.State != CANDIDATE {
				rf.mu.Unlock()
				return
			}

			if reply.VoteGranted {
				voteCounts++
				// mojority votes
				if voteCounts > len(rf.peers) / 2 {
					rf.mu.Unlock()
					go rf.becomeLeader()
					return
				}
			}

			rf.mu.Unlock()
		}(i)
	}
}

func (rf *Raft) becomeLeader() {
	rf.mu.Lock()
	rf.State = LEADER
	// Reinitialize state after election
	rf.NextIndex = make([]int, len(rf.peers))
	// for each server, initialize to 0
	rf.MatchIndex = make([]int, len(rf.peers))
	lastLogIndex := len(rf.log) - 1
	for i := 0; i < len(rf.NextIndex); i++ {
		// initialize to be lastIdx + 1 == len(log)
		rf.NextIndex[i] = lastLogIndex + 1
	}
	// update itself
	rf.MatchIndex[rf.me] = lastLogIndex
	rf.mu.Unlock()

	for rf.killed() == false {
		rf.mu.Lock()
		if rf.State != LEADER {
			rf.mu.Unlock()
			return
		}
		rf.mu.Unlock()

		for i := range rf.peers {
			if i == rf.me {
				continue
			}

			go func(i int) {
				// make sure still Leader
				rf.mu.Lock()
				if rf.State != LEADER {
					rf.mu.Unlock()
					return
				}
				// get snapshot
				term := rf.CurrentTerm
				leaderID := rf.me
				commitIndex := rf.CommitIndex
				lastLogIndex := len(rf.log) - 1
				next := rf.NextIndex[i]
				prevIndex := next - 1
				preTerm := rf.log[prevIndex].Term
				
				// append entries only when peers fall behind
				var entriesSlice []LogEntry
				if next <= lastLogIndex {
					entriesSlice = rf.log[next : lastLogIndex + 1]
				} 

				rf.mu.Unlock()

				args := &AppendEntriesArgs{
					Term: term,
					LeaderId: leaderID,
					PrevLogIndex: prevIndex,
					PrevLogTerm: preTerm,
					Entries: entriesSlice,
					LeaderCommit: commitIndex,
				}

				reply := &AppendEntriesReply{}
				// TODO: how to resend indefinitly until success
				ok := rf.sendAppendEntries(i, args, reply)
				
				// handle reply
				if !ok { return }

				rf.mu.Lock()
				currTerm := rf.CurrentTerm
				state := rf.State
				rf.mu.Unlock()

				if currTerm != args.Term || state != LEADER {
					return
				}

				if reply.Term > term {
					rf.mu.Lock()
					rf.CurrentTerm = reply.Term
					rf.State = FOLLOWER
					rf.VotedFor = -1
					rf.mu.Unlock()
					return
				}

				if reply.Success {
					newMatch := prevIndex + len(entriesSlice)
					rf.mu.Lock()
					if newMatch > rf.MatchIndex[i] {
						rf.MatchIndex[i] = newMatch
						rf.NextIndex[i] = newMatch + 1
					}
					
					for idx := lastLogIndex; idx >= commitIndex+1; idx-- {
						cnt := 0
						for p := range rf.peers {
							if rf.MatchIndex[p] >= idx {
								cnt++
							}
						}
						// 1. mojority
						// 2. only commit log of its current term
						if cnt > len(rf.peers) / 2 && rf.log[idx].Term == term{
							// prevent roll back
							rf.CommitIndex = max(rf.CommitIndex, idx)
							break
						}
					}
					rf.mu.Unlock()
				}

				if !reply.Success {
					// TODO: Currently simply roll back, need conflict improvement
					rf.mu.Lock()
					if rf.NextIndex[i] > 1 {
						rf.NextIndex[i] -= 1
					}
					rf.mu.Unlock()
				}

			}(i)
		}


		time.Sleep(time.Duration(100) * time.Millisecond)
	}
}

func (rf *Raft) ticker() {
	for rf.killed() == false {

		// Your code here (3A)
		// Check if a leader election should be started.
		rf.mu.Lock()
		elapsed := time.Since(rf.lastHeartbeat)
		state := rf.State
		rf.mu.Unlock()


		if (state == FOLLOWER || state == CANDIDATE ) && 
			elapsed > rf.electionTimeOut {
			rf.startElection()
		}

		// pause for a random amount of time between 50 and 350
		// milliseconds.
		ms := 50 + (rand.Int63() % 300)
		time.Sleep(time.Duration(ms) * time.Millisecond)
	}
}

func (rf *Raft) applier() {
	for rf.killed() == false {
		rf.mu.Lock()
		// need to apply commited logs
		if rf.LastApplied < rf.CommitIndex {
			nextIndex := rf.LastApplied + 1
			applyCommand := rf.log[nextIndex].Command
			rf.LastApplied = nextIndex
			applyIndex := nextIndex
			rf.mu.Unlock()

			applyMsg := raftapi.ApplyMsg{
				CommandValid: true,
				Command: applyCommand,
				CommandIndex: applyIndex,
			}

			rf.applyCh <- applyMsg
			continue
		} else {
			// no log to apply
			rf.mu.Unlock()
			time.Sleep(10 * time.Millisecond)
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
	persister *tester.Persister, applyCh chan raftapi.ApplyMsg) raftapi.Raft {
	rf := &Raft{}
	rf.peers = peers
	rf.persister = persister
	rf.me = me
	rf.applyCh = applyCh

	// Your initialization code here (3A, 3B, 3C).
	// 300ms - 400ms
	rf.electionTimeOut = NewElectionTimeOut()
	rf.CurrentTerm = 0
	rf.VotedFor = -1
	rf.State = FOLLOWER
	rf.lastHeartbeat = time.Now()
	// Add Dummy Entry
	rf.log = []LogEntry{}
	rf.log = append(rf.log, LogEntry{nil, 0})

	// initialize from state persisted before a crash
	rf.readPersist(persister.ReadRaftState())

	// start ticker goroutine to start elections
	go rf.ticker()
	go rf.applier()

	return rf
}

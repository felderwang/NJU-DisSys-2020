package raft

//
// this is an outline of the API that raft must expose to
// the service (or tester). see comments below for
// each of these functions for more details.
//
// rf = Make(...)
//   create a new Raft server.
// rf.Start(command interface{}) (index, term, IsLeader)
//   start agreement on a new Log entry
// rf.GetState() (term, IsLeader)
//   ask a Raft for its current term, and whether it thinks it is leader
// ApplyMsg
//   each time a new entry is committed to the Log, each Raft peer
//   should send an ApplyMsg to the service (or tester)
//   in the same server.
//

import (
	"bytes"
	"encoding/gob"
	"fmt"
	"labrpc"
	"math/rand"
	"sync"
	"time"
)

// import "bytes"
// import "encoding/gob"

const (
	LEADER_STATE      = 0
	FOLLOWER_STATE    = 1
	CANDIDATE_STATE   = 2
	HEARTBEAT_TIME    = 50
	ELECTION_TIME_MIN = 150
	ELECTION_TIME_MAX = 300
	REELECTION_TIME   = 1000
	TIME_UNIT         = time.Millisecond
)

func TimeoutTimerRandTime() time.Duration {
	RandElectionTime := ELECTION_TIME_MIN + rand.Intn(ELECTION_TIME_MAX-ELECTION_TIME_MIN)
	return time.Duration(RandElectionTime) * TIME_UNIT
}

//
// as each Raft peer becomes aware that successive Log entries are
// committed, the peer should send an ApplyMsg to the service (or
// tester) on the same server, via the applyCh passed to Make().
//
type ApplyMsg struct {
	Index       int
	Command     interface{}
	UseSnapshot bool   // ignore for lab2; only used in lab3
	Snapshot    []byte // ignore for lab2; only used in lab3
}
type Entry struct {
	// Index   int
	Term    int
	Command interface{}
}

//
// A Go object implementing a single Raft peer.
//
type Raft struct {
	mu        sync.Mutex
	peers     []*labrpc.ClientEnd
	persister *Persister
	me        int // index into peers[]
	//persistent state on all servers
	// Your data here.
	// Look at the paper's Figure 2 for a description of what
	// state a Raft server must maintain.
	CurrentTerm int
	VotedFor    int //-1 for nil
	Log         []Entry
	State       int //0 for Leader 1 for Follower 2 for Candidate
	//Volatile state on all servers
	CommitIndex int
	LastApplied int
	//Volatile state on leaders
	NextIndex  []int
	MatchIndex []int

	TimeoutTimer    *time.Timer
	ReelectionTimer *time.Timer

	SendHBChan         chan int
	CandidateToFollwer chan int
	LeaderToFollwer    chan int

	commitChan chan int

	ServerKill chan int
}

// return CurrentTerm and whether this server
// believes it is the leader.
func (rf *Raft) GetState() (int, bool) {

	var term int
	var IsLeader bool
	// Your code here.
	term = rf.CurrentTerm
	if rf.State == LEADER_STATE {
		IsLeader = true
	} else {
		IsLeader = false
	}
	return term, IsLeader
}

//
// save Raft's persistent state to stable storage,
// where it can later be retrieved after a crash and restart.
// see paper's Figure 2 for a description of what should be persistent.
//
func (rf *Raft) persist() {

	// Your code here.
	// Example:
	// w := new(bytes.Buffer)
	// e := gob.NewEncoder(w)
	// e.Encode(rf.xxx)
	// e.Encode(rf.yyy)
	// data := w.Bytes()
	// rf.persister.SaveRaftState(data)
	w := new(bytes.Buffer)
	e := gob.NewEncoder(w)
	e.Encode(rf.CurrentTerm)
	e.Encode(rf.VotedFor)
	e.Encode(rf.Log)
	data := w.Bytes()
	rf.persister.SaveRaftState(data)
}

//
// restore previously persisted state.
//
func (rf *Raft) readPersist(data []byte) {
	// Your code here.
	// Example:
	// r := bytes.NewBuffer(data)
	// d := gob.NewDecoder(r)
	// d.Decode(&rf.xxx)
	// d.Decode(&rf.yyy)
	r := bytes.NewBuffer(data)
	d := gob.NewDecoder(r)
	d.Decode(&rf.CurrentTerm)
	d.Decode(&rf.VotedFor)
	d.Decode(&rf.Log)
}

//
// example RequestVote RPC arguments structure.
//
type RequestVoteArgs struct {
	Term         int
	CandidateID  int
	LastLogIndex int
	LastLogTerm  int
	// Your data here.
}

//
// example RequestVote RPC reply structure.
//
type RequestVoteReply struct {
	Term        int
	VoteGranted bool
	// Your data here.
}

type AppendEntriesArgs struct {
	Term         int
	LeaderID     int
	PrevLogIndex int
	PrevLogTerm  int
	Entries      []Entry
	LeaderCommit int
}

type AppendEntriesReply struct {
	Term      int
	Success   bool
	NextIndex int
}

//
// example RequestVote RPC handler.
//
func (rf *Raft) RequestVote(args RequestVoteArgs, reply *RequestVoteReply) {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	if args.Term < rf.CurrentTerm {
		reply.Term = rf.CurrentTerm
		reply.VoteGranted = false
		fmt.Printf("Raft Server %v votefor %v false.args.Term %v < rf.CurrentTerm %v\n", rf.me, args.CandidateID, args.Term, rf.CurrentTerm)
		return
	}
	if args.Term == rf.CurrentTerm && rf.State == LEADER_STATE {
		reply.Term = rf.CurrentTerm
		reply.VoteGranted = false
		fmt.Printf("Raft Server %v votefor %v false for rf.State==Leader.\n", rf.me, args.CandidateID)
		return
	}
	if args.Term > rf.CurrentTerm {
		// fmt.Printf("Server %v(raft) get request from Server %v(args): args.Term=%v>rf.currentTerm=%v.\n", rf.me, args.CandidateID, args.Term, rf.CurrentTerm)

		rf.CurrentTerm = args.Term
		rf.VotedFor = -1
		rf.persist()
		if rf.State == CANDIDATE_STATE {
			fmt.Printf("Server %v candidate -> follower\n", rf.me)
			rf.CandidateToFollwer <- 1
			fmt.Printf("Atfer Server %v candidate -> follower, rf.State = %v\n", rf.me, rf.State)

		}
		if rf.State == LEADER_STATE {
			fmt.Printf("Server %v leader -> follower\n", rf.me)
			rf.LeaderToFollwer <- 1
			fmt.Printf("Atfer Server %v leader -> follower, rf.State = %v\n", rf.me, rf.State)
		}

	}
	if (args.LastLogTerm < rf.Log[len(rf.Log)-1].Term) || ((args.LastLogTerm == rf.Log[len(rf.Log)-1].Term) && (args.LastLogIndex < len(rf.Log)-1)) {
		reply.VoteGranted = false
		rf.persist()
		fmt.Printf("Raft Server %v votefor %v false for many.\n", rf.me, args.CandidateID)
	} else if rf.VotedFor == -1 || rf.VotedFor == args.CandidateID {
		rf.VotedFor = args.CandidateID
		reply.VoteGranted = true
		rf.persist()
		fmt.Printf("Server %v votefor %v success at term %v and Reset 1 TimeoutTimer\n", rf.me, args.CandidateID, rf.CurrentTerm)
		rf.TimeoutTimer.Reset(TimeoutTimerRandTime())
	}
	// Your code here.
}
func (rf *Raft) AppendEntries(args AppendEntriesArgs, reply *AppendEntriesReply) {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	// fmt.Printf("Server %v(raft) Server %v(args) AppendEntries args.Term(%v)  rf.CurrentTerm(%v).\n", rf.me, args.LeaderID, args.Term, rf.CurrentTerm)

	reply.Success = false
	reply.NextIndex = 1
	if args.Term < rf.CurrentTerm {
		reply.Term = rf.CurrentTerm
		reply.NextIndex = len(rf.Log)
		return
	}
	if args.Term > rf.CurrentTerm {
		// fmt.Printf("Before Server %v(raft) Server %v(args) args.Term(%v) > rf.CurrentTerm(%v).\n", rf.me, args.LeaderID, args.Term, rf.CurrentTerm)

		rf.CurrentTerm = args.Term
		rf.VotedFor = -1
		rf.persist()
		// fmt.Printf("After Server %v(raft) Server %v(args) args.Term(%v) = rf.CurrentTerm(%v).\n", rf.me, args.LeaderID, args.Term, rf.CurrentTerm)

		if rf.State == CANDIDATE_STATE { //todo args.LeaderID != rf.me
			rf.State = FOLLOWER_STATE
			rf.CandidateToFollwer <- 1
			rf.persist()
		}
		if rf.State == LEADER_STATE {
			rf.State = FOLLOWER_STATE
			rf.CurrentTerm = args.Term
			rf.VotedFor = -1
			rf.LeaderToFollwer <- 1
			rf.persist()
		}
	}
	// fmt.Printf("Server %v Reset 2 TimeoutTimer\n", rf.me)
	rf.TimeoutTimer.Reset(TimeoutTimerRandTime())
	reply.Term = rf.CurrentTerm

	localMaxIndex := len(rf.Log) - 1
	if args.PrevLogIndex > localMaxIndex { //args.PrevLogIndex too big
		reply.NextIndex = localMaxIndex + 1
		return
	}
	// if args.PrevLogIndex > 0{
	// 	localTerm := rf.Log[args.PrevLogIndex].Term
	// 	if args.PrevLogTerm != localTerm{
	// 		for i:=
	// 	}
	// }
	if args.PrevLogIndex >= 0 {
		if rf.Log[args.PrevLogIndex].Term == args.PrevLogTerm {
			rf.Log = rf.Log[:args.PrevLogIndex+1]
			rf.Log = append(rf.Log, args.Entries...)
			rf.persist()
			reply.Success = true
			reply.NextIndex = len(rf.Log) //todo
		} else {
			localCurrentTerm := rf.CurrentTerm
			for i := args.PrevLogIndex - 1; i >= 0; i++ {
				if rf.Log[i].Term != localCurrentTerm {
					reply.NextIndex = i + 1
					break
				}
			}
			// reply.NextIndex = args.PrevLogIndex - 1
			rf.persist()
			return
		}
	}
	if args.LeaderCommit > rf.CommitIndex {
		localMaxIndex := len(rf.Log) - 1
		if args.LeaderCommit > localMaxIndex {
			rf.CommitIndex = localMaxIndex
		} else {
			rf.CommitIndex = args.LeaderCommit
		}
		rf.commitChan <- 1
	}
	return
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
// returns true if labrpc says the RPC was delivered.
//
// if you're having trouble getting RPC to work, check that you've
// capitalized all field names in structs passed over RPC, and
// that the caller passes the address of the reply struct with &, not
// the struct itself.
//
func (rf *Raft) sendRequestVote(server int, args RequestVoteArgs, reply *RequestVoteReply) bool {
	ok := rf.peers[server].Call("Raft.RequestVote", args, reply)
	return ok
}

//
// the service using Raft (e.g. a k/v server) wants to start
// agreement on the next command to be appended to Raft's Log. if this
// server isn't the leader, returns false. otherwise start the
// agreement and return immediately. there is no guarantee that this
// command will ever be committed to the Raft Log, since the leader
// may fail or lose an election.
//
// the first return value is the index that the command will appear at
// if it's ever committed. the second return value is the current
// term. the third return value is true if this server believes it is
// the leader.
//
func (rf *Raft) Start(command interface{}) (int, int, bool) {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	var index, term int
	var IsLeader bool
	if rf.State != LEADER_STATE {
		index = -1
		term = rf.CurrentTerm
		IsLeader = false
	} else {
		index = len(rf.Log)
		term = rf.CurrentTerm
		IsLeader = true
		entry := Entry{term, command}
		rf.Log = append(rf.Log, entry)
		// fmt.Printf("Start() leader Server %v, index %v, term %v, entry %v, rf.Log %v.\n", rf.me, index, term, entry, rf.Log)
		rf.persist()

		rf.SendHBChan <- 1
	}
	return index, term, IsLeader
}

//
// the tester calls Kill() when a Raft instance won't
// be needed again. you are not required to do anything
// in Kill(), but it might be convenient to (for example)
// turn off debug output from this instance.
//
func (rf *Raft) Kill() {
	fmt.Printf("Server %v shut down.\n", rf.me)
	// rf.ServerKill <- 1
	return
	// Your code here, if desired.
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
	// fmt.Println("In Make")
	// Your initialization code here.
	var serverNum int = len(rf.peers)
	// var serverNum int
	// serverNum = 5
	rf.CurrentTerm = -1
	rf.VotedFor = -1
	rf.Log = make([]Entry, 1)
	// rf.Log[0].Command = interface{}
	rf.State = FOLLOWER_STATE
	rf.CommitIndex = 0
	rf.LastApplied = 0
	// rf.NextIndex = []int{}
	// rf.MatchIndex = []int{}
	rf.NextIndex = make([]int, serverNum)
	rf.MatchIndex = make([]int, serverNum)

	rf.SendHBChan = make(chan int)
	rf.CandidateToFollwer = make(chan int)
	rf.LeaderToFollwer = make(chan int)
	rf.commitChan = make(chan int)
	rf.ServerKill = make(chan int)

	rf.TimeoutTimer = time.NewTimer(TimeoutTimerRandTime())
	rf.ReelectionTimer = time.NewTimer(REELECTION_TIME * TIME_UNIT)
	// initialize from state persisted before a crash
	rf.readPersist(persister.ReadRaftState())
	go RaftServerCommit(rf, applyCh)
	// go func() {
	// 	for {
	// 		select {
	// 		case <-rf.commitChan:
	// 			// fmt.Printf("Server %v commitChan, rf.CommitIndex = %v.\n", rf.me, rf.CommitIndex)
	// 			index := rf.CommitIndex
	// 			for i := rf.LastApplied + 1; i <= index && i < len(rf.Log); i++ {
	// 				msg := ApplyMsg{Index: i, Command: rf.Log[i].Command}
	// 				// fmt.Printf("Server %v rf.LastApplied %v. applyMsg : %v\n", rf.me, rf.LastApplied, msg)
	// 				applyCh <- msg
	// 				rf.LastApplied = i
	// 				rf.persist()
	// 				fmt.Printf("Server %v applyCh sended, msg: %v. LastApplied+1: %v rf.CommintIndex %v\n", rf.me, msg, rf.LastApplied+1, index)
	// 			}
	// 		}
	// 	}
	// }()
	go RaftServer(rf)
	return rf
}

func RaftServerCommit(rf *Raft, applyCh chan ApplyMsg) {
	for {
		select {
		case <-rf.commitChan:
			// fmt.Printf("Server %v commitChan, rf.CommitIndex = %v.\n", rf.me, rf.CommitIndex)
			index := rf.CommitIndex
			for i := rf.LastApplied + 1; i <= index && i < len(rf.Log); i++ {
				msg := ApplyMsg{Index: i, Command: rf.Log[i].Command}
				// fmt.Printf("Server %v rf.LastApplied %v. applyMsg : %v\n", rf.me, rf.LastApplied, msg)
				applyCh <- msg
				rf.LastApplied = i
				rf.persist()
				fmt.Printf("Server %v applyCh sended, msg: %v. LastApplied+1: %v rf.CommintIndex %v\n", rf.me, msg, rf.LastApplied+1, index)
			}
		}
	}
}

func RaftServer(rf *Raft) {
	rf.Log[0].Term = -1
	// killcount := 0
	for i := 0; i < len(rf.peers); i++ {
		rf.NextIndex[i] = 1
		rf.MatchIndex[i] = 0
	}
	for {
		// select {
		// case <-rf.ServerKill:
		// 	killcount += 1
		// 	fmt.Printf("Server %v killcount = %v\n", rf.me, killcount)

		// 	if killcount == 2 {
		// 		fmt.Printf("Server %v has been shutdown\n", rf.me)
		// 		return
		// 	}
		// default:

		// }
		serverNum := len(rf.peers)
		switch state := rf.State; state {

		case LEADER_STATE:
			func() {
				fmt.Printf("Server %v in Leader state, Term = %v\n", rf.me, rf.CurrentTerm)
				for {
					rf.mu.Lock()
					if len(rf.NextIndex) < serverNum {

						for i := 0; i < serverNum-len(rf.NextIndex); i++ {
							rf.NextIndex = append(rf.NextIndex, 1)
							rf.MatchIndex = append(rf.MatchIndex, 0)
						}
					}
					for i := 0; i < serverNum; i++ {
						rf.NextIndex[i] = rf.LastApplied + 1
						rf.MatchIndex[i] = 0
					}
					rf.persist()
					rf.mu.Unlock()

					heartBeatTimer := time.NewTicker(HEARTBEAT_TIME * TIME_UNIT)
					defer heartBeatTimer.Stop()

					go func() {
						for _ = range heartBeatTimer.C {
							rf.SendHBChan <- 1
						}
					}()
					for {
						select {
						case <-rf.SendHBChan:
							for i := 0; i < serverNum; i++ {
								// fmt.Printf("Server %v rf.NextIndex[%v] = %v\n", rf.me, i, rf.NextIndex[i])
								if i != rf.me {
									rpcArgs := AppendEntriesArgs{}
									rpcReply := &AppendEntriesReply{}
									rpcArgs.Term = rf.CurrentTerm
									rpcArgs.LeaderID = rf.me
									rpcArgs.PrevLogIndex = rf.NextIndex[i] - 1
									// fmt.Printf("A Server %v rpcArgs.PrevLogIndex = %v\n", rf.me, rpcArgs.PrevLogIndex)

									if rpcArgs.PrevLogIndex > len(rf.Log)-1 {
										rpcArgs.PrevLogIndex = len(rf.Log) - 1
										// fmt.Printf("B Server %v rpcArgs.PrevLogIndex = %v\n", rf.me, rpcArgs.PrevLogIndex)
									}
									if rf.NextIndex[i] > 0 {
										rpcArgs.Entries = make([]Entry, len(rf.Log[rpcArgs.PrevLogIndex+1:]))
										copy(rpcArgs.Entries, rf.Log[rpcArgs.PrevLogIndex+1:])
									}
									// fmt.Printf("C Server %v rpcArgs.PrevLogIndex = %v\n", rf.me, rpcArgs.PrevLogIndex)
									rpcArgs.PrevLogTerm = rf.Log[rpcArgs.PrevLogIndex].Term
									rpcArgs.LeaderCommit = rf.CommitIndex

									go func(i int, args AppendEntriesArgs) {
										if ok := rf.peers[i].Call("Raft.AppendEntries", args, &rpcReply); ok {
											if rpcReply.Success {
												// fmt.Printf("Server %v(leader) appendentries to Server %v success.\n", rf.me, i)
												if len(args.Entries) > 0 {
													rf.mu.Lock()
													rf.NextIndex[i] = rpcReply.NextIndex
													// fmt.Printf("Server %v get next index %v from Server %v\n", rf.me, rf.NextIndex[i], i)
													rf.MatchIndex[i] = rf.NextIndex[i] - 1
													lastCommit := rf.CommitIndex
													for j := rf.CommitIndex + 1; j <= rf.MatchIndex[i]; j++ {
														commitServerNum := 1
														for serverIndex := range rf.peers {
															if serverIndex != rf.me && rf.MatchIndex[serverIndex] >= j {
																commitServerNum += 1
															}
														}
														if commitServerNum*2 > len(rf.peers) {
															lastCommit = j
														}
													}
													if lastCommit != rf.CommitIndex {
														// fmt.Printf("Server %v have commit %v\n", rf.me, lastCommit)
														rf.CommitIndex = lastCommit
														rf.commitChan <- 1
													}
													rf.mu.Unlock()
												}
											} else {
												rf.NextIndex[i] = rpcReply.NextIndex
											}
										}
									}(i, rpcArgs)
								}
							}
						case <-rf.LeaderToFollwer:
							fmt.Printf("Server %v leader -> follower in Term %v\n", rf.me, rf.CurrentTerm)
							rf.State = FOLLOWER_STATE
							rf.VotedFor = -1
							rf.persist()
							return
						}
					}
				}
			}()

		case FOLLOWER_STATE:
			for {
				select {
				case <-rf.TimeoutTimer.C:
					rf.CurrentTerm++
					fmt.Printf("Server %v Turn to candidate, term = %v. Reset 3 ReelectionTimer\n", rf.me, rf.CurrentTerm)
					rf.VotedFor = -1
					rf.State = CANDIDATE_STATE
					// rf.TimeoutTimer.Reset(TimeoutTimerRandTime())

					rf.ReelectionTimer.Reset(REELECTION_TIME * TIME_UNIT)
				}
				if rf.State == CANDIDATE_STATE {
					break
				}
			}
		case CANDIDATE_STATE:
			fmt.Printf("Server %v in candidate state term %v, totol vote is 1\n", rf.me, rf.CurrentTerm)
			func() {
				rf.VotedFor = rf.me
				vote := 1
				winElection := make(chan int)
				replyChan := make(chan *RequestVoteReply)
				go func() {
					for i := 0; i < serverNum; i++ {
						if i != rf.me {
							go func(i int) {
								rpcReply := &RequestVoteReply{}
								rpcArgs := RequestVoteArgs{rf.CurrentTerm, rf.me, len(rf.Log) - 1, rf.Log[len(rf.Log)-1].Term}
								ok := rf.peers[i].Call("Raft.RequestVote", rpcArgs, &rpcReply)
								if ok {
									replyChan <- rpcReply
								}
								return
							}(i)
						}
					}

					for {
						reply := <-replyChan
						if reply.Term > rf.CurrentTerm {
							rf.State = FOLLOWER_STATE
							rf.VotedFor = -1
							rf.CurrentTerm = reply.Term
							rf.persist()
							rf.CandidateToFollwer <- 1 //todo
							return
						}
						if reply.VoteGranted {
							vote += 1
							fmt.Printf("Server %v in candidate term %v get vote, total vote is %v\n", rf.me, rf.CurrentTerm, vote)
							if vote > serverNum/2 {
								winElection <- 1
								return
							}
						}
					}
				}()

				select {
				case <-winElection:
					rf.mu.Lock()
					rf.State = LEADER_STATE
					rf.VotedFor = -1
					fmt.Printf("Server %d become Leader from candidate in Term %d, state %d\n", rf.me, rf.CurrentTerm, rf.State)
					rf.persist()
					rf.NextIndex = make([]int, len(rf.peers))
					rf.MatchIndex = make([]int, len(rf.peers))
					for i := range rf.peers {
						rf.NextIndex[i] = len(rf.Log)
						rf.MatchIndex[i] = 0
					}
					rf.mu.Unlock()
					return
				case <-rf.CandidateToFollwer:
					rf.State = FOLLOWER_STATE
					rf.VotedFor = -1
					rf.persist()
					fmt.Printf("Server %d candidate -> follower in Term %d Reset 4 TimeoutTimer\n", rf.me, rf.CurrentTerm)

					rf.TimeoutTimer.Reset(TimeoutTimerRandTime())
					return
				case <-rf.ReelectionTimer.C:
					rf.CurrentTerm += 1
					rf.VotedFor = -1
					fmt.Printf("Server %v Reset 5 ReelectionTimer Term = %v\n", rf.me, rf.CurrentTerm)
					rf.ReelectionTimer.Reset(REELECTION_TIME * TIME_UNIT)
					rf.persist()
					return
				}
			}()
		}
	}
}

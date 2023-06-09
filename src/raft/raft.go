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
	"bytes"
	"fmt"
	"math/rand"
	"sync"
	"sync/atomic"
	"time"

	//	"6.5840/labgob"
	"6.5840/labgob"
	"6.5840/labrpc"
	Logger "6.5840/logger"
	"go.uber.org/zap"
)

const (
	ELECTION_TIMEOUT_MIN = 150
	ELECTION_TIMEOUT_MAX = 300
	HEARTBEAT_TIMEOUT    = 50
	VOTED_FOR_NONE       = -1
	LEADER_ID_NONE       = -1
	LEADER_LOG_TRAIL     = 20
	INIT_TERM            = 0
	INIT_INDEX           = 0
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

type Role int

const (
	Leader    Role = 1
	Follower  Role = 2
	Candidate Role = 4
)

type LogEntry struct {
	Index   int
	Term    int
	Command interface{}
}

func (l LogEntry) String() string {
	return fmt.Sprintf("Index: %d Term: %d Command: %v", l.Index, l.Term, l.Command)
}

type SnapshotCommand struct {
	Index int
	Bytes []byte
}

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

	role    Role
	roleStr string
	leader  atomic.Int32
	// Persistent state on all servers
	currentTerm atomic.Int32
	votedFor    atomic.Int32
	log         []LogEntry

	// Volatile state on all servers
	commitIndex atomic.Int32
	lastApplied atomic.Int32

	// Volatile state on leaders
	nextIndex  []int
	matchIndex []int

	// Channels
	eventCh           chan *Event
	applyCh           chan ApplyMsg
	appendEntriesCh   []chan int
	installSnapshotCh []chan int
	commitCh          chan bool
	snapshotCh        chan SnapshotCommand
	snapshotTrigger   chan bool

	//Utility
	logger                   *zap.SugaredLogger
	eventHandlers            map[Role]map[string]func(event *Event)
	shouldResetElectionTimer atomic.Bool

	// Snapshots
	lastSnapshotIndex atomic.Int32
	lastSnapshotTerm  atomic.Int32
}

// return currentTerm and whether this server
// believes it is the leader.
func (rf *Raft) GetState() (int, bool) {
	rf.lock()
	defer rf.unlock()
	term := int(rf.currentTerm.Load())
	return term, rf.role == Leader
}

func (rf *Raft) lock() {
	rf.mu.Lock()
}

func (rf *Raft) unlock() {
	rf.mu.Unlock()
}

func (rf *Raft) setVotedFor(votedFor int) {
	rf.votedFor.Store(int32(votedFor))
}

func (rf *Raft) getVotedFor() int {
	return int(rf.votedFor.Load())
}

func (rf *Raft) resetElectionTimer() {
	rf.shouldResetElectionTimer.Store(true)
}

func (rf *Raft) setCurrentTerm(term int) {
	rf.currentTerm.Store(int32(term))
}

func (rf *Raft) getCurrentTerm() int {
	return int(rf.currentTerm.Load())
}

func (rf *Raft) setLastApplied(index int) {
	rf.lastApplied.Store(int32(index))
}

func (rf *Raft) getLastApplied() int {
	return int(rf.lastApplied.Load())
}

func (rf *Raft) incLastApplied() {
	rf.lastApplied.Add(1)
}

func (rf *Raft) setCommitIndex(index int) {
	rf.commitIndex.Store(int32(index))
}

func (rf *Raft) getCommitIndex() int {
	return int(rf.commitIndex.Load())
}

func (rf *Raft) setNextIndex(index int, value int) {
	rf.nextIndex[index] = value
}

func (rf *Raft) getNextIndex(index int) int {
	return rf.nextIndex[index]
}

func (rf *Raft) setMatchIndex(index int, value int) {
	rf.matchIndex[index] = value
}

func (rf *Raft) getMatchIndex(index int) int {
	return rf.matchIndex[index]
}

func (rf *Raft) setRole(role Role) {
	rf.role = role
	switch rf.role {
	case Leader:
		rf.roleStr = "Leader"
	case Follower:
		rf.roleStr = "Follower"
	case Candidate:
		rf.roleStr = "Candidate"
	}
}

func (rf *Raft) getRole() Role {
	return rf.role
}

func (rf *Raft) getRoleString() string {
	switch rf.role {
	case Leader:
		return "Leader"
	case Follower:
		return "Follower"
	case Candidate:
		return "Candidate"
	default:
		return "Unknown"
	}
}

func (rf *Raft) getLastSnapshottedIndex() int {
	return int(rf.lastSnapshotIndex.Load())
}

func (rf *Raft) setLastSnapshottedIndex(index int) {
	rf.lastSnapshotIndex.Store(int32(index))
}

func (rf *Raft) getLastSnapshottedTerm() int {
	return int(rf.lastSnapshotTerm.Load())
}

func (rf *Raft) setLastSnapshottedTerm(term int) {
	rf.lastSnapshotTerm.Store(int32(term))
}

func (rf *Raft) getLogEntry(index int) LogEntry {
	firstIdx := rf.log[0].Index
	realIdx := index - firstIdx
	return rf.log[realIdx]
}

func (rf *Raft) trimLogs(index int) {
	firstIdx := rf.log[0].Index
	rf.log = rf.log[index-firstIdx+1:]
}

func (rf *Raft) addLogEntry(entries ...LogEntry) {
	rf.log = append(rf.log, entries...)
}

func (rf *Raft) setLeaderId(leader int) {
	rf.leader.Store(int32(leader))
}

// return the index of the last log entry or if the log is empty return -1
func (rf *Raft) getLastLogIndex() int {
	if len(rf.log) > 0 {
		return rf.log[len(rf.log)-1].Index
	}
	return rf.getLastSnapshottedIndex()
}

func (rf *Raft) getLastLogIndexTerm() (int, int) {
	if len(rf.log) > 0 {
		lastLog := rf.log[len(rf.log)-1]
		return lastLog.Index, lastLog.Term
	}
	return rf.getLastSnapshottedIndex(), rf.getLastSnapshottedTerm()
}

func (rf *Raft) incCurrentTerm() {
	rf.currentTerm.Add(1)
}

func (rf *Raft) String() string {
	return fmt.Sprintf("Server %d: Term %d State %s", rf.me, rf.currentTerm.Load(), rf.roleStr)
}

func (rf *Raft) generateRaftState() []byte {
	w := new(bytes.Buffer)
	e := labgob.NewEncoder(w)
	if e.Encode(rf.currentTerm.Load()) != nil || e.Encode(rf.getVotedFor()) != nil || e.Encode(rf.log) != nil || e.Encode(rf.getLastSnapshottedIndex()) != nil || e.Encode(rf.getLastSnapshottedTerm()) != nil {
		return nil
	}
	return w.Bytes()
}

// save Raft's persistent state to stable storage,
// where it can later be retrieved after a crash and restart.
// see paper's Figure 2 for a description of what should be persistent.
// before you've implemented snapshots, you should pass nil as the
// second argument to persister.Save().
// after you've implemented snapshots, pass the current snapshot
// (or nil if there's not yet a snapshot).
func (rf *Raft) persist() {
	raftState := rf.generateRaftState()
	if raftState == nil {
		rf.Errorf("Failed to generate raft state")
	} else {
		rf.persister.Save(raftState, rf.persister.ReadSnapshot())
	}
}

// restore previously persisted state.
func (rf *Raft) readPersist(data []byte) {
	if data == nil || len(data) < 1 { // bootstrap without any state?
		return
	}
	r := bytes.NewBuffer(data)
	d := labgob.NewDecoder(r)
	var currentTerm, votedFor, lastSnapshotIndex, lastSnapshotTerm int
	var log []LogEntry
	if d.Decode(&currentTerm) != nil || d.Decode(&votedFor) != nil || d.Decode(&log) != nil || d.Decode(&lastSnapshotIndex) != nil || d.Decode(&lastSnapshotTerm) != nil {
		rf.Errorf("Failed to decode raft state")
		return
	}
	rf.setCurrentTerm(currentTerm)
	rf.setLastSnapshottedIndex(lastSnapshotIndex)
	rf.setLastSnapshottedTerm(lastSnapshotTerm)
	rf.setVotedFor(votedFor)
	rf.log = log

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

func (rf *Raft) sendAppendEntries(server int, term int, args *AppendEntriesArgs, reply *AppendEntriesReply, serial int) {
	ok := rf.peers[server].Call("Raft.AppendEntries", args, reply)

	rf.lock()
	defer rf.unlock()

	if rf.getRole() != Leader || rf.killed() {
		return
	}

	if !ok {
		// HB failed No Retry
		if len(args.Entries) == 0 {
			return
		}
		// Outdated RPC
		if args.PrevLogIndex < rf.getNextIndex(server)-1 {
			return
		}
		select {
		case rf.appendEntriesCh[server] <- serial:
		default:
		}
	}

	if reply.Term > rf.getCurrentTerm() {
		rf.stepDown(reply.Term)
		rf.setLeaderId(reply.LeaderId)
		rf.resetElectionTimer()
		return
	}

	if term != rf.getCurrentTerm() {
		return
	}

	if reply.Success {
		oldNextIndex := rf.getNextIndex(server)
		oldMatchIndex := rf.getMatchIndex(server)

		nextIndex := max(args.PrevLogIndex+len(args.Entries)+1, oldNextIndex)
		matchIndex := max(args.PrevLogIndex+len(args.Entries), oldMatchIndex)
		rf.setNextIndex(server, nextIndex)
		rf.setMatchIndex(server, matchIndex)

		go rf.canCommit(term)

		if rf.getNextIndex(server) > oldNextIndex {
			select {
			case rf.snapshotTrigger <- true:
			default:
			}
		}

		return
	}
	installSnapshot := false
	if reply.XLen != 0 && reply.XTerm == 0 {
		rf.setNextIndex(server, reply.XLen)
	} else {
		var xindex, xterm int
		for i := len(rf.log) - 1; i >= -1; i-- {
			if i < 0 {
				xindex, xterm = rf.getLastLogIndexTerm()
			} else {
				xindex, xterm = rf.log[i].Index, rf.log[i].Term
			}

			if xterm == reply.XTerm {
				rf.setNextIndex(server, xindex+1)
				break
			}

			if xterm < reply.XTerm {
				rf.setNextIndex(server, reply.XIndex)
				break
			}

			if i < 0 {
				installSnapshot = true
				rf.setNextIndex(server, rf.getLastSnapshottedIndex()+1)
			}
		}
	}

	if installSnapshot || rf.getNextIndex(server) <= rf.getLastSnapshottedIndex() {
		select {
		case rf.installSnapshotCh[server] <- 0:
		default:
		}
	}

}

func (rf *Raft) sendInstallSnapshot(server int, args *InstallSnapshotArgs, reply *InstallSnapshotReply, serial int) {
	ok := rf.peers[server].Call("Raft.InstallSnapshot", args, reply)

	rf.lock()
	defer rf.unlock()

	if rf.getRole() != Leader || rf.killed() {
		return
	}

	if !ok {
		select {
		case rf.installSnapshotCh[server] <- serial:
		default:
		}
		return
	}

	if reply.Term > rf.getCurrentTerm() {
		rf.stepDown(reply.Term)
		rf.resetElectionTimer()
		return
	}

	if args.Term != rf.getCurrentTerm() {
		return
	}

	nextIndex := rf.getNextIndex(server)
	rf.setNextIndex(server, max(rf.getNextIndex(server), args.LastSnapshottedIndex+1))
	rf.setMatchIndex(server, max(rf.getMatchIndex(server), args.LastSnapshottedIndex))

	if rf.getNextIndex(server) > nextIndex {
		select {
		case rf.snapshotTrigger <- true:
		default:
		}
	}
}

func (rf *Raft) canCommit(term int) {
	rf.lock()
	defer rf.unlock()

	if rf.getRole() != Leader || rf.getCurrentTerm() != term || rf.killed() {
		return
	}

	if rf.getCommitIndex() >= rf.getLastLogIndex() {
		return
	}

	commitIndex := rf.getCommitIndex()
	for n := commitIndex + 1; n <= rf.getLastLogIndex(); n++ {
		if rf.getLogEntry(n).Term == term {
			count := 1
			for i := range rf.peers {
				if rf.getMatchIndex(i) >= n {
					count++
				}
			}
			if count > len(rf.peers)/2+1 {
				rf.setCommitIndex(n)
			}
		}
	}
	if commitIndex != rf.getCommitIndex() {
		select {
		case rf.commitCh <- true:
		default:
		}
	}
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
	isLeader := false

	rf.lock()
	defer rf.unlock()

	if rf.getRole() != Leader || rf.killed() {
		return index, term, isLeader
	}

	isLeader = true
	term = rf.getCurrentTerm()
	index = rf.getNextIndex(rf.me)
	rf.log = append(rf.log, LogEntry{
		Term:    term,
		Command: command,
		Index:   index,
	})

	rf.setNextIndex(rf.me, index+1)
	rf.setMatchIndex(rf.me, index)
	rf.persist()
	for i, ch := range rf.appendEntriesCh {
		if i != rf.me {
			select {
			case ch <- 0:
			default:
			}
		}
	}
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
	evt := rf.createEvent(EVENT_SHUTDOWN, nil, nil)
	rf.emit(evt, false)
}

func (rf *Raft) killed() bool {
	z := atomic.LoadInt32(&rf.dead)
	return z == 1
}

func (rf *Raft) ticker() {
	for !rf.killed() {
		if rf.shouldResetElectionTimer.Load() {
			rf.shouldResetElectionTimer.Store(false)
		} else {
			event := rf.createEvent(EVENT_START_ELECTIONS, nil, nil)
			rf.lock()
			if rf.getRole() != Leader {
				rf.emit(event, false)
			}
			rf.unlock()
		}
		// pause for a random amount of time between 50 and 350
		// milliseconds.
		ms := ELECTION_TIMEOUT_MIN + (rand.Int63() % ELECTION_TIMEOUT_MAX)
		time.Sleep(time.Duration(ms) * time.Millisecond)
	}
}

func (rf *Raft) serve() {
	// defer func() {
	// 	close(rf.eventCh)
	// }()
	for !rf.killed() {
		event, ok := <-rf.eventCh
		if rf.killed() || !ok {
			return
		}
		rf.lock()
		role := rf.getRole()
		rf.unlock()
		rf.eventHandlers[role][event.Name](event)
	}
}

func (rf *Raft) voteForSelf() {
	rf.lock()
	rf.setRole(Candidate)
	rf.setVotedFor(rf.me)
	rf.incCurrentTerm()
	rf.persist()
	rf.unlock()
}

func (rf *Raft) broadcastRequestVote() {
	rf.lock()
	if rf.getRole() != Candidate {
		rf.unlock()
		return
	}
	currentTerm := rf.getCurrentTerm()
	lastLogIndex, lastLogTerm := rf.getLastLogIndexTerm()
	rf.unlock()
	args := RequestVoteArgs{
		Term:         currentTerm,
		CandidateId:  rf.me,
		LastLogIndex: lastLogIndex,
		LastLogTerm:  lastLogTerm,
	}
	results := make(chan bool)
	for peer := range rf.peers {
		if peer == rf.me {
			continue
		}
		go func(peer int) {
			reply := RequestVoteReply{}
			rf.sendRequestVote(peer, &args, &reply)
			if reply.VoteGranted {
				results <- true
			} else {
				results <- false
			}
		}(peer)
	}
	votes := 1
	granted := 1 // vote for self
	go func() {
		for !rf.killed() {
			vote := <-results
			if vote {
				granted++
			}
			votes++
			if granted >= len(rf.peers)/2+1 {
				// We Win!
				evt := rf.createEvent(EVENT_END_ELECTIONS, true, nil)
				rf.emit(evt, false)
				return
			}

			if votes >= len(rf.peers)/2+1 {
				// We Lose!
				evt := rf.createEvent(EVENT_END_ELECTIONS, false, nil)
				rf.emit(evt, false)
				return
			}
		}
	}()
}

func (rf *Raft) winElections() {
	rf.Infof("won the election")
	rf.lock()
	rf.setRole(Leader)
	rf.resetVolatileState()
	term := rf.getCurrentTerm()
	rf.appendEntriesCh = make([]chan int, len(rf.peers))
	for i := range rf.appendEntriesCh {
		if i == rf.me {
			continue
		}
		rf.appendEntriesCh[i] = make(chan int)
		go rf.appender(i, rf.appendEntriesCh[i], term)
	}
	rf.installSnapshotCh = make([]chan int, len(rf.peers))
	for i := range rf.installSnapshotCh {
		if i == rf.me {
			continue
		}
		rf.installSnapshotCh[i] = make(chan int)
		go rf.snapshotInstaller(i, rf.installSnapshotCh[i], term)
	}
	rf.unlock()
	go rf.heartbeat(term)
}

func (rf *Raft) stepDown(term int) {
	rf.Debugf("Step down")
	rf.setRole(Follower)
	rf.setVotedFor(VOTED_FOR_NONE)
	rf.setCurrentTerm(term)
	rf.persist()
	select {
	case rf.snapshotTrigger <- true:
	default:
	}
}

func (rf *Raft) resetVolatileState() {
	lastLogIndex := rf.getLastLogIndex()
	rf.nextIndex = make([]int, len(rf.peers))
	rf.matchIndex = make([]int, len(rf.peers))
	for peer := range rf.peers {
		rf.setNextIndex(peer, lastLogIndex+1)
		rf.setMatchIndex(peer, 0)
	}
	rf.setMatchIndex(rf.me, lastLogIndex)
}

func (rf *Raft) isMyLogLeading(index int, term int) bool {
	lastIndex, lastTerm := rf.getLastLogIndexTerm()
	if lastTerm != term {
		return lastTerm > term
	}
	return lastIndex > index
}

func (rf *Raft) heartbeat(term int) {
	for !rf.killed() {
		rf.lock()
		role := rf.getRole()
		rf.unlock()
		if role != Leader || rf.getCurrentTerm() != term {
			return
		}

		event := rf.createEvent(EVENT_HEARTBEAT, nil, nil)
		rf.emit(event, false)

		time.Sleep(time.Duration(HEARTBEAT_TIMEOUT) * time.Millisecond)
	}
}

// Assumption is that the caller has the lock
func (rf *Raft) constructAppendEntriesRequest(server int) *AppendEntriesArgs {
	prevLogIndex := rf.getNextIndex(server) - 1
	prevLogTerm := rf.getLastSnapshottedTerm()
	if prevLogIndex > rf.getLastSnapshottedIndex() {
		prevLogTerm = rf.getLogEntry(prevLogIndex).Term
	}

	var entries []LogEntry

	if lastLogIndex := rf.getLastLogIndex(); lastLogIndex <= prevLogIndex {
		entries = nil
	} else if prevLogIndex >= rf.getLastSnapshottedIndex() {
		newEntries := rf.log[prevLogIndex-rf.log[0].Index+1:]
		entries = make([]LogEntry, len(newEntries))
		copy(entries, newEntries)
	} else {
		entries = nil
		return nil
	}
	args := AppendEntriesArgs{
		Term:         rf.getCurrentTerm(),
		LeaderId:     rf.me,
		PrevLogIndex: prevLogIndex,
		PrevLogTerm:  prevLogTerm,
		Entries:      entries,
		LeaderCommit: rf.getCommitIndex(),
	}
	return &args
}

func (rf *Raft) constructInstallSnapshotRequest(server int) *InstallSnapshotArgs {
	args := InstallSnapshotArgs{
		Term:                 rf.getCurrentTerm(),
		LeaderId:             rf.me,
		LastSnapshottedIndex: rf.getLastSnapshottedIndex(),
		LastSnapshottedTerm:  rf.getLastSnapshottedTerm(),
		Data:                 rf.persister.ReadSnapshot(),
	}
	rf.Debugf("Construct install sn: %v|T%v [%v]", rf.getLastSnapshottedIndex(), rf.getLastSnapshottedTerm(), len(rf.persister.ReadSnapshot()))
	return &args
}

func (rf *Raft) appender(server int, ch <-chan int, term int) {
	curr := 1
	for !rf.killed() {
		serial, ok := <-ch
		if !ok {
			return
		}
		rf.lock()
		if rf.getRole() != Leader || rf.getCurrentTerm() != term || rf.killed() {
			rf.Debugf("Appender for %v exiting", server)
			rf.unlock()
			return
		}
		args := rf.constructAppendEntriesRequest(server)
		if args == nil {
			rf.Infof("Cannot Send Append Entries to %v Should Send Install Snapshot", server)
			select {
			case rf.installSnapshotCh[server] <- 0:
			default:
			}
			rf.unlock()
			continue
		}
		rf.unlock()
		reply := AppendEntriesReply{}
		if serial == 0 || serial >= curr {
			go rf.sendAppendEntries(server, term, args, &reply, serial)
			curr++
		}
	}
}

func (rf *Raft) committer(applyCh chan<- ApplyMsg, commitCh chan bool) {
	defer func() {
		close(applyCh)
		for range commitCh {
			<-commitCh
		}
	}()
	for !rf.killed() {
		commit, ok := <-commitCh
		if !ok {
			return
		}
		rf.lock()
		if !commit {
			rf.commitCh = commitCh
			data := rf.persister.ReadSnapshot()

			if rf.getLastSnapshottedIndex() == 0 || len(data) == 0 {
				rf.unlock()
				continue
			}

			applyMsg := ApplyMsg{
				CommandValid:  false,
				SnapshotValid: true,
				Snapshot:      data,
				SnapshotIndex: rf.getLastSnapshottedIndex(),
				SnapshotTerm:  rf.getLastSnapshottedTerm(),
			}
			rf.unlock()
			applyCh <- applyMsg
			continue
		}
		for rf.commitCh != nil && rf.getCommitIndex() > rf.getLastApplied() {
			rf.incLastApplied()
			entry := rf.getLogEntry(rf.getLastApplied())
			rf.unlock()
			applyMsg := ApplyMsg{
				CommandValid: true,
				Command:      entry.Command,
				CommandIndex: entry.Index,
			}
			applyCh <- applyMsg
			rf.lock()
		}
		rf.unlock()

	}
}

func (rf *Raft) snapshotter(trigger <-chan bool) {
	var index int
	var snapshot []byte
	cmdCh := rf.snapshotCh
	for !rf.killed() {
		select {
		case cmd := <-cmdCh:
			index, snapshot = cmd.Index, cmd.Bytes
		case _, ok := <-trigger:
			if !ok {
				return
			}
		}
		rf.lock()
		suspendSnapshot := rf.shouldSuspendSnapshot(index)
		if cmdCh == nil {
			if suspendSnapshot {
				rf.unlock()
				continue
			}
			cmdCh = rf.snapshotCh
		}
		switch {
		case index <= rf.getLastSnapshottedIndex():
			// Already Snapshotted
		case suspendSnapshot:
			cmdCh = nil
		default:
			term := rf.getLogEntry(index).Term
			rf.trimLogs(index)
			rf.setLastSnapshottedTerm(term)
			rf.setLastSnapshottedIndex(index)

			state := rf.generateRaftState()
			rf.persister.Save(state, snapshot)

		}
		rf.unlock()
	}
}

func (rf *Raft) snapshotInstaller(server int, ch <-chan int, term int) {
	curr := 1
	var request *InstallSnapshotArgs
	for !rf.killed() {
		serial, ok := <-ch
		if !ok {
			return
		}
		rf.lock()
		if rf.getRole() != Leader || rf.getCurrentTerm() != term || rf.killed() {
			rf.unlock()
			return
		}
		switch {
		case request == nil || rf.getLastSnapshottedIndex() > request.LastSnapshottedIndex:
			request = rf.constructInstallSnapshotRequest(server)
			curr++
		case serial >= curr:
			curr++
		default:
			rf.unlock()
			continue
		}
		go rf.sendInstallSnapshot(server, request, &InstallSnapshotReply{}, serial)
		rf.unlock()
	}
}

func (rf *Raft) shouldSuspendSnapshot(index int) bool {
	if rf.getRole() != Leader {
		return false
	}

	for peer := range rf.peers {
		if dist := index - rf.getNextIndex(peer); dist >= 0 && dist <= LEADER_LOG_TRAIL {
			return true
		}
	}

	return false
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
	rf.logger = Logger.NewLogger(fmt.Sprintf("raft-%d.log", me))
	rf.Infof("Starting up raft-%d", me)
	Logger.SetDebugOff()

	rf.setVotedFor(VOTED_FOR_NONE)
	rf.setCurrentTerm(INIT_TERM)
	rf.setRole(Follower)
	rf.setLeaderId(LEADER_ID_NONE)
	rf.setLastSnapshottedIndex(INIT_INDEX)
	rf.setLastSnapshottedTerm(INIT_TERM)
	rf.resetElectionTimer()

	rf.applyCh = applyCh
	rf.eventCh = make(chan *Event)
	rf.commitCh = make(chan bool)
	rf.appendEntriesCh = nil
	rf.snapshotTrigger = make(chan bool, 1)
	rf.snapshotCh = make(chan SnapshotCommand)

	rf.eventHandlers = make(map[Role]map[string]func(event *Event))
	rf.eventHandlers[Follower] = make(map[string]func(event *Event))
	rf.eventHandlers[Candidate] = make(map[string]func(event *Event))
	rf.eventHandlers[Leader] = make(map[string]func(event *Event))

	rf.on(EVENT_HEARTBEAT, rf.handleHeartbeats, Leader)
	rf.on(EVENT_END_ELECTIONS, rf.handleEndElections, Candidate)
	rf.on(EVENT_START_ELECTIONS, rf.handleStartElections, Follower|Candidate)
	rf.on(EVENT_APPEND_ENTRIES, rf.handleAppendEntries, Follower|Candidate|Leader)
	rf.on(EVENT_REQUEST_VOTE, rf.handleRequestVote, Follower|Candidate|Leader)
	rf.on(EVENT_SHUTDOWN, rf.handleShutdown, Follower|Leader|Candidate)
	rf.on(EVENT_SNAPSHOT, rf.handleSnapshot, Follower|Leader|Candidate)
	rf.on(EVENT_INSTALL_SNAPSHOT, rf.handleInstallSnapshot, Follower|Candidate)

	// initialize from state persisted before a crash
	rf.readPersist(persister.ReadRaftState())

	rf.setCommitIndex(rf.getLastSnapshottedIndex())
	rf.setLastApplied(rf.getLastSnapshottedIndex())

	// start ticker goroutine to start elections
	go rf.ticker()
	go rf.serve()
	go rf.committer(applyCh, rf.commitCh)
	go rf.snapshotter(rf.snapshotTrigger)

	return rf
}

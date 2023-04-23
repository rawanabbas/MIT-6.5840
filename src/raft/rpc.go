package raft

type RequestVoteArgs struct {
	Term         int // candidate's term
	CandidateId  int // candidate requesting vote
	LastLogIndex int // index of candidate's last log entry
	LastLogTerm  int // term of candidate's last log entry
}

type RequestVoteReply struct {
	Term        int  // currentTerm, for candidate to update itself
	VoteGranted bool // true means candidate received vote
}

type AppendEntriesArgs struct {
	Term         int        // leader's term
	LeaderId     int        // so follower can redirect clients
	PrevLogIndex int        // index of log entry immediately preceding new ones
	PrevLogTerm  int        // term of prevLogIndex entry
	Entries      []LogEntry // log entries to store (empty for heartbeat; may send more than one for efficiency)
	LeaderCommit int        // leader's commitIndex
}

type AppendEntriesReply struct {
	Term     int  // currentTerm, for leader to update itself
	Success  bool // true if follower contained entry matching prevLogIndex and prevLogTerm
	LeaderId int  // leader id
	XLen     int  // length of the log diff
	XTerm    int  // term of the log diff
	XIndex   int  // index of the log diff
}

type InstallSnapshotArgs struct {
	Term                 int    // leader's term
	LeaderId             int    // so follower can redirect clients
	LastSnapshottedIndex int    // index of last snapshot
	LastSnapshottedTerm  int    // term of last snapshot
	Data                 []byte // raw data of the snapshot
}

type InstallSnapshotReply struct {
	Term int // currentTerm, for leader to update itself
}

// example RequestVote RPC handler.
func (rf *Raft) RequestVote(args *RequestVoteArgs, reply *RequestVoteReply) {
	replyCh := make(chan interface{})
	event := rf.createEvent(EVENT_REQUEST_VOTE, args, replyCh)
	rf.emit(event, false)
	if !rf.killed() {
		resp := <-replyCh
		r := resp.(*RequestVoteReply)
		reply.Term = r.Term
		reply.VoteGranted = r.VoteGranted
		return
	}
}

// the service says it has created a snapshot that has
// all info up to and including index. this means the
// service no longer needs the log through (and including)
// that index. Raft should now trim its log as much as possible.
func (rf *Raft) Snapshot(index int, snapshot []byte) {
	rf.Debugf("Snapshot called with index %v", index)
	cmd := &SnapshotCommand{
		Index: index,
		Bytes: snapshot,
	}
	event := rf.createEvent(EVENT_SNAPSHOT, cmd, nil)
	rf.emit(event, false)
}

func (rf *Raft) AppendEntries(args *AppendEntriesArgs, reply *AppendEntriesReply) {
	replyCh := make(chan interface{})
	event := rf.createEvent(EVENT_APPEND_ENTRIES, args, replyCh)
	rf.emit(event, false)
	if !rf.killed() {
		resp := <-replyCh
		r := resp.(*AppendEntriesReply)
		reply.Term = r.Term
		reply.Success = r.Success
		reply.LeaderId = r.LeaderId
		reply.XLen = r.XLen
		reply.XTerm = r.XTerm
		reply.XIndex = r.XIndex
		return
	}
}

func (rf *Raft) InstallSnapshot(args *InstallSnapshotArgs, reply *InstallSnapshotReply) {
	// Your code here (2D).
	replyCh := make(chan interface{})
	event := rf.createEvent(EVENT_INSTALL_SNAPSHOT, args, replyCh)
	rf.emit(event, false)
	if !rf.killed() {
		resp := <-replyCh
		r := resp.(*InstallSnapshotReply)
		reply.Term = r.Term
		return
	}
}

package raft

func (rf *Raft) defaultHandler(event *Event) {
	rf.logger.Warnf("%v Got an event %v while on state %v", rf.String(), event.Name, rf.getRoleString())
}

func (rf *Raft) handleRequestVote(event *Event) {
	rf.logger.Debugf("%v Got a RequestVote event", rf.String())
	request := event.Payload.(*RequestVoteArgs)
	reply := &RequestVoteReply{}

	rf.lock()
	defer rf.unlock()
	defer func() { event.Response <- reply }()

	reply.Term = rf.getCurrentTerm()
	reply.VoteGranted = false

	if request.Term < rf.getCurrentTerm() {
		return
	}

	if request.Term > rf.getCurrentTerm() {
		rf.stepDown(request.Term)
	}

	if rf.getVotedFor() == VOTED_FOR_NONE || rf.getVotedFor() == request.CandidateId {
		if !rf.isMyLogLeading(request.LastLogIndex, request.LastLogTerm) {
			reply.VoteGranted = true
			rf.setVotedFor(request.CandidateId)
			rf.resetElectionTimer()
			rf.persist()
		}
	}
}

func (rf *Raft) handleAppendEntries(event *Event) {
	rf.logger.Debugf("%v Got an AppendEntries event", rf.String())

	request := event.Payload.(*AppendEntriesArgs)
	reply := &AppendEntriesReply{}

	rf.lock()
	defer rf.unlock()
	defer func() { event.Response <- reply }()

	term := rf.getCurrentTerm()
	reply.Term = term
	reply.Success = false

	if request.Term < term || request.LeaderId == rf.me || rf.killed() {
		return
	}
	if request.Term > term {
		rf.stepDown(request.Term)
	}

	if rf.getRole() == Candidate && request.Term >= term {
		rf.stepDown(request.Term)
	}

	rf.resetElectionTimer()
	lastLogIndex := rf.getLastLogIndex()
	if request.PrevLogIndex > lastLogIndex {
		reply.XLen = lastLogIndex + 1
		return
	}

	var prevLogTerm int
	switch {
	case request.PrevLogIndex == rf.getLastSnapshottedIndex():
		prevLogTerm = rf.getLastSnapshottedTerm()
	case request.PrevLogIndex < rf.getLastSnapshottedIndex():
		request.PrevLogIndex = rf.getLastSnapshottedIndex()
		prevLogTerm = rf.getLastSnapshottedTerm()
		commonEntries := false
		for i := 0; i < len(request.Entries); i++ {
			if request.Entries[i].Index == rf.getLastSnapshottedIndex() && request.Entries[i].Term == rf.getLastSnapshottedTerm() {
				commonEntries = true
				request.Entries = request.Entries[i+1:]
				break
			}
		}
		if !commonEntries {
			request.Entries = make([]LogEntry, 0)
		}
	default:
		prevLogTerm = rf.getLogEntry(request.PrevLogIndex - 1).Term
	}

	if prevLogTerm != request.PrevLogTerm {
		reply.XTerm = prevLogTerm
		for i := request.PrevLogIndex - 1; i >= rf.getLastSnapshottedIndex(); i-- {
			reply.XIndex = rf.getLogEntry(i).Index
			if rf.getLogEntry(i).Term != prevLogTerm {
				break
			}
		}
		return
	}

	reply.Success = true
	if len(request.Entries) > 0 {
		entries := rf.log[request.PrevLogIndex-rf.getLastSnapshottedIndex():]
		var i int
		presist := false
		for i = 0; i < min(len(entries), len(request.Entries)); i++ {
			if entries[i].Term != request.Entries[i].Term {
				rf.log = rf.log[:request.PrevLogIndex-rf.getLastSnapshottedIndex()+i]
				presist = true
				break
			}
		}
		if i < len(request.Entries) {
			rf.log = append(rf.log, request.Entries[i:]...)
			presist = true
		}

		if presist {
			rf.persist()
		}
	}

}

func (rf *Raft) handleHeartbeats(event *Event) {
	for i, ch := range rf.appendEntriesCh {
		if i != rf.me {
			select {
			case ch <- 0:
			default:
			}
		}
	}

}

func (rf *Raft) handleStartElections(event *Event) {
	rf.logger.Debugf("%v Got a StartElections event", rf.String())
	rf.voteForSelf()
	rf.broadcastRequestVote()
}

func (rf *Raft) handleEndElections(event *Event) {
	rf.logger.Debugf("%v Got an EndElections event", rf.String())
	win := event.Payload.(bool)
	if win {
		rf.winElections()
	} else {
		rf.lock()
		term := rf.getCurrentTerm()
		rf.unlock()
		rf.stepDown(term)
	}
}

func (rf *Raft) handleShutdown(event *Event) {
	rf.logger.Debugf("%v Got a Shutdown event", rf.String())
}

func (rf *Raft) handleSnapshot(event *Event) {
	rf.logger.Debugf("%v Got a Snapshot event", rf.String())
}

func (rf *Raft) handleInstallSnapshot(event *Event) {
	rf.logger.Debugf("%v Got an InstallSnapshot event", rf.String())
}

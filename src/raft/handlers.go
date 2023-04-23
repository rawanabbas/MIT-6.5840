package raft

import "sync/atomic"

func (rf *Raft) defaultHandler(event *Event) {
	rf.Warnf("Got an event %v while on state %v", event.Name, rf.getRoleString())
}

func (rf *Raft) handleRequestVote(event *Event) {
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
	request := event.Payload.(*AppendEntriesArgs)
	reply := &AppendEntriesReply{}
	rf.Debugf(" an AppendEntries event %v with LC %v MC %v", len(request.Entries), request.LeaderCommit, rf.getCommitIndex())
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
		rf.Debugf("case1 [%v], returning %v", request.PrevLogIndex, lastLogIndex+1)
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
		// Skip common entries between snapshot and request
		for i := 0; i < len(request.Entries); i++ {
			if request.Entries[i].Index == rf.getLastSnapshottedIndex() && request.Entries[i].Term == rf.getLastSnapshottedTerm() {
				commonEntries = true
				rf.Debugf(" 1an AppendEntries trim %v -> %v", request.Entries, request.Entries[i+1:])
				request.Entries = request.Entries[i+1:]
				break
			}
		}
		if !commonEntries {
			request.Entries = make([]LogEntry, 0)
		}
	default:
		prevLogTerm = rf.getLogEntry(request.PrevLogIndex).Term
	}
	if prevLogTerm != request.PrevLogTerm {
		reply.XTerm = prevLogTerm
		for i := request.PrevLogIndex; i > rf.getLastSnapshottedIndex(); i-- {
			reply.XIndex = rf.getLogEntry(i).Index
			if rf.getLogEntry(i).Term != prevLogTerm {
				break
			}
		}
		rf.Debugf("case2 [%v], returning %v/%v", request.PrevLogIndex, reply.XIndex, reply.XTerm)
		return
	}
	rf.Debugf("appending, prev index %v, my last idx %v", request.PrevLogIndex, lastLogIndex)

	reply.Success = true
	if len(request.Entries) > 0 {
		entries := rf.log[request.PrevLogIndex-rf.getLastSnapshottedIndex():]
		var i int
		presist := false
		for i = 0; i < min(len(entries), len(request.Entries)); i++ {
			if entries[i].Term != request.Entries[i].Term {
				rf.Debugf("------Logs were [PLI: %v, LSI: %v, I:%v] %v  (i(%v) %v != %v)", request.PrevLogIndex, rf.getLastSnapshottedIndex(), i, rf.log, i, entries[i].Term, request.Entries[i].Term)
				rf.log = rf.log[:request.PrevLogIndex-rf.getLastSnapshottedIndex()+i]
				rf.Debugf("----Logs now %v", rf.log)
				presist = true
				break
			}
		}
		if i < len(request.Entries) {
			rf.Debugf(" 2 adding entries: %v to %v", request.Entries[i:], rf.log)
			rf.addLogEntry(request.Entries[i:]...)
			rf.Debugf("xxxxLogs now %v", rf.log)
			presist = true
		}

		if presist {
			rf.persist()
		}
	}
	if request.LeaderCommit > rf.getCommitIndex() {
		rf.setCommitIndex(min(request.LeaderCommit, rf.getLastLogIndex()))
		select {
		case rf.commitCh <- true:
		default:
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
	rf.Debugf("Got a StartElections event")
	rf.voteForSelf()
	rf.broadcastRequestVote()
}

func (rf *Raft) handleEndElections(event *Event) {
	rf.Debugf("Got an EndElections event")
	win := event.Payload.(bool)
	if win {
		rf.winElections()
	} else {
		rf.lock()
		term := rf.getCurrentTerm()
		rf.stepDown(term)
		rf.unlock()
	}
}

func (rf *Raft) handleShutdown(event *Event) {
	rf.Debugf("Got a Shutdown event")
	atomic.StoreInt32(&rf.dead, 1)
	rf.lock()
	for _, ch := range rf.appendEntriesCh {
		if ch != nil {
			close(ch)
		}
	}

	rf.appendEntriesCh = nil

	if rf.commitCh != nil {
		close(rf.commitCh)
	}
	rf.commitCh = nil
	close(rf.eventCh)
	rf.unlock()
}

func (rf *Raft) handleSnapshot(event *Event) {
	rf.Debugf("Got a Snapshot event")
	cmd := event.Payload.(*SnapshotCommand)
	rf.snapshotCh <- *cmd
}

func (rf *Raft) handleInstallSnapshot(event *Event) {
	rf.Debugf("Got an InstallSnapshot event")
	rf.lock()
	defer rf.unlock()
	request := event.Payload.(*InstallSnapshotArgs)
	reply := &InstallSnapshotReply{}
	rf.Debugf("===========================InstallSnapshot: %v/T%v", request.LastSnapshottedIndex, request.LastSnapshottedTerm)
	rf.Debugf("LOGS @ InstallSnapshot: %v/T%v: %v", request.LastSnapshottedIndex, request.LastSnapshottedTerm, rf.log)

	defer func() { event.Response <- reply }()
	term := rf.getCurrentTerm()
	reply.Term = term

	if request.Term < term || request.LeaderId == rf.me || rf.killed() {
		return
	}

	if request.Term > term {
		rf.stepDown(request.Term)
	}

	rf.resetElectionTimer()
	if request.LastSnapshottedIndex <= rf.getLastSnapshottedIndex() || request.LastSnapshottedIndex <= rf.getLastApplied() {
		return
	}

	rf.setLastSnapshottedIndex(request.LastSnapshottedIndex)
	rf.setLastSnapshottedTerm(request.LastSnapshottedTerm)
	rf.setCommitIndex(max(rf.getCommitIndex(), request.LastSnapshottedIndex))
	rf.setLastApplied(request.LastSnapshottedIndex)

	commonEntries := false
	for i, entry := range rf.log {
		if entry.Index == request.LastSnapshottedIndex && entry.Term == request.LastSnapshottedTerm {
			rf.log = rf.log[i+1:]
			rf.Debugf("LOGS common @ InstallSnapshot: %v/T%v", request.LastSnapshottedIndex, request.LastSnapshottedTerm, rf.log)
			commonEntries = true
			break
		}
	}

	if !commonEntries {
		rf.log = make([]LogEntry, 0)
	}

	rf.Debugf("Logs after Install Snapshot: %v | %v", rf.log, len(request.Data))
	rf.Debugf("last applied now is %v", rf.getLastApplied())
	state := rf.generateRaftState()
	rf.persister.Save(state, request.Data)
	rf.Debugf("===========================applying ins: %v/T%v", request.LastSnapshottedIndex, request.LastSnapshottedTerm)
	if rf.commitCh != nil {
		rf.Debugf("===========================not nil ins: %v/T%v", request.LastSnapshottedIndex, request.LastSnapshottedTerm)

		go func(ch chan bool) {
			rf.Debugf("===========================applying sending on ch: %v/T%v", request.LastSnapshottedIndex, request.LastSnapshottedTerm)
			ch <- false
			rf.Debugf("===========================send on ch ins: %v/T%v", request.LastSnapshottedIndex, request.LastSnapshottedTerm)

		}(rf.commitCh)
		rf.commitCh = nil
	}
	rf.Debugf("===========================done ins: %v/T%v", request.LastSnapshottedIndex, request.LastSnapshottedTerm)

}

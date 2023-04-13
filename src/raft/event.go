package raft

import "time"

const EVENT_REQUEST_VOTE = "RequestVote"
const EVENT_APPEND_ENTRIES = "AppendEntries"
const EVENT_START_ELECTIONS = "StartElections"
const EVENT_END_ELECTIONS = "EndElections"
const EVENT_RESET_ELECTIONS = "ResetElections"
const EVENT_HEARTBEAT = "Heartbeat"
const EVENT_SHUTDOWN = "Shutdown"
const EVENT_SNAPSHOT = "Snapshot"
const EVENT_INSTALL_SNAPSHOT = "InstallSnapshot"

type Event struct {
	Name         string
	Payload      interface{}
	Response     chan interface{}
	CreatedAt    time.Time
	CreatedState Role
}

func (rf *Raft) emit(event *Event, async bool) {
	if async {
		go func() {
			rf.eventCh <- event
		}()
	} else {
		rf.eventCh <- event
	}
}

// Event Handle Registration
func (rf *Raft) on(name string, handler func(event *Event), filter Role) {
	index := 0
	for ; filter != 0 && index < 3; filter = filter >> 1 {
		currentState := Role(1 << index)
		if filter&1 == 1 {
			rf.eventHandlers[currentState][name] = handler
		}

		index = index + 1
	}

	for _, state := range [3]Role{Leader, Follower, Candidate} {
		if _, ok := rf.eventHandlers[state][name]; !ok {
			rf.eventHandlers[state][name] = rf.defaultHandler
		}
	}
}

func (rf *Raft) createEvent(name string, payload interface{}, response chan interface{}) (event *Event) {
	rf.lock()
	defer rf.unlock()
	event = &Event{}
	event.Name = name
	event.Payload = payload
	event.Response = response
	event.CreatedAt = time.Now()
	event.CreatedState = rf.getRole()
	return
}

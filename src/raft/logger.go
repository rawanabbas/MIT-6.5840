package raft

func (rf *Raft) Debugf(msg string, args ...interface{}) {
	if rf.logger != nil {
		msg = rf.String() + " | " + msg
		rf.logger.Debugf(msg, args...)
	}
}

func (rf *Raft) Warnf(msg string, args ...interface{}) {
	if rf.logger != nil {
		msg = rf.String() + " | " + msg
		rf.logger.Warnf(msg, args...)
	}
}

func (rf *Raft) Errorf(msg string, args ...interface{}) {
	if rf.logger != nil {
		msg = rf.String() + " | " + msg
		rf.logger.Errorf(msg, args...)
	}
}

func (rf *Raft) Infof(msg string, args ...interface{}) {
	if rf.logger != nil {
		msg = rf.String() + " | " + msg
		rf.logger.Infof(msg, args...)
	}
}

func (rf *Raft) Fatalf(msg string, args ...interface{}) {
	if rf.logger != nil {
		msg = rf.String() + " | " + msg
		rf.logger.Fatalf(msg, args...)
	}
}

func (rf *Raft) Panicf(msg string, args ...interface{}) {
	if rf.logger != nil {
		msg = rf.String() + " | " + msg
		rf.logger.Panicf(msg, args...)
	}
}

func (rf *Raft) Printf(msg string, args ...interface{}) {
	if rf.logger != nil {
		msg = rf.String() + " | " + msg
		rf.logger.Infof(msg, args...)
	}
}

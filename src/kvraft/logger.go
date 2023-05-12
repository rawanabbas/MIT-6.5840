package kvraft

func (kv *KVServer) Debugf(msg string, args ...interface{}) {
	if kv.logger != nil {
		msg = kv.String() + " | " + msg
		kv.logger.Debugf(msg, args...)
	}
}

func (kv *KVServer) Warnf(msg string, args ...interface{}) {
	if kv.logger != nil {
		msg = kv.String() + " | " + msg
		kv.logger.Warnf(msg, args...)
	}
}

func (kv *KVServer) Errorf(msg string, args ...interface{}) {
	if kv.logger != nil {
		msg = kv.String() + " | " + msg
		kv.logger.Errorf(msg, args...)
	}
}

func (kv *KVServer) Infof(msg string, args ...interface{}) {
	if kv.logger != nil {
		msg = kv.String() + " | " + msg
		kv.logger.Infof(msg, args...)
	}
}

func (kv *KVServer) Fatalf(msg string, args ...interface{}) {
	if kv.logger != nil {
		msg = kv.String() + " | " + msg
		kv.logger.Fatalf(msg, args...)
	}
}

func (kv *KVServer) Panicf(msg string, args ...interface{}) {
	if kv.logger != nil {
		msg = kv.String() + " | " + msg
		kv.logger.Panicf(msg, args...)
	}
}

func (kv *KVServer) Printf(msg string, args ...interface{}) {
	if kv.logger != nil {
		msg = kv.String() + " | " + msg
		kv.logger.Infof(msg, args...)
	}
}

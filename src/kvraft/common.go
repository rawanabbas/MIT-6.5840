package kvraft

const (
	OK             = "OK"
	ErrNoKey       = "ErrNoKey"
	ErrWrongLeader = "ErrWrongLeader"
	ErrTimeout     = "ErrTimeout"
)

type Operation int

type Err string

const (
	GetOp    Operation = 0
	PutOp    Operation = 1
	AppendOp Operation = 2
)

// Put or Append
type PutAppendArgs struct {
	Key       string
	Value     string
	Op        Operation // "Put" or "Append"
	ClientId  int64
	RequestId int64
}

type PutAppendReply struct {
	Err Err
}

type GetArgs struct {
	Key       string
	ClientId  int64
	RequestId int64
}

type GetReply struct {
	Err   Err
	Value string
}

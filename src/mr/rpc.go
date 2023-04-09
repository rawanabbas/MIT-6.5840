package mr

//
// RPC definitions.
//
// remember to capitalize all names.
//

import (
	"os"
	"strconv"
)

//
// example to show how to declare the arguments
// and reply for an RPC.
//

const TASK_REQUEST = 1
const REDUCE_FINISH = 2
const MAP_FINISH = 3
const MAP = 4
const REDUCE = 5
const TASKS_DONE = 6

type ExampleArgs struct {
	X int
}

type ExampleReply struct {
	Y int
}

type ConnectionRequest struct {
	Addr string
}

type ConnectionReply struct {
	Success bool
}

type PingRequest struct {
}

type PingResponse struct {
	Success bool
}

// Add your RPC definitions here.
type UpdateStatusRequest struct {
	Type int
	Addr string
	Task string
}

type UpdateStatusReply struct {
	Filename       string
	NReduce        int
	TaskType       int
	ReduceFileList []string
	TaskNumber     int
	Task           int
}

type IntermediateFileRequest struct {
	Filename   string
	TaskNumber int
}

type IntermediateFileReply struct {
	Success bool
}

// Cook up a unique-ish UNIX-domain socket name
// in /var/tmp, for the coordinator.
// Can't use the current directory since
// Athena AFS doesn't support UNIX-domain sockets.
func coordinatorSock() string {
	s := "/var/tmp/5840-mr-"
	s += strconv.Itoa(os.Getuid())
	return s
}

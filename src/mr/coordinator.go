package mr

import (
	"fmt"
	"log"
	"net"
	"net/http"
	"net/rpc"
	"os"
	"strconv"
	"sync"
	"time"
)

const WORKER_UNALLOCATED = 0
const WORKER_MAP = 1
const WORKER_REDUCE = 2

const IDLE = 0
const BUSY = 1
const FINISHED = 2

type Coordinator struct {
	mapTasksFinished    bool
	reduceTasksFinished bool
	nReduce             int
	mTaskNumber         int
	rTaskNumber         int
	mtx                 *sync.RWMutex
	reduceTaskStatus    []int
	intermediateFiles   [][]string
	filenames           map[string]int
	workers             map[string]*WorkerS
	mapTasks            chan string
	reduceTasks         chan int
	jobsCompleted       chan bool
}

// Your code here -- RPC handlers for the worker to call.
func (c *Coordinator) Connect(request *ConnectionRequest, reply *ConnectionReply) error {
	log.Printf("Recieved a Connection Request from a Worker %v", request.Addr)
	c.mtx.Lock()
	worker := CreateWorker(request.Addr, WORKER_UNALLOCATED, "")
	c.workers[request.Addr] = &worker
	c.mtx.Unlock()
	reply.Success = true
	return nil
}

func (c *Coordinator) UpdateStatus(request *UpdateStatusRequest, reply *UpdateStatusReply) error {
	c.mtx.Lock()
	worker := c.workers[request.Addr]
	c.mtx.Unlock()
	if request.Type == TASK_REQUEST {
		select {
		case filename := <-c.mapTasks:
			// log.Println("A New Map Task")
			reply.TaskType = MAP
			reply.Filename = filename
			c.mtx.Lock()
			reply.NReduce = c.nReduce
			reply.TaskNumber = c.mTaskNumber
			c.mTaskNumber++
			c.filenames[filename] = BUSY
			worker := c.workers[request.Addr]
			worker.TaskType = WORKER_MAP
			worker.Task = filename
			c.mtx.Unlock()
		case reduceIdx := <-c.reduceTasks:
			// log.Println(">>>>>>>>>>>>>New Reduce Task")
			reply.TaskType = REDUCE
			c.mtx.Lock()
			reply.NReduce = c.nReduce
			reply.ReduceFileList = c.intermediateFiles[reduceIdx]
			reply.TaskNumber = c.rTaskNumber
			reply.Task = reduceIdx
			c.rTaskNumber++
			c.reduceTaskStatus[reduceIdx] = BUSY
			worker := c.workers[request.Addr]
			worker.TaskType = WORKER_REDUCE
			worker.Task = strconv.Itoa(reduceIdx)
			c.mtx.Unlock()
		case <-c.jobsCompleted:
			log.Printf("No More Jobs Terminate Worker")
			reply.TaskType = TASKS_DONE
		}
	} else if request.Type == MAP_FINISH {
		worker.TaskType = WORKER_UNALLOCATED
		worker.Task = ""
		c.mtx.Lock()
		c.filenames[request.Task] = FINISHED
		c.mtx.Unlock()
	} else if request.Type == REDUCE_FINISH {
		worker.TaskType = WORKER_UNALLOCATED
		worker.Task = ""
		idx, _ := strconv.Atoi(request.Task)
		c.mtx.Lock()
		c.reduceTaskStatus[idx] = FINISHED
		c.mtx.Unlock()
	}
	return nil
}

func (c *Coordinator) AddIntermediateFiles(request *IntermediateFileRequest, reply *IntermediateFileReply) error {
	taskNumber := request.TaskNumber
	filename := request.Filename
	c.mtx.Lock()
	c.intermediateFiles[taskNumber] = append(c.intermediateFiles[taskNumber], filename)
	c.mtx.Unlock()
	reply.Success = true
	return nil

}

// an example RPC handler.
//
// the RPC argument and reply types are defined in rpc.go.
func (c *Coordinator) Example(args *ExampleArgs, reply *ExampleReply) error {
	reply.Y = args.X + 1
	return nil
}

// start a thread that listens for RPCs from worker.go
func (c *Coordinator) server() {
	rpc.Register(c)
	rpc.HandleHTTP()
	//l, e := net.Listen("tcp", ":1234")
	go c.generateTasks()
	sockname := coordinatorSock()
	os.Remove(sockname)
	l, e := net.Listen("unix", sockname)
	if e != nil {
		log.Fatal("listen error:", e)
	}
	go http.Serve(l, nil)
	go c.startHeartbeatForWorkers()
}

// main/mrcoordinator.go calls Done() periodically to find out
// if the entire job has finished.
func (c *Coordinator) Done() bool {
	c.mtx.RLock()
	ret := c.reduceTasksFinished
	c.mtx.RUnlock()
	return ret
}

func (c *Coordinator) generateTasks() {
	// Generate Map Tasks
	// c.mtx.RLock()
	// defer c.mtx.RUnlock()
	fnCopy := make(map[string]int)
	c.mtx.Lock()
	tskCopy := make([]int, len(c.reduceTaskStatus))

	for filename, status := range c.filenames {
		fnCopy[filename] = status
	}
	copy(tskCopy, c.reduceTaskStatus)
	c.mtx.Unlock()
	for filename, status := range fnCopy {
		if status == IDLE {
			c.mapTasks <- filename
		}
	}
	done := false
	for !done {
		done = c.areMapTasksDone()
	}
	c.mtx.Lock()
	c.mapTasksFinished = true
	c.mtx.Unlock()
	// log.Println("Generating Reduce Tasks")
	// Generate Reduce Tasks
	for i := range tskCopy {
		if tskCopy[i] == IDLE {
			c.reduceTasks <- i
		}
	}
	done = false
	for !done {
		done = c.areReduceTasksDone()
	}
	log.Printf("All Reduce Tasks are Done")
	c.jobsCompleted <- true
	c.mtx.Lock()
	c.reduceTasksFinished = true
	c.mtx.Unlock()
}

func (c *Coordinator) areMapTasksDone() (done bool) {
	// c.mtx.RLock()
	// defer c.mtx.RUnlock()
	done = true
	fnCopy := make(map[string]int)
	c.mtx.Lock()
	for filename, status := range c.filenames {
		fnCopy[filename] = status
	}
	c.mtx.Unlock()

	for _, status := range fnCopy {
		if status != FINISHED {
			done = false
			return
		}
		done = true
	}
	return
}

func (c *Coordinator) areReduceTasksDone() (done bool) {
	c.mtx.Lock()
	tskCopy := make([]int, len(c.reduceTaskStatus))
	copy(tskCopy, c.reduceTaskStatus)
	c.mtx.Unlock()
	for _, status := range tskCopy {
		if status != FINISHED {
			done = false
			return
		}
		done = true
	}
	return
}

func (c *Coordinator) startHeartbeatForWorkers() {
	ticker := time.NewTicker(15 * time.Second)
	defer ticker.Stop()
	for {
		<-ticker.C
		c.mtx.Lock()
		for _, worker := range c.workers {
			if worker.TaskType == WORKER_UNALLOCATED {
				continue
			}
			if !callWorker(worker.Addr, "Worker.Ping", PingRequest{}, &PingResponse{}) {
				if worker.TaskType == WORKER_MAP {
					c.filenames[worker.Task] = IDLE
					c.mapTasks <- worker.Task
					worker.TaskType = WORKER_UNALLOCATED
				} else if worker.TaskType == WORKER_REDUCE {
					taskIdx, _ := strconv.Atoi(worker.Task)
					c.reduceTaskStatus[taskIdx] = IDLE
					c.reduceTasks <- taskIdx
					worker.TaskType = WORKER_UNALLOCATED
				}
			}
		}
		c.mtx.Unlock()
	}
}

func callWorker(addr string, rpcname string, args interface{}, reply interface{}) bool {
	c, err := rpc.DialHTTP("tcp", addr)
	if err != nil {
		log.Printf("dialing error: %v", err.Error())
	} else {
		defer c.Close()

		err = c.Call(rpcname, args, reply)
		if err == nil {
			return true
		}

		fmt.Println(err)
	}
	return false
}

// create a Coordinator.
// main/mrcoordinator.go calls this function.
// nReduce is the number of reduce tasks to use.
func MakeCoordinator(files []string, nReduce int) *Coordinator {
	c := Coordinator{}
	c.workers = make(map[string]*WorkerS)
	c.filenames = make(map[string]int)
	c.mapTasks = make(chan string)
	c.reduceTasks = make(chan int)
	c.mapTasksFinished = false
	c.reduceTasksFinished = false
	c.nReduce = nReduce
	c.reduceTaskStatus = make([]int, nReduce)
	c.intermediateFiles = make([][]string, nReduce)
	c.jobsCompleted = make(chan bool)
	c.rTaskNumber = 0
	c.mTaskNumber = 0
	c.mtx = new(sync.RWMutex)
	c.mtx.Lock()
	for _, file := range files {
		c.filenames[file] = IDLE
	}

	for i := 0; i < nReduce; i++ {
		c.reduceTaskStatus[i] = IDLE
	}
	c.mtx.Unlock()
	c.server()
	return &c
}

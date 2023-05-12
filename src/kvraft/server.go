package kvraft

import (
	"fmt"
	"log"
	"sync"
	"sync/atomic"
	"time"

	"6.5840/labgob"
	"6.5840/labrpc"
	Logger "6.5840/logger"
	"6.5840/raft"
	"go.uber.org/zap"
)

const Debug = false

func DPrintf(format string, a ...interface{}) (n int, err error) {
	if Debug {
		log.Printf(format, a...)
	}
	return
}

type Op struct {
	Type      Operation
	Key       string
	Value     string
	ClientId  int64
	RequestId int64
}

type KVServer struct {
	mu      sync.Mutex
	me      int
	rf      *raft.Raft
	applyCh chan raft.ApplyMsg
	dead    int32 // set by Kill()

	maxraftstate int // snapshot if log grows this big
	db           map[string]string
	responsesCh  map[int]chan Op
	snapshotCh   chan bool
	cache        map[int64]int64
	lastApplied  atomic.Int64
	logger       *zap.SugaredLogger
}

func (kv *KVServer) lock() {
	kv.mu.Lock()
}

func (kv *KVServer) unlock() {
	kv.mu.Unlock()
}

func (kv *KVServer) String() string {
	return fmt.Sprintf("KVServer %d", kv.me)
}

func (op *Op) String() string {
	return fmt.Sprintf("Op %d %s %s", op.Type, op.Key, op.Value)
}

func (op *Op) isEqual(other Op) bool {
	return op.Type == other.Type && op.Key == other.Key && op.Value == other.Value
}

func (kv *KVServer) getLastApplied() int {
	return int(kv.lastApplied.Load())
}

func (kv *KVServer) setLastApplied(lastApplied int) {
	kv.lastApplied.Store(int64(lastApplied))
}

func (kv *KVServer) constructOperation(operation Operation, clientId int64, requestId int64, key string, value string) Op {
	return Op{
		Type:      operation,
		Key:       key,
		Value:     value,
		ClientId:  clientId,
		RequestId: requestId,
	}
}

func (kv *KVServer) Get(args *GetArgs, reply *GetReply) {
	// Your code here.
	kv.Debugf("Get called with args -{%v}-", args)
	op := kv.constructOperation(GetOp, args.ClientId, args.RequestId, args.Key, "")
	err, appliedOp := kv.sendToStateMachine(op)
	if err != OK {
		kv.Debugf("An error occurred while sending %s to state machine: %s", op.String(), err)
		reply.Err = err
		return
	}

	if op.isEqual(*appliedOp) {
		kv.lock()
		defer kv.unlock()
		val, ok := kv.db[op.Key]
		if !ok {
			reply.Err = ErrNoKey
			kv.Errorf("No Key -{%v}- found in db", op.Key)
			return
		}
		reply.Err = OK
		reply.Value = val
		kv.Debugf("Key -{%v}- found in db", op.Key)
		return
	}
	kv.Errorf("Applied op -{%v}- is not equal to sent op -{%v}-", appliedOp, op)
}

func (kv *KVServer) PutAppend(args *PutAppendArgs, reply *PutAppendReply) {
	// Your code here.
	kv.Debugf("PutAppend called with args -{%v}-", args)
	kv.lock()
	prevRequestId, ok := kv.cache[args.ClientId]
	kv.unlock()
	if ok && prevRequestId >= args.RequestId {
		reply.Err = OK
		return
	}

	op := kv.constructOperation(args.Op, args.ClientId, args.RequestId, args.Key, args.Value)
	err, appliedOp := kv.sendToStateMachine(op)
	if err != OK {
		reply.Err = err
		return
	}

	if op.isEqual(*appliedOp) {
		reply.Err = OK
		return
	} else {
		kv.Errorf("Applied op -{%v}- is not equal to sent op -{%v}-", appliedOp, op)
	}
}

// the tester calls Kill() when a KVServer instance won't
// be needed again. for your convenience, we supply
// code to set rf.dead (without needing a lock),
// and a killed() method to test rf.dead in
// long-running loops. you can also add your own
// code to Kill(). you're not required to do anything
// about this, but it may be convenient (for example)
// to suppress debug output from a Kill()ed instance.
func (kv *KVServer) Kill() {
	atomic.StoreInt32(&kv.dead, 1)
	kv.rf.Kill()
	// Your code here, if desired.
}

func (kv *KVServer) killed() bool {
	z := atomic.LoadInt32(&kv.dead)
	return z == 1
}

func (kv *KVServer) applier() {
	for !kv.killed() {
		kv.Debugf("Waiting for application")
		msg := <-kv.applyCh
		kv.Debugf("Received -{%v}- on applyCh", msg)

		if msg.CommandValid {
			op := msg.Command.(Op)
			kv.lock()
			lastRequestId, ok := kv.cache[op.ClientId]
			if !ok || lastRequestId < op.RequestId {
				if msg.CommandIndex > kv.getLastApplied() {
					kv.setLastApplied(msg.CommandIndex)
				}
				kv.Debugf("It is a new request!")
				kv.cache[op.ClientId] = op.RequestId
				switch op.Type {
				case PutOp:
					kv.db[op.Key] = op.Value
				case AppendOp:
					kv.db[op.Key] += op.Value
				case GetOp:
					op.Value = kv.db[op.Key]
				}
			} else {
				kv.Debugf("Ignoring old request ... lastRequestId -{%v}- requestId -{%v}- ok -{%v}-", lastRequestId, op.RequestId, ok)
			}
			ch, hasChannel := kv.responsesCh[msg.CommandIndex]
			kv.unlock()
			if hasChannel {
				kv.Debugf("Sending response to channel -{%v}-", op)
				select {
				case ch <- op:
				default:
				}
			} else {
				kv.Debugf("No Channel found for -{%v}-", msg.CommandIndex)
			}
		} else if msg.SnapshotValid {

		} else {
			continue
		}
	}
	kv.Debugf("Applier exiting")
}

// servers[] contains the ports of the set of
// servers that will cooperate via Raft to
// form the fault-tolerant key/value service.
// me is the index of the current server in servers[].
// the k/v server should store snapshots through the underlying Raft
// implementation, which should call persister.SaveStateAndSnapshot() to
// atomically save the Raft state along with the snapshot.
// the k/v server should snapshot when Raft's saved state exceeds maxraftstate bytes,
// in order to allow Raft to garbage-collect its log. if maxraftstate is -1,
// you don't need to snapshot.
// StartKVServer() must return quickly, so it should start goroutines
// for any long-running work.
func StartKVServer(servers []*labrpc.ClientEnd, me int, persister *raft.Persister, maxraftstate int) *KVServer {
	// call labgob.Register on structures you want
	// Go's RPC library to marshall/unmarshall.
	labgob.Register(Op{})

	kv := new(KVServer)
	kv.me = me
	kv.maxraftstate = maxraftstate
	kv.logger = Logger.NewLogger(fmt.Sprintf("kvserver-%d.log", me))
	// Logger.SetDebugOff()
	// You may need initialization code here.

	kv.applyCh = make(chan raft.ApplyMsg)
	kv.rf = raft.Make(servers, me, persister, kv.applyCh)

	// You may need initialization code here.
	kv.db = make(map[string]string)
	kv.responsesCh = make(map[int]chan Op)
	kv.cache = make(map[int64]int64)
	kv.lastApplied.Store(-1)
	go kv.applier()
	return kv
}

func (kv *KVServer) sendToStateMachine(op Op) (Err, *Op) {
	kv.Debugf("Sending %s to state machine", op.String())
	index, _, isLeader := kv.rf.Start(op)
	kv.Debugf("Sent %s to state machine", op.String())
	if !isLeader {
		kv.Debugf("|-> Not leader")
		return ErrWrongLeader, nil
	}
	kv.lock()
	kv.Debugf("Creating channel for %d", index)
	ch, ok := kv.responsesCh[index]
	if !ok {
		ch = make(chan Op, 1)
		kv.responsesCh[index] = ch
	}
	kv.unlock()
	kv.Debugf("Waiting for %v application - rid %v", index, op.RequestId)
	select {
	case op := <-ch:
		kv.Debugf("Applied %v - rid %v", index, op.RequestId)
		return OK, &op
	case <-time.After(1 * time.Second):
		kv.Debugf("Timeout %v - rid %v", index, op.RequestId)
		return ErrTimeout, nil
	}
}

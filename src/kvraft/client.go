package kvraft

import (
	"crypto/rand"
	"math/big"

	"6.5840/labrpc"
)

type Clerk struct {
	servers []*labrpc.ClientEnd
	// You will have to modify this struct.
	id        int64
	requestId int64
	leaderId  int64
}

func nrand() int64 {
	max := big.NewInt(int64(1) << 62)
	bigx, _ := rand.Int(rand.Reader, max)
	x := bigx.Int64()
	return x
}

func MakeClerk(servers []*labrpc.ClientEnd) *Clerk {
	ck := new(Clerk)
	ck.servers = servers
	ck.id = nrand()
	ck.leaderId = 0
	ck.requestId = 0
	return ck
}

// fetch the current value for a key.
// returns "" if the key does not exist.
// keeps trying forever in the face of all other errors.
//
// you can send an RPC with code like this:
// ok := ck.servers[i].Call("KVServer.Get", &args, &reply)
//
// the types of args and reply (including whether they are pointers)
// must match the declared types of the RPC handler function's
// arguments. and reply must be passed as a pointer.
func (ck *Clerk) Get(key string) string {
	ck.requestId++
	leader := ck.leaderId
	request := GetArgs{
		Key:       key,
		ClientId:  ck.id,
		RequestId: ck.requestId,
	}
	ret := ""
	for ; ; leader = (leader + 1) % int64(len(ck.servers)) {
		reply := GetReply{}
		ok := ck.servers[leader].Call("KVServer.Get", &request, &reply)
		if ok {
			if reply.Err == OK {
				ret = reply.Value
				ck.leaderId = leader
				break
			}

			if reply.Err == ErrNoKey {
				break
			}
		}
	}

	if ck.leaderId != leader {
		ck.leaderId = leader
	}
	return ret
}

// shared by Put and Append.
//
// you can send an RPC with code like this:
// ok := ck.servers[i].Call("KVServer.PutAppend", &args, &reply)
//
// the types of args and reply (including whether they are pointers)
// must match the declared types of the RPC handler function's
// arguments. and reply must be passed as a pointer.
func (ck *Clerk) PutAppend(key string, value string, op Operation) {
	ck.requestId++
	leader := ck.leaderId
	request := PutAppendArgs{
		Key:       key,
		Value:     value,
		Op:        op,
		ClientId:  ck.id,
		RequestId: ck.requestId,
	}
	for ; ; leader = (leader + 1) % int64(len(ck.servers)) {
		reply := PutAppendReply{}
		ok := ck.servers[leader].Call("KVServer.PutAppend", &request, &reply)
		if ok && reply.Err == OK {
			break
		}
	}

	if ck.leaderId != leader {
		ck.leaderId = leader
	}
}

func (ck *Clerk) Put(key string, value string) {
	ck.PutAppend(key, value, PutOp)
}
func (ck *Clerk) Append(key string, value string) {
	ck.PutAppend(key, value, AppendOp)
}

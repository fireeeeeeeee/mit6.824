package kvraft

import (
	"crypto/rand"
	"fmt"
	"math/big"
	"strconv"

	"../labrpc"
)

type Clerk struct {
	servers []*labrpc.ClientEnd
	// You will have to modify this struct.
	clerkID int
	rpcID   int
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
	ck.rpcID = 0
	ck.clerkID = 0
	// You'll have to add code here.
	return ck
}

func (ck *Clerk) getid() {
	if ck.clerkID != 0 {
		return
	}
	ck.clerkID = -1
	id, err := strconv.Atoi(ck.Get("ClerkID"))
	for err != nil {
		fmt.Println(err)
		id, err = strconv.Atoi(ck.Get("ClerkID"))
	}
	ck.clerkID = id
}

//
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
//
func (ck *Clerk) Get(key string) string {
	ck.getid()
	ck.rpcID++
	args := GetArgs{Key: key, ClerkID: ck.clerkID, RpcID: ck.rpcID}
	reply := GetReply{}
	l := len(ck.servers)
	for i := 0; ; i++ {
		ok := ck.servers[i%l].Call("KVServer.Get", &args, &reply)
		if ok {
			if reply.Err == OK {
				return reply.Value
			} else if reply.Err == ErrNoKey {
				return ""
			}

		}
	}

	return ""
}

//
// shared by Put and Append.
//
// you can send an RPC with code like this:
// ok := ck.servers[i].Call("KVServer.PutAppend", &args, &reply)
//
// the types of args and reply (including whether they are pointers)
// must match the declared types of the RPC handler function's
// arguments. and reply must be passed as a pointer.
//
func (ck *Clerk) PutAppend(key string, value string, op string) {
	// You will have to modify this function.
	ck.getid()
	ck.rpcID++
	args := PutAppendArgs{Key: key, Value: value, Op: op, ClerkID: ck.clerkID, RpcID: ck.rpcID}
	reply := PutAppendReply{}
	l := len(ck.servers)
	for i := 0; ; i++ {
		ok := ck.servers[i%l].Call("KVServer.PutAppend", &args, &reply)
		if ok {
			if reply.Err == OK {
				return
			}
		}
	}
}

func (ck *Clerk) Put(key string, value string) {
	ck.PutAppend(key, value, "Put")
}
func (ck *Clerk) Append(key string, value string) {
	ck.PutAppend(key, value, "Append")
}

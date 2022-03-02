package kvraft

import (
	"fmt"
	"log"
	"strconv"
	"sync"
	"sync/atomic"
	"time"

	"../labgob"
	"../labrpc"
	"../raft"
)

const Debug = 0

func DPrintf(format string, a ...interface{}) (n int, err error) {
	if Debug > 0 {
		log.Printf(format, a...)
	}
	return
}

type Op struct {
	// Your definitions here.
	// Field names must start with capital letters,
	// otherwise RPC will break.
	Key   string
	Value string
	Op    string

	Me      int
	RpcID   int
	ClerkID int
}

type rpcResult struct {
	Value   string
	Me      int
	RpcID   int
	ClerkID int
}

type myMap struct {
	mu     sync.Mutex
	target map[int]rpcResult
	cnt    map[int]int
}

func (myMap *myMap) new() {
	myMap.cnt = make(map[int]int)
	myMap.target = make(map[int]rpcResult)
}

func (myMap *myMap) add(key int) {
	myMap.mu.Lock()
	myMap.cnt[key]++
	myMap.mu.Unlock()
}
func (myMap *myMap) remove(key int) {
	myMap.mu.Lock()
	myMap.cnt[key]--
	if myMap.cnt[key] == 0 {
		delete(myMap.cnt, key)
		delete(myMap.target, key)
	}
	myMap.mu.Unlock()
}
func (myMap *myMap) wait(key int) rpcResult {
	myMap.mu.Lock()
	for {
		_, ok := myMap.target[key]
		if ok {
			break
		}
		myMap.mu.Unlock()
		time.Sleep(10 * time.Millisecond)
		myMap.mu.Lock()
	}
	myMap.mu.Unlock()
	return myMap.target[key]
}
func (myMap *myMap) addTarget(key int, value rpcResult) {
	myMap.mu.Lock()
	defer myMap.mu.Unlock()
	if myMap.cnt[key] == 0 {
		return
	}
	myMap.target[key] = value

}

type KVServer struct {
	mu           sync.Mutex
	me           int
	rf           *raft.Raft
	applyCh      chan raft.ApplyMsg
	dead         int32 // set by Kill()
	maxraftstate int   // snapshot if log grows this big
	myMap        myMap

	state    map[string]string
	ckAnswer map[int]rpcResult
	ckCount  int
	// Your definitions here.
}

func (kv *KVServer) Get(args *GetArgs, reply *GetReply) {
	var result rpcResult
	result, has := kv.ckAnswer[args.ClerkID]
	fmt.Println(kv.me, args.ClerkID, args.RpcID, args.Key)
	if has && result.RpcID == args.RpcID {
		//result in cache
		if result.Value == "" {
			reply.Err = ErrNoKey
			reply.Value = ""
		} else {
			reply.Err = OK
			reply.Value = result.Value
		}
	} else {

		op := Op{Key: args.Key, Op: "Get", Me: kv.me, RpcID: args.RpcID, ClerkID: args.ClerkID}
		index, _, isLeader := kv.rf.Start(op)
		if !isLeader {
			reply.Err = ErrWrongLeader
			return
		}
		kv.myMap.add(index)
		result := kv.myMap.wait(index)
		if result.RpcID != op.RpcID || result.ClerkID != op.ClerkID || result.Me != op.Me {
			reply.Err = ErrWrongLeader
		} else {
			if result.Value == "" {
				reply.Err = ErrNoKey
				reply.Value = ""
			} else {
				reply.Err = OK
				reply.Value = result.Value
			}
		}
		kv.myMap.remove(index)
	}
}

func (kv *KVServer) PutAppend(args *PutAppendArgs, reply *PutAppendReply) {
	// Your code here.
	var result rpcResult
	result, has := kv.ckAnswer[args.ClerkID]
	if has && result.RpcID == args.RpcID {
		reply.Err = OK
	} else {
		op := Op{Key: args.Key, Value: args.Value, Op: args.Op, Me: kv.me, RpcID: args.RpcID, ClerkID: args.ClerkID}
		index, _, isLeader := kv.rf.Start(op)
		if !isLeader {
			reply.Err = ErrWrongLeader
			return
		}

		kv.myMap.add(index)
		result = kv.myMap.wait(index)
		if result.RpcID != op.RpcID || result.ClerkID != op.ClerkID || result.Me != op.Me {
			reply.Err = ErrWrongLeader
		} else {
			reply.Err = OK
		}
		kv.myMap.remove(index)
	}
}

//
// the tester calls Kill() when a KVServer instance won't
// be needed again. for your convenience, we supply
// code to set rf.dead (without needing a lock),
// and a killed() method to test rf.dead in
// long-running loops. you can also add your own
// code to Kill(). you're not required to do anything
// about this, but it may be convenient (for example)
// to suppress debug output from a Kill()ed instance.
//
func (kv *KVServer) Kill() {
	atomic.StoreInt32(&kv.dead, 1)
	kv.rf.Kill()
	// Your code here, if desired.
}

func (kv *KVServer) killed() bool {
	z := atomic.LoadInt32(&kv.dead)
	return z == 1
}

func (kv *KVServer) step(op Op) string {
	if op.Op == "Append" {
		kv.state[op.Key] += op.Value
	} else if op.Op == "Put" {
		kv.state[op.Key] = op.Value
	} else if op.Op == "Get" {
		if op.Key == "ClerkID" {
			kv.ckCount++
			kv.state[op.Key] = strconv.Itoa(kv.ckCount)
		}
	} else {
		DPrintf("Error op")
	}
	return kv.state[op.Key]
}

func (kv *KVServer) read() {
	for msg := range kv.applyCh {
		kv.mu.Lock()
		op := msg.Command.(Op)
		value := kv.step(op)
		index := msg.CommandIndex
		rpcResult := rpcResult{Value: value, RpcID: op.RpcID, ClerkID: op.ClerkID, Me: op.Me}
		if op.ClerkID != -1 {
			kv.ckAnswer[op.ClerkID] = rpcResult
		}
		kv.myMap.addTarget(index, rpcResult)
		kv.mu.Unlock()
	}
}

//
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
//
func StartKVServer(servers []*labrpc.ClientEnd, me int, persister *raft.Persister, maxraftstate int) *KVServer {
	// call labgob.Register on structures you want
	// Go's RPC library to marshall/unmarshall.
	labgob.Register(Op{})

	kv := new(KVServer)
	kv.me = me
	kv.maxraftstate = maxraftstate

	// You may need initialization code here.

	kv.applyCh = make(chan raft.ApplyMsg)
	kv.rf = raft.Make(servers, me, persister, kv.applyCh)
	kv.state = make(map[string]string)
	kv.ckAnswer = make(map[int]rpcResult)
	kv.ckCount = 0
	kv.myMap.new()
	go kv.read()
	// You may need initialization code here.

	return kv
}

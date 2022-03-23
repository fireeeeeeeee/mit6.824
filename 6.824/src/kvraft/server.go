package kvraft

import (
	"log"
	"math/rand"
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

// type myMap struct {
// 	mu       sync.Mutex
// 	target   map[int]rpcResult
// 	cnt      map[int]int
// 	commited int
// }

// func (myMap *myMap) new() {
// 	myMap.cnt = make(map[int]int)
// 	myMap.target = make(map[int]rpcResult)
// }

// func (myMap *myMap) add(key int) bool {
// 	myMap.mu.Lock()
// 	defer myMap.mu.Unlock()
// 	_, ok := myMap.target[key]
// 	if key <= myMap.commited && !ok {
// 		return false
// 	}
// 	myMap.cnt[key]++
// 	return true

// }
// func (myMap *myMap) remove(key int) {
// 	myMap.mu.Lock()
// 	myMap.cnt[key]--
// 	if myMap.cnt[key] == 0 {
// 		delete(myMap.cnt, key)
// 		delete(myMap.target, key)
// 	}
// 	myMap.mu.Unlock()
// }
// func (myMap *myMap) wait(key int) rpcResult {
// 	myMap.mu.Lock()
// 	var rec rpcResult
// 	var ok bool
// 	for {
// 		rec, ok = myMap.target[key]
// 		if ok {
// 			break
// 		}
// 		myMap.mu.Unlock()
// 		time.Sleep(10 * time.Millisecond)
// 		myMap.mu.Lock()
// 	}
// 	myMap.mu.Unlock()
// 	return rec
// }
// func (myMap *myMap) addTarget(key int, value rpcResult) {
// 	myMap.mu.Lock()
// 	defer myMap.mu.Unlock()
// 	myMap.target[key] = value
// 	myMap.commited = key
// 	//fmt.Println("1", key)
// }

type KVServer struct {
	mu           sync.Mutex
	me           int
	rf           *raft.Raft
	applyCh      chan raft.ApplyMsg
	dead         int32 // set by Kill()
	maxraftstate int   // snapshot if log grows this big

	state      map[string]string
	ckAnswer   map[int]rpcResult // 从raft中获得到的某个client的最后一条信息
	ckCount    int               //client 数量
	lastCommit map[int]int       //用于优化rpc突然断开的情况
	commited   int

	// Your definitions here.
}

type myReply struct {
	Err   Err
	Value string
}

func (kv *KVServer) handleOPs(op Op) myReply {
	kv.mu.Lock()
	defer kv.mu.Unlock()
	reply := myReply{}
	handleResult := func(result *rpcResult) {
		if result.Value == "" {
			reply.Err = ErrNoKey
			reply.Value = ""
		} else {
			reply.Err = OK
			reply.Value = result.Value
		}
	}
	var result rpcResult
	result, has := kv.ckAnswer[op.ClerkID]
	if has && result.RpcID > op.RpcID {
		//this rpc must be failed , it's fine to return anything
		handleResult(&result)
	} else if has && result.RpcID == op.RpcID {
		//result in cache
		handleResult(&result)
	} else {
		kv.mu.Unlock()
		index, _, isLeader := kv.rf.Start(op)
		kv.mu.Lock()
		if !isLeader {
			reply.Err = ErrWrongLeader
		} else {
			ok := false
			for {
				if kv.commited > index {
					break
				}
				kv.mu.Unlock()
				time.Sleep(10 * time.Millisecond)
				kv.mu.Lock()
			}
			result, ok = kv.ckAnswer[op.ClerkID]
			if !ok || kv.ckAnswer[op.ClerkID].RpcID != op.RpcID {
				reply.Err = ErrWrongLeader
			} else {
				handleResult(&result)
			}
		}
	}
	return reply
}

func (kv *KVServer) Get(args *GetArgs, reply *GetReply) {
	if args.Key == "ClerkID" {

		kv.mu.Lock()
		defer kv.mu.Unlock()
		kv.ckCount++
		reply.Err = OK
		reply.Value = strconv.Itoa(rand.Intn(100000000))
		return
	}

	op := Op{Key: args.Key, Op: "Get", Me: kv.me, RpcID: args.RpcID, ClerkID: args.ClerkID}
	myReply := kv.handleOPs(op)
	reply.Err = myReply.Err
	reply.Value = myReply.Value
}

func (kv *KVServer) PutAppend(args *PutAppendArgs, reply *PutAppendReply) {
	// Your code here.
	op := Op{Key: args.Key, Value: args.Value, Op: args.Op, Me: kv.me, RpcID: args.RpcID, ClerkID: args.ClerkID}
	myReply := kv.handleOPs(op)
	reply.Err = myReply.Err
	if reply.Err == ErrNoKey {
		reply.Err = OK
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

func (kv *KVServer) step(op Op) {
	if op.Op == "Emp" {
		return
	}

	preRpcresult, ok := kv.ckAnswer[op.ClerkID]
	if ok && preRpcresult.RpcID == op.RpcID {
		return
	}
	if op.Op == "Append" {
		kv.state[op.Key] += op.Value
	} else if op.Op == "Put" {
		kv.state[op.Key] = op.Value
	} else if op.Op == "Get" {

	} else {
		DPrintf("Error op")
	}
	rpcResult := rpcResult{Value: kv.state[op.Key], RpcID: op.RpcID, ClerkID: op.ClerkID, Me: op.Me}
	kv.ckAnswer[op.ClerkID] = rpcResult
}

func (kv *KVServer) read() {
	for msg := range kv.applyCh {
		kv.mu.Lock()
		op := msg.Command.(Op)
		kv.step(op)
		index := msg.CommandIndex
		kv.commited = index + 1
		kv.mu.Unlock()
	}
}

func (kv *KVServer) heartBeat() {
	for {

		op := Op{Op: "Emp", RpcID: -1, ClerkID: -2}
		kv.rf.Start(op)

		time.Sleep(time.Millisecond * 100)
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
	kv.lastCommit = make(map[int]int)
	kv.commited = 0
	go kv.read()
	go kv.heartBeat()
	// You may need initialization code here.

	return kv
}

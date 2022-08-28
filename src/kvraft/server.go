package kvraft

import (
	"6.824/labgob"
	"6.824/labrpc"
	"6.824/raft"
	"log"
	"sync"
	"sync/atomic"

	"time"
)

const Debug = true

func DPrintf(format string, a ...interface{}) (n int, err error) {
	if Debug {
		log.Printf(format, a...)
	}
	return
}

type StateMachine struct {
	lastApplied	int
	memory		map[string]string
}

const ExecuteTimeout = 500 * time.Millisecond

func (kv *KVServer) applier() {
	for {
		msg := <- kv.applyCh

		if msg.CommandValid {
			// ???
			command := msg.Command.(Op)
			
			switch command.Type {
			case get_str:
				kv.mu.Lock()
				value, ok := kv.SM.memory[command.Key]
				tmpCh := kv.notifyChs[kv.index2clientId[msg.CommandIndex]]
				kv.mu.Unlock()
				if ok {
					tmpCh <- &Res {
						Err: 	OK,
						Value: 	value,
					}
				} else {
					tmpCh <- &Res {
						Err: 	ErrNoKey,
						Value: 	"",
					}
				}
			case put_str:
				kv.mu.Lock()
				kv.SM.memory[command.Key] = command.Value
				tmpCh := kv.notifyChs[kv.index2clientId[msg.CommandIndex]]
				kv.mu.Unlock()
				tmpCh <- &Res {
					Err: 	OK,
				}
			case append_str:
				_, ok := kv.SM.memory[command.Key]
				if !ok {
					kv.SM.memory[command.Key] = command.Value	
				} else {
					kv.SM.memory[command.Key] += command.Value
				}
				kv.mu.Lock()
				tmpCh := kv.notifyChs[kv.index2clientId[msg.CommandIndex]] 
				kv.mu.Unlock()
				tmpCh <- &Res {
					Err: 	OK,
				}
			}
		} else if msg.SnapshotValid {

		}
	}
}

type Op struct {
	// Your definitions here.
	// Field names must start with capital letters,
	// otherwise RPC will break.
	Type	string
	Key		string
	Value	string
}
type Res struct {
	// 保存 state machine 执行结果
	Err   Err
	Value string
}

// 
// Raft 的一个优化想法: Start 加入 entry 的时候直接就调用 AppendEntry, 不要等下一次超时
type KVServer struct {
	mu      sync.Mutex
	me      int
	rf      *raft.Raft
	applyCh chan raft.ApplyMsg
	dead    int32 // set by Kill()

	maxraftstate int // snapshot if log grows this big

	// Your definitions here.
	SM			StateMachine
	notifyChs 	map[int64]chan *Res
	index2clientId	map[int]int64

	// 用于记录已经计算完成的 command 的结果
	// 该记录主要是为了防止重复 apply, 故只用于 Put/Append 等只写命令, Get 是只读命令不用记录
	// resultRes[cilentId][commandId] = <Err>
	resultRec	map[int64]map[int] Err
}

func (kv *KVServer) initNotiCh(clientId int64) {
	if _, ok := kv.notifyChs[clientId]; !ok {
		kv.notifyChs[clientId] = make(chan *Res)
	}
}
func (kv *KVServer) Get(args *GetArgs, reply *GetReply) {
	DPrintf("server(#%v).Get(%v)\n", kv.me, args)
	command := Op {
		Type:	get_str,
		Key:	args.Key,
	}

	index, _, isLeader := kv.rf.Start(command)
	if !isLeader {
		reply.Err = ErrWrongLeader
		return
	}
	kv.mu.Lock()
	kv.initNotiCh(args.ClientId)
	kv.index2clientId[index] = args.ClientId
	kv.mu.Unlock()

	select {
	case res := <- kv.notifyChs[args.ClientId]:
		reply.Err = res.Err
		reply.Value = res.Value
	case <- time.After(ExecuteTimeout):
		reply.Err = ErrTimeOut
		reply.Value = ""
	}
}

func (kv *KVServer) haveReplied(clientId int64, commandId int) (bool, Err) {
	kv.mu.Lock()
	ret, ok := kv.resultRec[clientId][commandId]
	kv.mu.Unlock()
	if !ok {
		return false, ""
	}
	return true, ret
}
func (kv *KVServer) PutAppend(args *PutAppendArgs, reply *PutAppendReply) {
	DPrintf("server(#%v).PutAppend(%v)\n", kv.me, args)
	if ok, res := kv.haveReplied(args.ClientId, args.CommandId); ok {
		reply.Err = res
		return
	}

	if _, ok := kv.resultRec[args.ClientId]; !ok {
		kv.resultRec[args.ClientId] = make(map[int] Err)
	}

	command := Op {
		Type:	args.Op,
		Key:	args.Key,
		Value:	args.Value,
	}
	index, _, isLeader := kv.rf.Start(command)
	if !isLeader {
		kv.resultRec[args.ClientId][args.CommandId] = ErrWrongLeader
		reply.Err = ErrWrongLeader
	
		return
	}
	kv.mu.Lock()
	kv.initNotiCh(args.ClientId)
	kv.index2clientId[index] = args.ClientId
	kv.mu.Unlock()

	select {
	case res := <- kv.notifyChs[args.ClientId]:
		reply.Err = res.Err
	case <- time.After(ExecuteTimeout):
		reply.Err = ErrTimeOut
	}
	go func() {
		kv.mu.Lock()
		kv.resultRec[args.ClientId][args.CommandId] = reply.Err
		kv.mu.Unlock()
	}()
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

	kv.applyCh = make(chan raft.ApplyMsg)
	kv.rf = raft.Make(servers, me, persister, kv.applyCh)

	// You may need initialization code here.
	kv.SM.memory = make(map[string]string)
	kv.notifyChs = make(map[int64]chan *Res)
	kv.resultRec = make(map[int64]map[int] Err)
	kv.index2clientId =	make(map[int]int64)
	DPrintf("StartKVServer\n")

	go kv.applier()

	return kv
}

package kvraft

import (
	"6.824/labgob"
	"6.824/labrpc"
	"6.824/raft"
	"sync"
	"sync/atomic"

	"time"
)



type Op struct {
	// Your definitions here.
	// Field names must start with capital letters,
	// otherwise RPC will break.
	Type	string
	Key		string
	Value	string
	ClientId 		int64
	SequenceNum		int
}

type Res struct {
	// 保存结果
	Err   Err
	Value string
}



type StateMachine struct {
	memory			map[string]string
}

func(vm *StateMachine) get(key string) (string, Err) {
	value, ok := vm.memory[key]
	if ok {
		return value, OK
	} else {
		return "", ErrNoKey
	}
}

func(vm *StateMachine) put(key string, value string) Err {
	vm.memory[key] = value
	return OK
}

func(vm *StateMachine) append(key string, value string) Err {
	vm.memory[key] += value
	return OK
}

func newStateMachine() *StateMachine {
	return &StateMachine{make(map[string]string)}
}



type KVServer struct {
	mu      sync.RWMutex
	me      int
	rf      *raft.Raft
	applyCh chan raft.ApplyMsg
	dead    int32 // set by Kill()

	maxraftstate int // snapshot if log grows this big

	// Your definitions here.
	SM			StateMachine
	notifyChs 	map[int]chan *Res

	// 用于记录已完成的写命令 (Put/Append) 的结果, 为了防止重复 apply.
	lastWriteRes	map[int64]Err
	lastWriteSN		map[int64]int
}

func (kv *KVServer) getNotifyCh(index int) chan *Res {
	ret, ok := kv.notifyChs[index]
	if !ok {
		kv.notifyChs[index] = make(chan *Res)
		ret = kv.notifyChs[index]
	}

	return ret
}

func (kv *KVServer) removeOldChan(index int) {
	for k, ch := range kv.notifyChs {
		if k < index {
			Debug(dWarn, "server#%d remove(%d)\n", kv.me, k)
			select {
				case <-ch: // try to drain the channel
				default:
			}
			delete(kv.notifyChs, k)
		} else if k > index {
			panic("removeOldChan(): index too small? something error\n")
		} else if k == index {
			// ?
		}
	}
}

func (kv *KVServer) applier() {
	for {
		msg := <- kv.applyCh
		
		if msg.CommandValid {
			command := msg.Command.(Op)
			res := &Res{
				Err:	OK,
			}

			// Type	string
			// Key		string
			// Value	string
			// ClientId 		int64
			// SequenceNum		int
			Debug(dInfo, "server#%d: applier get msg(CommandValid:%v, CommandIndex:%d, CommandTerm:%d Command:{Type:%s, Key:%s, Val:%s, CId:%d, SN:%d})\n", 
			kv.me, msg.CommandValid, msg.CommandIndex, msg.CommandTerm, command.Type, command.Key, command.Value, command.ClientId, command.SequenceNum)

			if command.Type == get_str {	// Get
				kv.mu.RLock()
				value, ok := kv.SM.memory[command.Key]
				kv.mu.RUnlock()

				if !ok {
					res.Err = ErrNoKey
				} else {
					res.Value = value
				}
			} else {
				kv.mu.RLock()
				err, ok := kv.haveApplied(command.ClientId, command.SequenceNum)
				kv.mu.RUnlock()

				if ok {
					res.Err = err
				} else if command.Type == put_str {
					kv.mu.Lock()
					kv.SM.put(command.Key, command.Value)
					kv.updateLastSN(command.ClientId, command.SequenceNum, OK)
					kv.mu.Unlock()
				} else if command.Type == append_str {
					kv.mu.Lock()
					kv.SM.append(command.Key, command.Value)
					kv.updateLastSN(command.ClientId, command.SequenceNum, OK)
					kv.mu.Unlock()
				}
			}
			
			currentTerm, isLeader := kv.rf.GetState()
			Debug(dInfo, "server#%d: currentTerm = %d\n", kv.me, currentTerm)
			if isLeader && msg.CommandTerm == currentTerm {
				kv.mu.Lock()
				ch := kv.getNotifyCh(msg.CommandIndex)
				kv.mu.Unlock()
				
				Debug(dWarn, "server#%d: ch block?\n", kv.me)
				select {
					case ch <- res:
					case <- time.After(ExecuteTimeout):
				}
				
				Debug(dWarn, "server#%d: no.\n", kv.me)
			}
		} else if msg.SnapshotValid {

		}
	}
}

func (kv *KVServer) Get(args *GetArgs, reply *GetReply) {
	command := Op {
		Type:	get_str,
		Key:	args.Key,
	}

	index, _, isLeader := kv.rf.Start(command)
	if !isLeader {
		reply.Status = ErrWrongLeader
		return
	}
	kv.mu.Lock()
	// kv.removeOldChan(index)
	ch := kv.getNotifyCh(index)
	kv.mu.Unlock()

	select {
		case res := <- ch:
			reply.Status, reply.Value = res.Err, res.Value
		case <- time.After(ExecuteTimeout):
			reply.Status, reply.Value = ErrTimeOut, ""
	}

	// if kv.persister.RaftStateSize() > kv.maxraftstate {

	// }
}



func (kv *KVServer) haveApplied(clientId int64, sequenceNum int) (Err, bool) {
	SN, ok := kv.lastWriteSN[clientId]
	if ok && SN >= sequenceNum {
		return kv.lastWriteRes[clientId], true
	}
	
	return "", false 
}

func (kv *KVServer) updateLastSN(clientId int64, sequenceNum int, response Err) {
	SN, ok := kv.lastWriteSN[clientId]
	if !ok || SN < sequenceNum {
		kv.lastWriteSN[clientId] = sequenceNum
		kv.lastWriteRes[clientId] = response
	}
	
}

func (kv *KVServer) PutAppend(args *PutAppendArgs, reply *PutAppendReply) {
	Debug(dLog, "server#%d, %s(key:%s,val:%s,CID:%d,SN:%d)\n", 
		kv.me, args.Op, args.Key, args.Value, args.ClientId, args.SequenceNum)

	kv.mu.RLock()
	res, ok := kv.haveApplied(args.ClientId, args.SequenceNum)
	kv.mu.RUnlock()
	if ok {
		reply.Status = res
		return
	}

	command := Op {
		Type:	args.Op,
		Key:	args.Key,
		Value:	args.Value,
		ClientId:	args.ClientId,
		SequenceNum:args.SequenceNum,
	}

	index, _, isLeader := kv.rf.Start(command)
	if !isLeader {
		reply.Status = ErrWrongLeader
		
		return
	}
	Debug(dCommit, "server#%d Start(%v), index:%d\n", kv.me, command, index)

	kv.mu.Lock()
	// kv.removeOldChan(index)
	ch := kv.getNotifyCh(index)
	kv.mu.Unlock()

	select {
	case res := <- ch:
		reply.Status = res.Err
		// go func() {
		// 	kv.mu.Lock()
		// 	kv.updateLastSN(args.ClientId, args.SequenceNum, reply.Status)
		// 	kv.mu.Unlock()
		// }()
	case <- time.After(ExecuteTimeout):
		reply.Status = ErrTimeOut
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
	DebugInit()

	labgob.Register(Op{})

	kv := new(KVServer)
	kv.me = me
	kv.maxraftstate = maxraftstate

	kv.applyCh = make(chan raft.ApplyMsg)
	kv.rf = raft.Make(servers, me, persister, kv.applyCh)

	// You may need initialization code here.
	kv.SM = *newStateMachine()
	kv.notifyChs = make(map[int]chan *Res)

	// 从 1 开始
	kv.lastWriteSN = make(map[int64]int)
	kv.lastWriteRes = make(map[int64]Err)
	Debug(dError, "S#%d: Restart.\n", kv.me)

	go kv.applier()

	return kv
}

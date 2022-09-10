package kvraft

import "6.824/labrpc"
import "crypto/rand"
import "math/big"

import "time"


type Clerk struct {
	servers []*labrpc.ClientEnd
	// You will have to modify this struct.
	id			int64
	sequenceNum	int
	leaderId	int
}

// 生成随机数
func nrand() int64 {
	max := big.NewInt(int64(1) << 62)
	bigx, _ := rand.Int(rand.Reader, max)
	x := bigx.Int64()
	return x
}


func MakeClerk(servers []*labrpc.ClientEnd) *Clerk {
	ck := new(Clerk)
	ck.servers = servers
	// You'll have to add code here.
	
	ck.id = nrand()
	ck.sequenceNum = 0
	ck.leaderId = 0
	// Debug(dClient, "MakeClient: id = %v\n", ck.id)

	return ck
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
	// 一个 command 对应一个 sequenceNum, 故后面的重试全部使用这一个 SequenceNum
	ck.sequenceNum++

	args := GetArgs{
		Key: 		key,
	}
	var reply GetReply

	for {
		if ck.leaderId == 0 {
			time.Sleep(50 * time.Millisecond)
		}
		
		reply = GetReply{}
		if !ck.servers[ck.leaderId].Call("KVServer.Get", &args, &reply) {
			ck.leaderId = (ck.leaderId + 1) % len(ck.servers)
			continue
		}

		if reply.Status == OK {
			return reply.Value
		} else if reply.Status == ErrNoKey {
			return ""
		} else if reply.Status == ErrWrongLeader {
			ck.leaderId = (ck.leaderId + 1) % len(ck.servers)
		}
	}
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
	// 一个 command 对应一个 sequenceNum, 故后面的重试全部使用这一个 SequenceNum
	ck.sequenceNum++
	
	// Debug(dClient, "client#%v %s(key: %s, value: %s, SN: %d)\n", ck.id, op, key, value, ck.sequenceNum)
	args := PutAppendArgs {
		Key: 		key,
		Value:		value,
		Op:			op,

		ClientId: 	ck.id,
		SequenceNum:	ck.sequenceNum,
	}
	var reply PutAppendReply

	for {
		if ck.leaderId == 0 {
			time.Sleep(50 * time.Millisecond)
		}

		reply = PutAppendReply{}
		
		if !ck.servers[ck.leaderId].Call("KVServer.PutAppend", &args, &reply) {
			ck.leaderId = (ck.leaderId + 1) % len(ck.servers)
			continue
		}
		// Debug(dClient, "client#%v, SN:%d, leaderId:%d, reply:%v\n", ck.id, ck.sequenceNum, ck.leaderId, reply)
		if reply.Status == OK {
			break
		} else if reply.Status == ErrWrongLeader || reply.Status == ErrTimeOut {
			ck.leaderId = (ck.leaderId + 1) % len(ck.servers)
		}
	}
}

func (ck *Clerk) Put(key string, value string) {
	ck.PutAppend(key, value, put_str)
}
func (ck *Clerk) Append(key string, value string) {
	ck.PutAppend(key, value, append_str)
}

package kvraft

import "6.824/labrpc"
import "crypto/rand"
import "math/big"
import "time"


type Clerk struct {
	servers []*labrpc.ClientEnd
	// You will have to modify this struct.
	id			int64
	comStamp	int
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
	ck.comStamp = 0
	ck.leaderId = 0
	DPrintf("MakeClerk: ck.id = %v\n", ck.id)

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
	DPrintf("client(%v).Get(%s)\n", ck.id, key)
	// 一个 command 对应一个 comStamp, 故后面的重试全部使用这一个 CommandId
	tmpComId := ck.comStamp
	ck.comStamp++

	args := GetArgs{
		Key: 		key,
		ClientId: 	ck.id,
		CommandId:	tmpComId,
	}
	var reply GetReply

	for {
		reply = GetReply{}
		if !ck.servers[ck.leaderId].Call("KVServer.Get", &args, &reply) {
			continue
		}
		DPrintf("client(#%v).Get: reply = %v\n", ck.id, reply)
		if reply.Err == OK {
			return reply.Value
		} else if reply.Err == ErrNoKey {
			return ""
		} else if reply.Err == ErrWrongLeader {
			ck.leaderId = (ck.leaderId + 1) % len(ck.servers)
			
			time.Sleep(100 * time.Millisecond)
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
	DPrintf("client(#%v).PutAppend(%s,%s,%s)\n", ck.id, key, value, op)
	// 一个 command 对应一个 comStamp, 故后面的重试全部使用这一个 CommandId
	tmpComId := ck.comStamp
	ck.comStamp++
	
	args := PutAppendArgs {
		Key: 		key,
		Value:		value,
		Op:			op,
		ClientId: 	ck.id,
		CommandId:	tmpComId,
	}
	var reply PutAppendReply

	for {
		reply = PutAppendReply{}
		
		if !ck.servers[ck.leaderId].Call("KVServer.PutAppend", &args, &reply) {
			continue
		}
		DPrintf("client(#%v).PutAppend: reply = %v\n", ck.id, reply)
		if reply.Err == OK {
			break
		} else if reply.Err == ErrWrongLeader {
			ck.leaderId = (ck.leaderId + 1) % len(ck.servers)
			time.Sleep(100 * time.Millisecond)
		}
	}
}

func (ck *Clerk) Put(key string, value string) {
	ck.PutAppend(key, value, put_str)
}
func (ck *Clerk) Append(key string, value string) {
	ck.PutAppend(key, value, append_str)
}

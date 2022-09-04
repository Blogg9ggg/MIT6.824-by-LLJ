package kvraft

import "time"

const (
	OK             = "OK"
	ErrNoKey       = "ErrNoKey"
	ErrWrongLeader = "ErrWrongLeader"
	ErrTimeOut	   = "ErrTimeOut"

	get_str		= "get"
	put_str		= "put"
	append_str	= "append"

	ExecuteTimeout = 500 * time.Millisecond
)

type Err string

// Put or Append
type PutAppendArgs struct {
	Key   string
	Value string
	Op    string // "Put" or "Append"
	// You'll have to add definitions here.
	// Field names must start with capital letters,
	// otherwise RPC will break.
	ClientId	int64
	SequenceNum	int
}

type PutAppendReply struct {
	Status Err
}

type GetArgs struct {
	Key string
}

type GetReply struct {
	Status	Err
	Value 	string
}


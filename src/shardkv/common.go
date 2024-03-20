package shardkv

import "6.5840/shardctrler"

// Sharded key/value server.
// Lots of replica groups, each running Raft.
// Shardctrler decides which group serves each shard.
// Shardctrler may change shard assignment from time to time.
//
// You will have to modify these definitions.
const configTime = 50
const checkShardMovingTime = 100
const (
	OK             = "OK"
	ErrNoKey       = "ErrNoKey"
	ErrWrongGroup  = "ErrWrongGroup"
	ErrWrongLeader = "ErrWrongLeader"
	ErrTimeOut     = "ErrTimeOut"
	ErrWaitShard   = "ErrWaitShard"
	ErrConfigNum   = "ErrConfigNum"
	ErrDulplicate  = "ErrDulplicate"
)

type Err string

const (
	Servering = "Servering"
	Pulling   = "Pulling"
	Pushing   = "Pushing"
	NotFind   = "NotFind"
	Waitting  = "Waitting"
)

type Status string

const (
	Put       = "Put"
	Get       = "Get"
	Append    = "Append"
	Config    = "Config"
	AddShard  = "AddShard"
	MoveShard = "MoveShard"
)

type Type string

// Put or Append
type PutAppendArgs struct {
	// You'll have to add definitions here.
	Key   string
	Value string
	Op    string // "Put" or "Append"
	// You'll have to add definitions here.
	// Field names must start with capital letters,
	// otherwise RPC will break.
	SeqNum   int
	ClientId int64
}

type PutAppendReply struct {
	Err Err
}

type GetArgs struct {
	Key string
	// You'll have to add definitions here.
	SeqNum   int
	ClientId int64
}

type GetReply struct {
	Err   Err
	Value string
}

type Shard struct {
	DataBase map[string]string
	//Config   shardctrler.Config
}

type RequestShardsArgs struct {
	Config       shardctrler.Config
	ShardIndexes int
	ClientId     int
	SeqNum       int
}

type RequestShardsReply struct {
	Data Shard
	Err  Err
}

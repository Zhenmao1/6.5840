package kvraft

import (
	"crypto/rand"
	"math/big"

	"6.5840/labrpc"
)

type Clerk struct {
	servers []*labrpc.ClientEnd
	// You will have to modify this struct.
	//mu sync.Mutex
	//发起命令需要附带的参数
	leaderId int
	clientId int64
	seqNum   int
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
	// You'll have to add code here.
	ck.leaderId = 0
	ck.clientId = nrand()
	ck.seqNum = 0
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
// Get方法
func (ck *Clerk) Get(key string) string {
	//ck.mu.Lock()
	clientId := ck.clientId
	leaderId := ck.leaderId
	seqNum := ck.seqNum + 1
	args := GetArgs{Key: key, SeqNum: seqNum, ClientId: clientId}
	//ck.mu.Unlock()

	for {
		reply := GetReply{}
		DPrintf("****客户端 %v 对 KV 服务器 %v 发起第%d指令 GET 指令信息：%v，Key：%v，Value：%v", clientId, leaderId, args.SeqNum, seqNum, key, "")
		ok := ck.servers[leaderId].Call("KVServer.Get", &args, &reply)
		if !ok {
			DPrintf("客户端 %v 对 KV 服务器 %v 发起第%d指令 GET 请求失败，尝试更换领导者为 %v，指令信息：%v，Key：%v，Value：%v", clientId, leaderId, args.SeqNum, (leaderId+1)%len(ck.servers), seqNum, key, "")
			leaderId = (leaderId + 1) % len(ck.servers)
			continue
		}
		switch reply.Err {
		case ErrWrongLeader:
			DPrintf("KV 服务器 %v 第%d指令不是领导者，尝试更换领导者为 %v，指令信息：%v，Key：%v，Value：%v", leaderId, args.SeqNum, (leaderId+1)%len(ck.servers), seqNum, key, "")
			leaderId = (leaderId + 1) % len(ck.servers)
			continue
		case ErrTimeOut:
			DPrintf("客户端 %v 对 KV 服务器 %v 发起第%d指令 GET 请求超时，尝试更换领导者为 %v，指令信息：%v，Key：%v，Value：%v", clientId, leaderId, args.SeqNum, (leaderId+1)%len(ck.servers), seqNum, key, "")
			leaderId = (leaderId + 1) % len(ck.servers)
			continue
		default:
			//ck.mu.Lock()
			DPrintf("客户端 %v 对 KV 服务器 %v 发起第%d指令 GET 正确返回 %v，Key：%v，Value：%v", clientId, leaderId, ck.seqNum, reply, key, "")
			ck.leaderId = leaderId
			if reply.Err == ErrDulplicate {
				//ck.mu.Unlock()
				return reply.Value
			}
			ck.seqNum++
			//ck.mu.Unlock()

			if reply.Err == ErrNoKey {
				return ""
			} else {
				return reply.Value
			}
		}
	}
}

// PutAppend方法
func (ck *Clerk) PutAppend(key string, value string, op string) {
	//ck.mu.Lock()
	leaderId := ck.leaderId
	seqNum := ck.seqNum + 1
	clientId := ck.clientId
	//ck.mu.Unlock()
	//DPrintf("key:%s****value:%s", key, value)
	for {
		args := PutAppendArgs{Key: key, Value: value, Op: op, SeqNum: seqNum, ClientId: clientId}
		reply := PutAppendReply{}
		DPrintf("****客户端 %v 对 KV 服务器 %v 发起第%d指令 PUT/APPEND 指令信息：%v，Key：%v，Value：%v", clientId, leaderId, args.SeqNum, seqNum, key, value)
		ok := ck.servers[leaderId].Call("KVServer.PutAppend", &args, &reply)
		if !ok {
			DPrintf("客户端 %v 对 KV 服务器 %v 发起第%d指令 PUT/APPEND 请求失败，尝试更换领导者为 %v", clientId, leaderId, args.SeqNum, (leaderId+1)%len(ck.servers))
			leaderId = (leaderId + 1) % len(ck.servers)
			continue
		}
		switch reply.Err {
		case ErrWrongLeader:
			DPrintf("客户端 %v 尝试对 KV 服务器 %v 发起第%d指令 PUT/APPEND，但它不是领导者，尝试更换领导者为 %v，指令信息：%v，Key：%v，Value：%v", clientId, leaderId, args.SeqNum, (leaderId+1)%len(ck.servers), seqNum, key, value)
			leaderId = (leaderId + 1) % len(ck.servers)
			continue
		case ErrTimeOut:
			DPrintf("客户端 %v 对 KV 服务器 %v 发起第%d指令 PUT/APPEND 请求超时，尝试更换领导者为 %v，指令信息：%v，Key：%v，Value：%v", clientId, leaderId, args.SeqNum, (leaderId+1)%len(ck.servers), seqNum, key, value)
			leaderId = (leaderId + 1) % len(ck.servers)
			continue
		default:
			//ck.mu.Lock()
			DPrintf("客户端 %v 对 KV 服务器 %v 发起第%d指令 PUT/APPEND 正确返回 %v，Key：%v，Value：%v", clientId, leaderId, ck.seqNum, reply, key, value)
			ck.leaderId = leaderId
			if reply.Err == ErrDulplicate {
				//ck.mu.Unlock()
				return
			}
			ck.seqNum++
			return
		}
	}
}

func (ck *Clerk) Put(key string, value string) {
	ck.PutAppend(key, value, "Put")
}
func (ck *Clerk) Append(key string, value string) {
	ck.PutAppend(key, value, "Append")
}

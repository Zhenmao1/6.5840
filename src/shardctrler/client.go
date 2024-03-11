package shardctrler

//
// Shardctrler clerk.
//

import (
	"crypto/rand"
	"math/big"
	"sync"
	"time"

	"6.5840/labrpc"
)

type Clerk struct {
	servers []*labrpc.ClientEnd
	// Your data here.
	clientId int64
	seqNum   int
	mu       sync.Mutex
}
type CommandResponse struct {
	Err    Err
	Config Config
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
	// Your code here.
	ck.clientId = nrand()
	ck.seqNum = 1

	return ck
}

func (ck *Clerk) Query(num int) Config {
	args := &QueryArgs{}

	ck.mu.Lock()
	// Your code here.
	args.Num = num
	args.ClientId = ck.clientId
	args.SeqNum = ck.seqNum
	ck.mu.Unlock()
	for {
		// try each known server.
		for index, srv := range ck.servers {
			var reply QueryReply
			DPrintf("****客户端 %v 对 sc 服务器 %d 发起第%d指令 Query 指令信息：%v", args.ClientId, index, args.SeqNum, args)
			ok := srv.Call("ShardCtrler.Query", args, &reply)
			if ok && reply.Err != ErrWrongLeader {
				ck.seqNum++
				return reply.Config
			}
		}
		time.Sleep(100 * time.Millisecond)
	}
}

func (ck *Clerk) Join(servers map[int][]string) {
	args := &JoinArgs{}
	// Your code here.
	ck.mu.Lock()
	args.Servers = servers
	args.ClientId = ck.clientId
	args.SeqNum = ck.seqNum
	ck.mu.Unlock()
	for {
		// try each known server.
		for index, srv := range ck.servers {
			var reply JoinReply
			DPrintf("****客户端 %v 对 sc 服务器 %d 发起第%d指令 Join 指令信息：%v", args.ClientId, index, args.SeqNum, args)
			ok := srv.Call("ShardCtrler.Join", args, &reply)
			if ok && reply.Err != ErrWrongLeader {
				ck.seqNum++
				return
			}
		}
		time.Sleep(100 * time.Millisecond)
	}
}

func (ck *Clerk) Leave(gids []int) {
	args := &LeaveArgs{}
	// Your code here
	ck.mu.Lock()
	args.GIDs = gids
	args.ClientId = ck.clientId
	args.SeqNum = ck.seqNum
	ck.mu.Unlock()
	for {
		// try each known server.
		for index, srv := range ck.servers {
			var reply LeaveReply
			DPrintf("****客户端 %v 对 sc 服务器 %d 发起第%d指令 Leave 指令信息：%v", args.ClientId, index, args.SeqNum, args)
			ok := srv.Call("ShardCtrler.Leave", args, &reply)
			if ok && reply.Err != ErrWrongLeader {
				ck.seqNum++
				return
			}
		}
		time.Sleep(100 * time.Millisecond)
	}
}

func (ck *Clerk) Move(shard int, gid int) {
	args := &MoveArgs{}
	// Your code here.
	ck.mu.Lock()
	args.Shard = shard
	args.GID = gid
	args.ClientId = ck.clientId
	args.SeqNum = ck.seqNum
	ck.mu.Unlock()
	for {
		// try each known server.
		for index, srv := range ck.servers {
			var reply MoveReply
			DPrintf("****客户端 %v 对 sc 服务器 %d 发起第%d指令 Move 指令信息：%v", args.ClientId, index, args.SeqNum, args)
			ok := srv.Call("ShardCtrler.Move", args, &reply)
			if ok && reply.Err != ErrWrongLeader {
				ck.seqNum++
				return
			}
		}
		time.Sleep(100 * time.Millisecond)
	}
}

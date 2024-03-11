package shardkv

import (
	"crypto/rand"
	"math/big"
	"time"

	"6.5840/labrpc"
	"6.5840/shardctrler"
)

// 计算键属于哪个分片
// 请使用此函数，
// 并请不要修改它。
func key2shard(key string) int {
	shard := 0
	if len(key) > 0 {
		shard = int(key[0])
	}
	shard %= shardctrler.NShards
	return shard
}

// 生成一个随机数，用于RPC调用的ID等
func nrand() int64 {
	max := big.NewInt(int64(1) << 62)
	bigx, _ := rand.Int(rand.Reader, max)
	x := bigx.Int64()
	return x
}

// 客户端Clerk结构体
type Clerk struct {
	sm       *shardctrler.Clerk             // 分片控制器的客户端
	config   shardctrler.Config             // 当前的分片配置
	make_end func(string) *labrpc.ClientEnd // 函数，用于根据服务器名生成RPC客户端端点
	//lab4B

	seqNum   int
	clientId int64
}

// 创建一个新的Clerk
// ctrlers[] 是调用shardctrler.MakeClerk()所需的。
// make_end(servername) 将配置中的服务器名称转换为可以发送RPCs的labrpc.ClientEnd。
func MakeClerk(ctrlers []*labrpc.ClientEnd, make_end func(string) *labrpc.ClientEnd) *Clerk {
	ck := new(Clerk)
	ck.sm = shardctrler.MakeClerk(ctrlers) // 使用分片控制器的端点创建控制器客户端
	ck.make_end = make_end                 // 设置RPC端点生成函数
	// 可以在这里进行额外的初始化工作

	ck.clientId = nrand()
	ck.seqNum = 1
	return ck
}

// 获取键的当前值。
// 如果键不存在，则返回""。
// 面对所有其他错误时，将永远重试。
func (ck *Clerk) Get(key string) string {

	args := GetArgs{}
	args.Key = key
	args.SeqNum = ck.seqNum
	args.ClientId = ck.clientId

	for {
		shard := key2shard(key)        // 根据键计算它所在的分片
		gid := ck.config.Shards[shard] // 根据分片找到负责该分片的组
		if servers, ok := ck.config.Groups[gid]; ok {
			// 尝试查询分片所在组的每个服务器
			for si := 0; si < len(servers); si++ {
				srv := ck.make_end(servers[si])
				var reply GetReply
				DPrintf("####客户端 %v 对 kv 服务器 %s 发起第%d指令 GET 指令信息：%v", args.ClientId, servers[si], args.SeqNum, args)
				ok := srv.Call("ShardKV.Get", &args, &reply)
				if ok && (reply.Err == OK || reply.Err == ErrNoKey) {
					ck.seqNum++
					return reply.Value
				}
				if ok && (reply.Err == ErrWrongGroup) {
					break
				}
				// 如果不成功，或者错误为ErrWrongLeader，继续尝试其他服务器
			}
		}
		time.Sleep(100 * time.Millisecond) // 在重试前稍作等待
		ck.config = ck.sm.Query(-1)        // 从控制器查询最新配置
		DPrintf("####客户端 %v 对 ctrller 服务器 发起query 指令config%v", args.ClientId, ck.config)
	}
	return ""
}

// Put和Append共用的函数。
func (ck *Clerk) PutAppend(key string, value string, op string) {
	args := PutAppendArgs{}
	args.Key = key
	args.Value = value
	args.Op = op
	args.SeqNum = ck.seqNum
	args.ClientId = ck.clientId
	for {
		shard := key2shard(key)        // 计算键所在的分片
		gid := ck.config.Shards[shard] // 找到负责该分片的组
		if servers, ok := ck.config.Groups[gid]; ok {
			// 尝试向负责的组的每个服务器发送Put或Append请求
			for si := 0; si < len(servers); si++ {
				srv := ck.make_end(servers[si])
				var reply PutAppendReply
				DPrintf("####客户端 %v 对 kv 服务器 %s 发起第%d指令 Put/Append 指令信息：%v", args.ClientId, servers[si], args.SeqNum, args)
				ok := srv.Call("ShardKV.PutAppend", &args, &reply)
				if ok && reply.Err == OK {
					ck.seqNum++
					return
				}
				if ok && reply.Err == ErrWrongGroup {
					break
				}
				// 如果不成功，或者错误为ErrWrongLeader，或者时间超过，继续尝试其他服务器
				if ok && (reply.Err == ErrTimeOut || reply.Err == ErrWrongLeader) {
					continue
				}
			}
		}

		time.Sleep(100 * time.Millisecond) // 在重试前稍作等待
		ck.config = ck.sm.Query(-1)        // 从控制器查询最新配置
		DPrintf("####客户端 %v 对 ctrller 服务器 发起query 指令config%v", args.ClientId, ck.config)
	}
}

// 提供Put方法，封装PutAppend调用
func (ck *Clerk) Put(key string, value string) {
	ck.PutAppend(key, value, "Put")
}

// 提供Append方法，封装PutAppend调用
func (ck *Clerk) Append(key string, value string) {
	ck.PutAppend(key, value, "Append")
}

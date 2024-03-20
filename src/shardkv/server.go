package shardkv

import (
	"sync"
	"time"

	"6.5840/labgob" // RPC通信库
	"6.5840/labrpc"
	"6.5840/raft"
	"6.5840/shardctrler"
)

// Raft共识算法库
// Go语言的同步库，提供互斥锁等同步机制
// Go的序列化库

// Op 结构体定义了一个操作，可以是Get、Put或Append。
// 注意字段名必须以大写字母开头，否则RPC调用时无法正常序列化/反序列化。
type Op struct {
	// 你的定义在这里。
	Option       Type
	ClientId     int64
	SeqNum       int
	Key          string
	Value        string
	Index        int
	Config       shardctrler.Config
	Data         Shard
	ShardIndexes int
	Err          Err
}

// ShardKV 结构体代表了一个分片的键值存储服务实例。
type ShardKV struct {
	mu           sync.Mutex                     // 互斥锁，用于保护共享数据的安全访问
	me           int                            // 当前服务器在集群中的索引号
	rf           *raft.Raft                     // 该实例关联的Raft协议实现
	applyCh      chan raft.ApplyMsg             // 从Raft层接收已提交日志项的通道
	make_end     func(string) *labrpc.ClientEnd // 函数，用于根据服务器名称生成RPC端点
	gid          int                            // 当前分片组的组ID（GID）
	ctrlers      []*labrpc.ClientEnd            // 分片控制器的RPC端点列表
	maxraftstate int                            // 日志达到多大时进行快照，以节省空间（如果为-1，则不进行快照）
	configMu     sync.Mutex                     //用来保护configMu和currentConfig以及shardStatus

	// 你的定义在这里。
	//askShard      []bool
	currentConfig shardctrler.Config
	lastConfig    shardctrler.Config
	waitApplyCh   map[int]chan Op
	completed     map[int64]int
	dead          int32
	shards        []Shard
	sm            *shardctrler.Clerk
	shardStatus   []Status
	includeIndex  int

	//防止shard过程的日志暴增
	seqNum int
	//不设置clientID去重，gid*10+me一定不会重复
}

// Get 方法处理客户端的Get请求。
func (kv *ShardKV) Get(args *GetArgs, reply *GetReply) {
	// Your code here.
	if kv.killed() {
		reply.Err = ErrWrongLeader
		return
	}
	shardId := key2shard(args.Key)
	//当前要访问的shard不属于自己

	//DPrintf("kvserver%v 收到client%v 第Seq%v条指令,key%s向下传递给raft", kv.me, args.ClientId, args.SeqNum, args.Key)
	kv.mu.Lock()
	if kv.currentConfig.Shards[shardId] != kv.gid {
		reply.Err = ErrWrongGroup
		DPrintf("kvserver%v %v 收到client%v 第Seq%v条指令,但key%s不属于这个group", kv.gid, kv.me, args.ClientId, args.SeqNum, args.Key)
		kv.mu.Unlock()
		return
	}
	if kv.shardStatus[shardId] == Pulling || kv.shardStatus[shardId] == Pushing || kv.shardStatus[shardId] == Waitting {
		reply.Err = ErrWaitShard
		DPrintf("kvserver%v %v 收到client%v 第Seq%v条指令,但shard%d处于迁移过程%s", kv.gid, kv.me, args.ClientId, args.SeqNum, shardId, kv.shardStatus[shardId])
		kv.mu.Unlock()
		return
	}
	kv.mu.Unlock()
	//当前要访问的不是leader
	_, Isleader := kv.rf.GetState()
	if !Isleader {
		reply.Err = ErrWrongLeader
		return
	}
	DPrintf("kvserver%v %v 收到client%v 第Seq%v条指令,key%s向下传递给raft", kv.gid, kv.me, args.ClientId, args.SeqNum, args.Key)
	cmd := Op{
		Option:   Get,
		Key:      args.Key,
		ClientId: args.ClientId,
		SeqNum:   args.SeqNum,
	}
	lastAppliedIndex, _, _ := kv.rf.Start(cmd)

	//应该在他提交之后再创建chan，不然此处宕掉，会产生一条莫名的chan
	waitCh := kv.makeWaitApplyCh(lastAppliedIndex)
	select {
	case replyOp := <-waitCh:
		//正确收到goruntine的通知，可以应用到状态机了，之后返回
		//第一步先关闭通道
		if cmd.ClientId != replyOp.ClientId || cmd.SeqNum != replyOp.SeqNum {
			if Isleader {
				DPrintf("kvserver%v 向raft发出的op%v，没有正确的返回%v", kv.me, cmd, replyOp)
			}
			reply.Err = ErrWrongLeader
			return
		}
		kv.mu.Lock()
		if ch, ok := kv.waitApplyCh[replyOp.Index]; ok {
			close(ch)
			delete(kv.waitApplyCh, replyOp.Index)
		}
		reply.Err = OK
		reply.Value = replyOp.Value
		if replyOp.Err == ErrWrongGroup {
			reply.Err = ErrWrongGroup
		}
		if Isleader {
			DPrintf("kvserver%v %v get正确执行，返回参数%s", kv.gid, kv.me, reply.Value)
		}
		kv.mu.Unlock()
		return
	case <-time.After(time.Duration(time.Millisecond * 5000)):
		reply.Err = ErrTimeOut
		return
	}
}

// PutAppend 方法处理客户端的Put和Append请求。
func (kv *ShardKV) PutAppend(args *PutAppendArgs, reply *PutAppendReply) {
	if kv.killed() {
		reply.Err = ErrWrongLeader
		return
	}

	//当前要访问的shard不属于自己
	shardId := key2shard(args.Key)
	kv.mu.Lock()
	if kv.currentConfig.Shards[shardId] != kv.gid {
		reply.Err = ErrWrongGroup
		DPrintf("kvserver%v %v 收到client%v 第Seq%v条指令,但key不属于这个group", kv.gid, kv.me, args.ClientId, args.SeqNum)
		kv.mu.Unlock()
		return
	}
	if kv.shardStatus[shardId] == Pulling || kv.shardStatus[shardId] == Pushing || kv.shardStatus[shardId] == Waitting {
		reply.Err = ErrWaitShard
		DPrintf("kvserver%v 收到client%v 第Seq%v条指令,但shard%d处于迁移过程%s", kv.me, args.ClientId, args.SeqNum, shardId, kv.shardStatus[shardId])
		kv.mu.Unlock()
		return
	}
	kv.mu.Unlock()
	//DPrintf("server%v 收到client%v 第Seq%v条指令", kv.me, args.ClientId, args.SeqNum)
	_, Isleader := kv.rf.GetState()
	if !Isleader {
		DPrintf("kvserver%v 收到client第Seq%v条指令，但自己不是leader", kv.me, args.SeqNum)
		reply.Err = ErrWrongLeader
		return
	}
	DPrintf("kvserver%v %v 收到client%v 第Seq%v条指令,key%s,value%s向下传递给raft", kv.gid, kv.me, args.ClientId, args.SeqNum, args.Key, args.Value)

	cmd := Op{
		Key:      args.Key,
		Value:    args.Value,
		ClientId: args.ClientId,
		SeqNum:   args.SeqNum,
	}
	if args.Op == "Put" {
		cmd.Option = Put
	} else {
		cmd.Option = Append
	}
	lastAppliedIndex, _, _ := kv.rf.Start(cmd)
	//应该在他提交之后再创建chan，不然此处宕掉，会产生一条莫名的chan
	waitCh := kv.makeWaitApplyCh(lastAppliedIndex)
	select {
	case replyOp := <-waitCh:
		//正确收到goruntine的通知，可以应用到状态机了，之后返回
		//第一步先关闭通道
		if cmd.ClientId != replyOp.ClientId || cmd.SeqNum != replyOp.SeqNum {
			if Isleader {
				DPrintf("kvserver%v %v向raft发出的op%v，没有正确的返回%v", kv.gid, kv.me, cmd, replyOp)
			}
			reply.Err = ErrWrongLeader
			return
		}
		kv.mu.Lock()
		if ch, ok := kv.waitApplyCh[replyOp.Index]; ok {
			close(ch)
			delete(kv.waitApplyCh, replyOp.Index)
		}
		kv.mu.Unlock()
		reply.Err = OK
		if replyOp.Err == ErrWrongGroup {
			reply.Err = ErrWrongGroup
		}
		if Isleader {
			DPrintf("kvserver%v %v成功PUT/APPEND key%s value%s 返回参数%v", kv.gid, kv.me, replyOp.Key, replyOp.Value, reply)
		}
		return
	case <-time.After(time.Duration(time.Millisecond * 5000)):
		DPrintf("kvserver%d定时器时间到", kv.me)
		reply.Err = ErrTimeOut
		return
	}
}

func (kv *ShardKV) CatchApplyOp() {
	for {
		if kv.killed() {
			return
		}
		applyMsg := <-kv.applyCh
		if _, Isleader := kv.rf.GetState(); Isleader {
			DPrintf("kvserver%v %v 从applyCH 收到index%v cmd%v", kv.gid, kv.me, applyMsg.CommandIndex, applyMsg)
		}
		if applyMsg.CommandValid {
			kv.commandHandler(applyMsg)
		}
		if applyMsg.SnapshotValid {
			kv.snapshotHandler(applyMsg)
		}
	}
}

func (kv *ShardKV) commandHandler(applyMsg raft.ApplyMsg) {
	kv.mu.Lock()
	defer kv.mu.Unlock()
	op := applyMsg.Command.(Op)

	//所有的kvserver都会拿到op，但只有leader需要返回处理，其余只用应用到状态机
	shardId := key2shard(op.Key)
	switch op.Option {
	case Get:
		if kv.completed[op.ClientId] < op.SeqNum {
			//DPrintf("database%v", kv.shards[shardId].DataBase)
			if kv.shardStatus[shardId] == Servering {
				op.Value = kv.shards[shardId].DataBase[op.Key]
			} else {
				op.Err = ErrWrongGroup
				DPrintf("database%v,要应用到shard发现shard转移，返回给上层get错误，重新发起命令", kv.shards[shardId].DataBase)
			}
		}
		op.Value = kv.shards[shardId].DataBase[op.Key]
		kv.completed[op.ClientId] = op.SeqNum
	case Put:
		if kv.completed[op.ClientId] < op.SeqNum {
			//DPrintf("database%v", kv.shards[shardId].DataBase)
			if kv.shardStatus[shardId] == Servering {
				kv.shards[shardId].DataBase[op.Key] = op.Value
			} else {
				op.Err = ErrWrongGroup
				DPrintf("database%v,要应用到shard发现shard转移，返回给上层put错误，重新发起命令", kv.shards[shardId].DataBase)
			}

		}
		op.Value = kv.shards[shardId].DataBase[op.Key]
		kv.completed[op.ClientId] = op.SeqNum
	case Append:
		if kv.completed[op.ClientId] < op.SeqNum {
			//DPrintf("database%v", kv.shards[shardId].DataBase)
			if kv.shardStatus[shardId] == Servering {
				kv.shards[shardId].DataBase[op.Key] += op.Value
			} else {
				op.Err = ErrWrongGroup
				DPrintf("database%v,要应用到shard发现shard转移，返回给上层append错误，重新发起命令", kv.shards[shardId].DataBase)
			}
			op.Value = kv.shards[shardId].DataBase[op.Key]
		}
		kv.completed[op.ClientId] = op.SeqNum
	case Config:
		//这里的seq已经借用了config的编号了
		kv.configHandller(op)
	case AddShard:
		kv.addShardHandller(op)
	case MoveShard:
		kv.moveShardHandller(op)
	}
	//检查是否是leader,是的话要应用到状态机
	if _, isLeader := kv.rf.GetState(); isLeader {
		kv.isNeedSnapShot(applyMsg)
		waitCh, ok := kv.waitApplyCh[applyMsg.CommandIndex]
		if ok {
			waitCh <- op
		} else {
			DPrintf("kvserver%v %v检测到raft发出的applymsg，但是返回的通道关闭了%v", kv.gid, kv.me, op)
		}
	}

}

// StartServer 函数用于启动一个ShardKV服务器实例。
// servers[] 包含了该组中所有服务器的端口。
// me 是当前服务器在servers[]中的索引。
// 该k/v服务器通过底层的Raft实现来存储快照，当Raft的保存状态超过maxraftstate字节时，应该进行快照，以允许Raft清理其日志。
// 如果maxraftstate是-1，就不需要进行快照。
// gid 是该组的组ID（GID），用于与shardctrler交互。
// 通过ctrlers[]可以向shardctrler发送RPCs。
// make_end(servername) 函数用于将Config.Groups[gid][i]中的服务器名转换为可以发送RPCs的labrpc.ClientEnd。
// 查看client.go了解如何使用ctrlers[]和make_end()向拥有特定分片的组发送RPCs。
// StartServer() 必须快速返回，所以它应该为任何长时间运行的工作启动goroutines。
func StartServer(servers []*labrpc.ClientEnd, me int, persister *raft.Persister, maxraftstate int, gid int, ctrlers []*labrpc.ClientEnd, make_end func(string) *labrpc.ClientEnd) *ShardKV {
	// 在你希望Go的RPC库能够序列化/反序列化的结构体上调用labgob.Register。
	labgob.Register(Op{})

	kv := new(ShardKV)
	kv.me = me
	kv.maxraftstate = maxraftstate
	kv.make_end = make_end
	kv.gid = gid
	kv.ctrlers = ctrlers

	// 初始化代码在这里。
	kv.shards = make([]Shard, shardctrler.NShards)
	kv.applyCh = make(chan raft.ApplyMsg)
	kv.rf = raft.Make(servers, me, persister, kv.applyCh)
	// 因为可能会crash重连
	snapshot := persister.ReadSnapshot()
	if len(snapshot) > 0 {
		kv.snapShotDecode(snapshot)
	}
	kv.waitApplyCh = make(map[int]chan Op)
	kv.completed = make(map[int64]int)
	kv.includeIndex = 0
	kv.sm = shardctrler.MakeClerk(ctrlers)
	kv.shards = make([]Shard, shardctrler.NShards)
	//kv.askShard = make([]bool, shardctrler.NShards)
	kv.shardStatus = make([]Status, shardctrler.NShards)
	// 遍历切片并初始化每个元素的 map
	for i := range kv.shards {
		kv.shards[i].DataBase = make(map[string]string, 0)
	}
	go kv.CatchApplyOp()
	go kv.CatchConfig()
	go kv.CatchShards()
	return kv
}

func (kv *ShardKV) makeWaitApplyCh(index int) chan Op {
	kv.mu.Lock()
	defer kv.mu.Unlock()
	waitCh, ok := kv.waitApplyCh[index]
	if !ok {
		waitCh = make(chan Op, 1)
		kv.waitApplyCh[index] = waitCh
	}
	return waitCh
}

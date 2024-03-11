package shardctrler

import (
	"sync"
	"time"

	"6.5840/labgob"
	"6.5840/labrpc"
	"6.5840/raft"
)

type ShardCtrler struct {
	mu      sync.Mutex
	me      int
	rf      *raft.Raft
	applyCh chan raft.ApplyMsg

	// Your data here.
	configs []Config // indexed by config num

	//lab4A
	complete    map[int64]int
	waitApplyCh map[int]chan Op
	//以下是ring算法负责迁移和负载平衡
	ring         map[uint32]int // 一致性哈希环，映射哈希值到组ID
	sortedHashes []uint32       // 哈希环上哈希值的有序列表
}

type Op struct {
	// Your data here.
	Option   OpType
	ClientId int64
	SeqNum   int
	Servers  map[int][]string
	GIDs     []int
	Shard    int
	Num      int
	Config   Config
}

func (sc *ShardCtrler) Join(args *JoinArgs, reply *JoinReply) {
	// Your code here.
	if _, isleader := sc.rf.GetState(); !isleader {
		DPrintf("Scserver%v收到client%v第Seq%v条指令，但自己不是leader", sc.me, args.ClientId, args.SeqNum)
		reply.Err = ErrWrongLeader
		return
	}
	op := Op{
		Option:   Join,
		ClientId: args.ClientId,
		SeqNum:   args.SeqNum,
		Servers:  args.Servers,
	}
	lastAppliedIndex, _, _ := sc.rf.Start(op)
	waitCh := sc.makeWaitApplyCh(lastAppliedIndex)
	select {
	case replyOp := <-waitCh:
		//正确收到goruntine的通知，可以应用到状态机了，之后返回
		//第一步先关闭通道
		if op.ClientId != replyOp.ClientId || op.SeqNum != replyOp.SeqNum {
			DPrintf("scserver%d 向raft发出的op%v，没有正确的返回%v", sc.me, op, replyOp)
			reply.Err = ErrWrongLeader
			return
		}
		reply.Err = OK
		DPrintf("scserver%d成功添加 Config%v GID%v shard%v num%d", sc.me, replyOp.Config, replyOp.Config.Groups, replyOp.Config.Shards, replyOp.Config.Num)
		return
	case <-time.After(time.Duration(time.Millisecond * 5000)):
		DPrintf("scserver%v定时器时间到", sc.me)
		reply.Err = ErrTimeOut
		return
	}

}

func (sc *ShardCtrler) Leave(args *LeaveArgs, reply *LeaveReply) {
	// Your code here.
	// Your code here.
	if _, isleader := sc.rf.GetState(); !isleader {
		DPrintf("Scserver%v收到client%v第Seq%v条指令，但自己不是leader", sc.me, args.ClientId, args.SeqNum)
		reply.Err = ErrWrongLeader
		return
	}
	op := Op{
		Option:   Leave,
		ClientId: args.ClientId,
		SeqNum:   args.SeqNum,
		GIDs:     args.GIDs,
	}
	lastAppliedIndex, _, _ := sc.rf.Start(op)
	waitCh := sc.makeWaitApplyCh(lastAppliedIndex)
	select {
	case replyOp := <-waitCh:
		//正确收到goruntine的通知，可以应用到状态机了，之后返回
		//第一步先关闭通道
		if op.ClientId != replyOp.ClientId || op.SeqNum != replyOp.SeqNum {
			DPrintf("scserver%d 向raft发出的op%v，没有正确的返回%v", sc.me, op, replyOp)
			reply.Err = ErrWrongLeader
			return
		}
		reply.Err = OK
		DPrintf("scserver%d成功添加 Config%v GID%v shard%v num%d", sc.me, replyOp.Config, replyOp.Config.Groups, replyOp.Config.Shards, replyOp.Config.Num)
		return
	case <-time.After(time.Duration(time.Millisecond * 5000)):
		DPrintf("scserve%dr定时器时间到", sc.me)
		reply.Err = ErrTimeOut
		return
	}
}

func (sc *ShardCtrler) Move(args *MoveArgs, reply *MoveReply) {
	// Your code here.
	// Your code here.
	if _, isleader := sc.rf.GetState(); !isleader {
		DPrintf("Scserver%d收到client%v第Seq%v条指令，但自己不是leader", sc.me, args.ClientId, args.SeqNum)
		reply.Err = ErrWrongLeader
		return
	}
	op := Op{
		Option:   Move,
		ClientId: args.ClientId,
		SeqNum:   args.SeqNum,
		Shard:    args.Shard,
		GIDs:     make([]int, 0),
	}
	op.GIDs = append(op.GIDs, args.GID)
	lastAppliedIndex, _, _ := sc.rf.Start(op)
	waitCh := sc.makeWaitApplyCh(lastAppliedIndex)
	select {
	case replyOp := <-waitCh:
		//正确收到goruntine的通知，可以应用到状态机了，之后返回
		//第一步先关闭通道
		if op.ClientId != replyOp.ClientId || op.SeqNum != replyOp.SeqNum {
			DPrintf("scserver%d 向raft发出的op%v，没有正确的返回%v", sc.me, op, replyOp)
			reply.Err = ErrWrongLeader
			return
		}
		reply.Err = OK
		DPrintf("scserver%d成功添加 Config%v GID%v shard%v num%d", sc.me, replyOp.Config, replyOp.Config.Groups, replyOp.Config.Shards, replyOp.Config.Num)
		return
	case <-time.After(time.Duration(time.Millisecond * 5000)):
		DPrintf("scserver定时器时间到")
		reply.Err = ErrTimeOut
		return
	}
}

func (sc *ShardCtrler) Query(args *QueryArgs, reply *QueryReply) {
	// Your code here.
	if _, isleader := sc.rf.GetState(); !isleader {
		//DPrintf("Scserver%d收到client%v第Seq%v条指令，但自己不是leader", sc.me, args.ClientId, args.SeqNum)
		reply.Err = ErrWrongLeader
		return
	}
	op := Op{
		Option:   Query,
		ClientId: args.ClientId,
		SeqNum:   args.SeqNum,
		Num:      args.Num,
	}
	DPrintf("Scserver%d收到client第Seq%v条指令，构造op%v", sc.me, args.SeqNum, op)
	lastAppliedIndex, _, _ := sc.rf.Start(op)
	waitCh := sc.makeWaitApplyCh(lastAppliedIndex)
	select {
	case replyOp := <-waitCh:
		//正确收到goruntine的通知，可以应用到状态机了，之后返回
		//第一步先关闭通道
		if op.ClientId != replyOp.ClientId || op.SeqNum != replyOp.SeqNum {
			DPrintf("scserver %d向raft发出的op%v，没有正确的返回%v", sc.me, op, replyOp)
			reply.Err = ErrWrongLeader
			return
		}
		sc.mu.Lock()
		if ch, ok := sc.waitApplyCh[lastAppliedIndex]; ok {
			close(ch)
			delete(sc.waitApplyCh, lastAppliedIndex)
		}
		sc.mu.Unlock()
		reply.Err = OK
		reply.Config = replyOp.Config
		DPrintf("scserver%d成功添加 Config%v GID%v shard%v num%d", sc.me, replyOp.Config, replyOp.Config.Groups, replyOp.Config.Shards, replyOp.Config.Num)
		return
	case <-time.After(time.Duration(time.Millisecond * 5000)):
		DPrintf("scserver定时器时间到")
		reply.Err = ErrTimeOut
		return
	}

}

func (sc *ShardCtrler) makeWaitApplyCh(index int) chan Op {
	sc.mu.Lock()
	defer sc.mu.Unlock()
	waitCh, ok := sc.waitApplyCh[index]
	if !ok {
		waitCh = make(chan Op, 1)
		sc.waitApplyCh[index] = waitCh
	}
	return waitCh
}

// the tester calls Kill() when a ShardCtrler instance won't
// be needed again. you are not required to do anything
// in Kill(), but it might be convenient to (for example)
// turn off debug output from this instance.
func (sc *ShardCtrler) Kill() {
	sc.rf.Kill()
	// Your code here, if desired.
}

// needed by shardkv tester
func (sc *ShardCtrler) Raft() *raft.Raft {
	return sc.rf
}

func (sc *ShardCtrler) CatchApplyMsg() {
	for {
		applyMsg := <-sc.applyCh
		if _, isleader := sc.rf.GetState(); isleader {
			DPrintf("ShardCtrler%d 从applyCH 收到index%v applyMsg%v", sc.me, applyMsg.CommandIndex, applyMsg)
		}

		if applyMsg.CommandValid {
			sc.commandHandler(applyMsg)
		}

	}
}
func (sc *ShardCtrler) commandHandler(applyMsg raft.ApplyMsg) {
	sc.mu.Lock()
	defer sc.mu.Unlock()
	op := applyMsg.Command.(Op)
	switch op.Option {
	case Query:
		if sc.complete[op.ClientId] < op.SeqNum {
			if op.Num == -1 || op.Num >= len(sc.configs) {
				op.Config = sc.configs[len(sc.configs)-1]
			} else {
				op.Config = sc.configs[op.Num]
			}

		}
	// 分片控制器应创建一个新的配置，在该配置中，将分片分配给指定的组。Move 的目的是
	// 允许我们测试你的软件。在 Move 之后执行 Join 或 Leave 可能会撤销 Move 的操作
	// 因为 Join 和 Leave 会进行重新平衡。
	case Move:
		if sc.complete[op.ClientId] < op.SeqNum {
			// 先拷贝旧的分组信息
			newGroups := make(map[int][]string)
			oldConfig := sc.configs[len(sc.configs)-1]
			for gid, server := range oldConfig.Groups {
				newGroups[gid] = server
			}
			newShards := oldConfig.Shards
			newShards[op.Shard] = op.GIDs[0]
			newConfig := Config{Num: oldConfig.Num + 1, Shards: newShards, Groups: newGroups}
			sc.configs = append(sc.configs, newConfig)
			DPrintf("scserver%d执行move成功,最新的配置信息index%d,config%v", sc.me, newConfig.Num, newConfig)
		}
	case Leave:
		if sc.complete[op.ClientId] < op.SeqNum {
			// 先拷贝旧的分组信息
			newGroups := make(map[int][]string)
			oldConfig := sc.configs[len(sc.configs)-1]
			for gid, server := range oldConfig.Groups {
				newGroups[gid] = server
			}
			//删除离开的group0
			for _, gid := range op.GIDs {
				delete(newGroups, gid)
			}
			newConfig := Config{Num: oldConfig.Num + 1, Shards: oldConfig.Shards, Groups: newGroups}
			sc.configs = append(sc.configs, newConfig)
			sc.updateRing()
			sc.rebalanceShards()
			DPrintf("scserver%d执行leave成功,最新的配置信息index%d,config%v", sc.me, newConfig.Num, newConfig)
		}
	case Join:
		if sc.complete[op.ClientId] < op.SeqNum {
			//先拷贝旧的分组信息
			newGroups := make(map[int][]string)
			oldConfig := sc.configs[len(sc.configs)-1]
			for gid, server := range oldConfig.Groups {
				newGroups[gid] = server
			}
			//添加新的分组
			for newgid, newserver := range op.Servers {
				newGroups[newgid] = newserver
			}
			newConfig := Config{Num: oldConfig.Num + 1, Shards: oldConfig.Shards, Groups: newGroups}
			sc.configs = append(sc.configs, newConfig)
			sc.updateRing()
			sc.rebalanceShards()
			op.Config = sc.configs[len(sc.configs)-1]
			DPrintf("scserver%d执行jion成功,最新的配置信息index%d,config%v", sc.me, newConfig.Num, op.Config)

		}
	}
	sc.complete[op.ClientId] = op.SeqNum

	if _, isleader := sc.rf.GetState(); isleader {
		waitCh, exist := sc.waitApplyCh[applyMsg.CommandIndex]
		if exist {
			waitCh <- op
		} else {
			DPrintf("SCserver%d检测到raft发出的applymsg，但是返回的通道关闭了%v", sc.me, op)
		}
	}
}

// servers[] contains the ports of the set of
// servers that will cooperate via Raft to
// form the fault-tolerant shardctrler service.
// me is the index of the current server in servers[].
func StartServer(servers []*labrpc.ClientEnd, me int, persister *raft.Persister) *ShardCtrler {
	sc := new(ShardCtrler)
	sc.me = me

	sc.configs = make([]Config, 1)
	sc.configs[0].Groups = map[int][]string{}

	labgob.Register(Op{})
	sc.applyCh = make(chan raft.ApplyMsg)
	sc.rf = raft.Make(servers, me, persister, sc.applyCh)

	// Your code here.
	sc.complete = make(map[int64]int)
	sc.waitApplyCh = make(map[int]chan Op)
	sc.ring = make(map[uint32]int)
	sc.sortedHashes = make([]uint32, 0)
	go sc.CatchApplyMsg()
	return sc
}

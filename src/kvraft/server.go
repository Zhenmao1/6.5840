package kvraft

import (
	"bytes"
	"sync"
	"sync/atomic"
	"time"

	"6.5840/labgob"
	"6.5840/labrpc"
	"6.5840/raft"
)

type Op struct {
	// Your definitions here.
	// Field names must start with capital letters,
	// otherwise RPC will break.
	Option   string
	Key      string
	Value    string
	SeqNum   int
	Index    int
	ClientId int64
}

type KVServer struct {
	mu      sync.Mutex
	me      int
	rf      *raft.Raft
	applyCh chan raft.ApplyMsg
	dead    int32 // set by Kill()

	maxraftstate int // snapshot if log grows this big

	// Your definitions here.
	//lab3A
	dataBase    map[string]string
	waitApplyCh map[int]chan Op //seqNum-Op
	completed   map[int64]int

	//lab3B
	includeIndex int
}

func (kv *KVServer) Get(args *GetArgs, reply *GetReply) {
	// Your code here.
	if kv.killed() {
		reply.Err = ErrWrongLeader
		return
	}

	_, Isleader := kv.rf.GetState()
	if !Isleader {
		reply.Err = ErrWrongLeader
		return
	}
	//判断命令是否重复执行了,不能在这里进行去重检查，应该在handler处，这样命令才同步
	//eg: kvserver收到请求，在raft一致，handler更改maxSeq，没有应用到状态机，自己挂了
	//超时重发至另一个节点，原本这条指令属于重复指令，但现在会被二次执行（新节点的maxseq没有更新
	/* kv.mu.Lock()
	if kv.completed[args.ClientId] >= args.SeqNum {
		DPrintf("server%v 已经执行了这条指令参数为%v", kv.me, args)
		reply.Err = ErrDulplicate
		reply.Value = kv.dataBase[args.Key]
	}
	kv.mu.Unlock() */
	DPrintf("kvserver%v 收到client%v 第Seq%v条指令,key%s向下传递给raft", kv.me, args.ClientId, args.SeqNum, args.Key)
	cmd := Op{
		Option:   "Get",
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
			DPrintf("kvserver%v 向raft发出的op%v，没有正确的返回%v", kv.me, cmd, replyOp)
			reply.Err = ErrWrongLeader
			return
		}
		kv.mu.Lock()
		if ch, ok := kv.waitApplyCh[replyOp.Index]; ok {
			close(ch)
			delete(kv.waitApplyCh, replyOp.Index)
		}
		reply.Err = OK
		reply.Value = kv.dataBase[args.Key]
		DPrintf("kvserver%v get正确执行，返回参数%s", kv.me, reply.Value)
		kv.mu.Unlock()
		return
	case <-time.After(time.Duration(time.Millisecond * 5000)):
		reply.Err = ErrTimeOut
		return
	}

}

func (kv *KVServer) PutAppend(args *PutAppendArgs, reply *PutAppendReply) {
	// Your code here.
	// Your code here.
	if kv.killed() {
		reply.Err = ErrWrongLeader
		return
	}
	//DPrintf("server%v 收到client%v 第Seq%v条指令", kv.me, args.ClientId, args.SeqNum)
	_, Isleader := kv.rf.GetState()
	if !Isleader {
		DPrintf("kvserver%v 收到client第Seq%v条指令，但自己不是leader", kv.me, args.SeqNum)
		reply.Err = ErrWrongLeader
		return
	}
	/* 	//判断命令是否重复执行了
	   	if kv.completed[args.ClientId] >= args.SeqNum {
	   		DPrintf("kvserver%v 已经执行了这条指令参数为%v", kv.me, args)
	   		reply.Err = ErrDulplicate
	   	} */

	DPrintf("kvserver%v 收到client%v 第Seq%v条指令,key%s,value%s向下传递给raft", kv.me, args.ClientId, args.SeqNum, args.Key, args.Value)

	cmd := Op{
		Option:   args.Op,
		Key:      args.Key,
		Value:    args.Value,
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
			DPrintf("kvserver%v 向raft发出的op%v，没有正确的返回%v", kv.me, cmd, replyOp)
			reply.Err = ErrWrongLeader
			return
		}
		reply.Err = OK
		DPrintf("kvserver%v成功添加 key%s value%s 返回参数%v", kv.me, replyOp.Key, replyOp.Value, reply)
		return
	case <-time.After(time.Duration(time.Millisecond * 5000)):
		DPrintf("kvserver%d定时器时间到", kv.me)
		reply.Err = ErrTimeOut
		return
	}
}

func (kv *KVServer) makeWaitApplyCh(index int) chan Op {
	kv.mu.Lock()
	defer kv.mu.Unlock()
	waitCh, ok := kv.waitApplyCh[index]
	if !ok {
		waitCh = make(chan Op, 1)
		kv.waitApplyCh[index] = waitCh
	}
	return waitCh
}

func (kv *KVServer) CatchApplyOp() {
	for {
		if kv.killed() {
			return
		}
		applyMsg := <-kv.applyCh
		DPrintf("kvserver节点%v 从applyCH 收到index%v cmd%v", kv.me, applyMsg.CommandIndex, applyMsg)
		if applyMsg.CommandValid {
			kv.commandHandler(applyMsg)
		}
		if applyMsg.SnapshotValid {
			kv.snapshotHandler(applyMsg)
		}
	}
}

func (kv *KVServer) snapshotHandler(applyMsg raft.ApplyMsg) {
	snapshot := applyMsg.Snapshot
	kv.mu.Lock()
	defer kv.mu.Unlock()
	if applyMsg.SnapshotIndex <= kv.includeIndex {
		DPrintf("KVServer%d收到过期日志，自己的includeIndex%d,收到的includeIndex%d", kv.me, kv.includeIndex, applyMsg.SnapshotIndex)
		return
	}
	kv.snapShotDecode(snapshot)
}

// 加锁使用
func (kv *KVServer) isNeedSnapShot(applyMsg raft.ApplyMsg) {
	if kv.rf.RaftStateSize() >= kv.maxraftstate {
		snapshot := kv.snapShotEncode()
		DPrintf("kvserver%v 日志长度超过限定长度%d,开始日志压缩%v", kv.me, kv.maxraftstate, snapshot)
		kv.rf.Snapshot(applyMsg.CommandIndex, snapshot)
		if kv.includeIndex < applyMsg.CommandIndex {
			kv.includeIndex = applyMsg.CommandIndex
		}
	}
}

// 加锁使用
func (kv *KVServer) snapShotEncode() []byte {
	w := new(bytes.Buffer)
	e := labgob.NewEncoder(w)
	e.Encode(kv.dataBase)
	e.Encode(kv.completed)
	e.Encode(kv.includeIndex)
	data := w.Bytes()
	return data
}

// 加锁使用
func (kv *KVServer) snapShotDecode(snapshot []byte) {
	if snapshot == nil {
		DPrintf("kvserver%d收到快照，但快照为空", kv.me)
		return
	}
	DPrintf("kvserver%d收到快照%v，但快照为空", kv.me, snapshot)
	r := bytes.NewBuffer(snapshot)
	d := labgob.NewDecoder(r)
	var database map[string]string
	var completed map[int64]int
	var includedIndex int
	if d.Decode(&database) != nil || d.Decode(&completed) != nil || d.Decode(&includedIndex) != nil {
		DPrintf("kvserver%d快照解码失败", kv.me)
		return
	}
	kv.dataBase = database
	kv.completed = completed
	if kv.includeIndex < includedIndex {
		kv.includeIndex = includedIndex
	}

	DPrintf("kvserver%d快照解码成功data%v complete%v include%v", kv.me, kv.dataBase, kv.completed, kv.includeIndex)
}

func (kv *KVServer) commandHandler(applyMsg raft.ApplyMsg) {
	kv.mu.Lock()
	defer kv.mu.Unlock()
	op := applyMsg.Command.(Op)

	//所有的kvserver都会拿到op，但只有leader需要返回处理，其余只用应用到状态机
	switch op.Option {
	case "Get":
		op.Value = kv.dataBase[op.Key]
	case "Put":
		if kv.completed[op.ClientId] < op.SeqNum {
			kv.dataBase[op.Key] = op.Value
		}
	case "Append":
		if kv.completed[op.ClientId] < op.SeqNum {
			kv.dataBase[op.Key] += op.Value
		}
	}
	kv.completed[op.ClientId] = op.SeqNum
	//检查是否是leader
	if _, isLeader := kv.rf.GetState(); isLeader {
		kv.isNeedSnapShot(applyMsg)
		waitCh, ok := kv.waitApplyCh[applyMsg.CommandIndex]
		if ok {
			waitCh <- op
		} else {
			DPrintf("kvserver%d检测到raft发出的applymsg，但是返回的通道关闭了%v", kv.me, op)
		}
	}

}

// the tester calls Kill() when a KVServer instance won't
// be needed again. for your convenience, we supply
// code to set rf.dead (without needing a lock),
// and a killed() method to test rf.dead in
// long-running loops. you can also add your own
// code to Kill(). you're not required to do anything
// about this, but it may be convenient (for example)
// to suppress debug output from a Kill()ed instance.
func (kv *KVServer) Kill() {
	atomic.StoreInt32(&kv.dead, 1)
	kv.rf.Kill()
	// Your code here, if desired.
}

func (kv *KVServer) killed() bool {
	z := atomic.LoadInt32(&kv.dead)
	return z == 1
}

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
func StartKVServer(servers []*labrpc.ClientEnd, me int, persister *raft.Persister, maxraftstate int) *KVServer {
	// call labgob.Register on structures you want
	// Go's RPC library to marshall/unmarshall.
	labgob.Register(Op{})

	kv := new(KVServer)
	kv.me = me
	kv.maxraftstate = maxraftstate

	// You may need initialization code here.

	kv.applyCh = make(chan raft.ApplyMsg)
	kv.rf = raft.Make(servers, me, persister, kv.applyCh)

	// You may need initialization code here.

	kv.completed = make(map[int64]int)
	kv.dataBase = make(map[string]string)
	kv.waitApplyCh = make(map[int]chan Op)
	kv.includeIndex = 0
	// 因为可能会crash重连
	snapshot := persister.ReadSnapshot()
	if len(snapshot) > 0 {
		kv.snapShotDecode(snapshot)
	}

	go kv.CatchApplyOp()

	return kv
}

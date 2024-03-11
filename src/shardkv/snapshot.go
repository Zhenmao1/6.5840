package shardkv

import (
	"bytes"

	"6.5840/labgob"
	"6.5840/raft"
)

// 加锁使用
func (kv *ShardKV) snapShotDecode(snapshot []byte) {
	if snapshot == nil {
		DPrintf("kvserver%d收到快照，但快照为空", kv.me)
		return
	}
	DPrintf("kvserver%d收到快照%v，但快照为空", kv.me, snapshot)
	r := bytes.NewBuffer(snapshot)
	d := labgob.NewDecoder(r)
	var shards []Shard
	var completed map[int64]int
	var includedIndex int
	if d.Decode(&shards) != nil || d.Decode(&completed) != nil || d.Decode(&includedIndex) != nil {
		DPrintf("kvserver%d快照解码失败", kv.me)
		return
	}
	kv.shards = shards
	kv.completed = completed
	if kv.includeIndex < includedIndex {
		kv.includeIndex = includedIndex
	}
	DPrintf("kvserver%d快照解码成功data%v complete%v include%v", kv.me, kv.shards, kv.completed, kv.includeIndex)
}

func (kv *ShardKV) snapshotHandler(applyMsg raft.ApplyMsg) {
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
func (kv *ShardKV) isNeedSnapShot(applyMsg raft.ApplyMsg) {
	if kv.rf.RaftStateSize() >= kv.maxraftstate && kv.maxraftstate != -1 {
		snapshot := kv.snapShotEncode()
		DPrintf("kvserver%v 日志长度超过限定长度%d,开始日志压缩%v", kv.me, kv.maxraftstate, snapshot)
		kv.rf.Snapshot(applyMsg.CommandIndex, snapshot)
		if kv.includeIndex < applyMsg.CommandIndex {
			kv.includeIndex = applyMsg.CommandIndex
		}
	}
}

// 加锁使用
func (kv *ShardKV) snapShotEncode() []byte {
	w := new(bytes.Buffer)
	e := labgob.NewEncoder(w)
	e.Encode(kv.shards)
	e.Encode(kv.completed)
	e.Encode(kv.includeIndex)
	data := w.Bytes()
	return data
}

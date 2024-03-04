package raft

import (
	"bytes"

	"6.5840/labgob"
)

// 服务表示已经创建了一个包含所有信息直到（包括）索引的快照。
// 这意味着服务不再需要（包括）该索引之前的日志。
// Raft现在应该尽可能地裁剪它的日志。
func (rf *Raft) Snapshot(index int, snapshot []byte) {
	// 您的代码在这里（2D）。
	//节点假设掉线
	rf.mu.Lock()
	defer rf.mu.Unlock()
	if rf.killed() || rf.role != leader {
		DPrintf("节点 %d 已经掉线或者不是leader，拒绝clerk的快照安装", rf.me)
		return
	}
	DPrintf("节点 %d 开始快照安装", rf.me)
	//要压缩的日志之前已经被压缩
	if rf.includedIndex >= index {
		DPrintf("6666节点%d收到的快照rf.includedIndex%v index%v,要压缩的日志之前已经被压缩", rf.me, rf.includedIndex, index)
		return
	}
	//要压缩的日志之前的日志还没提交到状态机
	if rf.lastApplied < index {
		DPrintf("6666节点%d收到的快照rf.commitindex%v index%v, 日志还没提交到状态机，不能压缩", rf.me, rf.lastApplied, index)
		return
	}
	//开始处理合理的快照，先压缩日志
	compactLogs := make([]entry, 0)
	compactLogs = append(compactLogs, rf.logs[index-rf.includedIndex:]...)

	rf.includedIndex = index
	rf.includedTerm = rf.logs[index-rf.includedIndex].Term

	rf.logs = compactLogs
	DPrintf("6666节点%d最新的压缩日志includeIndex%d term%d ，log%v", rf.me, rf.includedIndex, rf.includedTerm, rf.logs)

	//已经安装了快照，就要立即更新的自己的提交状态
	if rf.commitIndex < index {
		rf.commitIndex = index
	}
	if rf.lastApplied < index {
		rf.lastApplied = index
	}
	//持久化
	w := new(bytes.Buffer)
	e := labgob.NewEncoder(w)
	e.Encode(rf.curTerm)
	e.Encode(rf.voteFor)
	e.Encode(rf.logs)
	e.Encode(rf.includedIndex)
	e.Encode(rf.includedTerm)
	raftState := w.Bytes()
	rf.persister.Save(raftState, snapshot)
	DPrintf("6666节点%d安装了snapshot%v之后，持久化状态和快照", rf.me, rf.persister.ReadRaftState())
	for i := 0; i < len(rf.peers); i++ {
		if i == rf.me {
			continue
		}
		go rf.RequestInstallSnapshot(i)
	}
}

func (rf *Raft) CondInstallSnapshot(lastIncludedTerm int, lastIncludedIndex int, snapshot []byte) bool {
	return true
}

func (rf *Raft) RequestInstallSnapshot(server int) {
	rf.mu.Lock()

	args := RequstSnapshotArgs{
		Term:         rf.curTerm,
		LeaderId:     rf.me,
		IncludeIndex: rf.includedIndex,
		IncludeTerm:  rf.includedTerm,
		Data:         rf.persister.ReadSnapshot(),
	}
	rf.mu.Unlock()
	reply := RequstSnapshotReply{}
	DPrintf("6666leader节点%d 向节点%d发送snapshot%v", rf.me, server, args.Data)
	ok := rf.peers[server].Call("Raft.InstallSnapshot", &args, &reply)
	if ok {
		rf.mu.Lock()
		defer rf.mu.Unlock()
		if reply.Term > rf.curTerm {
			DPrintf("6666leader节点%d 收到节点%dsnapshot返回，但自己的任期不正确，转为follower", rf.me, server)
			rf.setFollower(reply.Term, -1)
			rf.persist()
		} else {
			rf.nextIndex[server] = max(args.IncludeIndex+1, rf.nextIndex[server])
			rf.matchIndex[server] = args.IncludeIndex
			DPrintf("6666leader节点%d 收到节点%dsnapshot安装正确返回,更新后自己的next%v commit%v", rf.me, server, rf.nextIndex, rf.commitIndex)
		}
	} else {
		DPrintf("6666leader节点%d 没有收到节点%dsnapshot安装返回", rf.me, server)
	}

}

func (rf *Raft) InstallSnapshot(args *RequstSnapshotArgs, reply *RequstSnapshotReply) {
	rf.mu.Lock()

	DPrintf("6666节点%d收到的快照rf.includedIndex%v Term%d  index%v term%d snapshot%v", rf.me, rf.includedIndex, rf.includedTerm, args.IncludeIndex, args.IncludeTerm, args.Data)
	//任期不对，立即返回
	reply.Term = rf.curTerm
	if rf.curTerm > args.Term {
		rf.mu.Unlock()
		return
	}
	//任期正确,重置状态
	rf.setFollower(args.Term, args.LeaderId)
	//快照命令过期
	if rf.includedIndex >= args.IncludeIndex {
		rf.mu.Unlock()
		return
	}

	//开始处理合理的快照，先压缩日志
	compactLogs := make([]entry, 0)
	if len(rf.logs) > args.IncludeIndex-rf.includedIndex {
		compactLogs = append(compactLogs, rf.logs[args.IncludeIndex-rf.includedIndex:]...)
	}
	rf.logs = compactLogs

	rf.includedIndex = args.IncludeIndex
	rf.includedTerm = args.IncludeTerm
	DPrintf("6666节点%d最新的压缩日志includeIndex%d，log%v", rf.me, rf.includedIndex, rf.logs)

	//已经安装了快照，就要立即更新的自己的提交状态
	if rf.commitIndex < args.IncludeIndex {
		rf.commitIndex = args.IncludeIndex
	}
	if rf.lastApplied < args.IncludeIndex {
		rf.lastApplied = args.IncludeIndex
	}

	rf.mu.Unlock()
	rf.applyCh <- ApplyMsg{CommandValid: false, SnapshotValid: true,
		Snapshot: args.Data, SnapshotIndex: args.IncludeIndex, SnapshotTerm: args.IncludeTerm}

	//持久化
	w := new(bytes.Buffer)
	e := labgob.NewEncoder(w)
	e.Encode(rf.curTerm)
	e.Encode(rf.voteFor)
	e.Encode(rf.logs)
	e.Encode(rf.includedIndex)
	e.Encode(rf.includedTerm)
	raftState := w.Bytes()
	rf.persister.Save(raftState, args.Data)
	DPrintf("6666节点%d安装了snapshot%v之后，持久化状态和快照", rf.me, rf.persister.ReadSnapshot())

}

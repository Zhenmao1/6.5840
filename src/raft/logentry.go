package raft

import (
	"math"
	"sort"
	"time"
)

func IsEmpty(args AppendEntryArgs) bool {
	if args.Entries == nil || len(args.Entries) == 0 {
		return true
	}
	return false
}

func (rf *Raft) ConstructAppendArgs(server int) (*AppendEntryArgs, *AppendEntryReply) {
	rf.mu.Lock()
	//DPrintf("3333节点 %d 向 节点 %d 发起心跳 term%d", rf.me, server, rf.curTerm)
	args := AppendEntryArgs{
		Term:         rf.curTerm,
		LeaderId:     rf.me,
		LeaderCommit: rf.commitIndex,
	}
	args.PrevLogIndex, args.PrevLogTerm = rf.getPrevLogIndex(server)
	//假设有新的日志要发送
	if len(rf.logs)+rf.includedIndex >= rf.nextIndex[server] {
		args.Entries = rf.logs[rf.nextIndex[server]-1-rf.includedIndex:]
		DPrintf("4444节点 %d 向节点%d发出日志,参数term id preInd preTerm commit为%v", rf.me, server, args)
	} else {
		//没有日志需要发送，但是可能会rollback follower的log
		DPrintf("3333节点 %d log%v 向节点 %d 参数%v 发起心跳nextindex%v", rf.me, rf.logs, server, args, rf.nextIndex[server])
	}
	reply := AppendEntryReply{NextIndex: -2}
	rf.mu.Unlock()
	return &args, &reply
}
func (rf *Raft) Heartbeat() {
	rf.mu.Lock()
	rf.resetHeartTime()
	rf.mu.Unlock()
	for i := 0; i < len(rf.peers); i++ {
		if rf.killed() {
			return
		}
		if i != rf.me {
			go func(server int) {
				//在发送日志或者心跳之前，可能会先发起快照
				//构造请求参数
				rf.mu.Lock()
				next := rf.nextIndex[server] - 1
				role := rf.role
				rf.mu.Unlock()
				if role != leader {
					return
				}
				if next < rf.includedIndex {
					rf.RequestInstallSnapshot(server)
					return
				}
				args, reply := rf.ConstructAppendArgs(server)
				ok := rf.SendAppendLogEntry(server, args, reply)
				if !ok {
					DPrintf("4444节点 %d 向节点%d发出日志或者心跳失败,节点断链%d", rf.me, server, server)
				} else {
					// 日志返回
					if len(args.Entries) > 0 {
						rf.AppendEntryHandler(server, args, reply)
					} else {
						//是心跳返回
						rf.heartBeatHandler(server, args, reply)
					}
				}

			}(i)
		}
	}

}

// 心跳返回的处理函数
func (rf *Raft) heartBeatHandler(server int, args *AppendEntryArgs, reply *AppendEntryReply) {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	if !reply.Success {
		if reply.Term > args.Term {
			DPrintf("3333节点 %d 在心跳返回后得知不是最新任期，重置选举时间,重置状态持久化", args.LeaderId)
			rf.resetElectionTimeout()
			rf.curTerm = reply.Term
			rf.role = follower
			rf.voteFor = -1
			rf.persist()
			return
		} else {
			rf.nextIndex[server] = reply.NextIndex
			DPrintf("3333节点 %d 在发起心跳后没有收到节点%d正确返回,返回参数%v,next为%v", args.LeaderId, server, reply, rf.nextIndex)
		}
	} else {
		//返回成功,更新最新的对应节点的匹配日志
		if reply.NextIndex >= rf.nextIndex[server] {
			rf.nextIndex[server] = reply.NextIndex + 1
		}
		DPrintf("3333节点 %d 收到节点%d的正确心跳返回,参数%v next%d", args.LeaderId, server, args, rf.nextIndex[server])
	}
}
func (rf *Raft) AppendEntryHandler(server int, args *AppendEntryArgs, reply *AppendEntryReply) {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	if reply.Success {
		rf.nextIndex[server] = reply.NextIndex + 1
		rf.matchIndex[server] = min(args.PrevLogIndex+len(args.Entries), rf.nextIndex[server]-1)
		DPrintf("4444节点 %d 发出日志收到节点%d正确返回,next %v,match %v ,尝试提交", args.LeaderId,
			server, rf.nextIndex, rf.matchIndex)
		rf.tryUpdateCommitIndex()
	} else {
		//DPrintf("4444节点 %d 发出日志收到节点%d错误返回 reply%v", args.LeaderId, server, reply)
		if reply.Term > rf.curTerm {
			rf.role = follower
			rf.curTerm = reply.Term
			rf.voteFor = -1
			rf.resetElectionTimeout()
			rf.persist()
			DPrintf("1111节点 %d 发出日志收到节点%d错误返回,转为follower reply%v 持久化", args.LeaderId, server, reply)
			return
		}
		if reply.NextIndex >= 0 {
			rf.nextIndex[server] = reply.NextIndex
			DPrintf("4444节点 %d 把节点%v的nextindex改为%v", args.LeaderId, server, reply.NextIndex)
		}
		//rf.matchIndex[server] = reply.NextIndex - 1
	}

}

// 假设这是当领导者接收到跟随者的响应时调用的方法
// 这个方法尝试更新commitIndex
func (rf *Raft) tryUpdateCommitIndex() {
	// 创建matchIndex的副本并排序，以便找到大多数节点已复制的最小日志索引
	matches := make([]int, len(rf.matchIndex))
	copy(matches, rf.matchIndex)
	DPrintf("matche的所有下标 %v", matches)
	sort.Ints(matches)
	// 在Raft中，大多数节点的定义取决于集群的大小
	// 这里为简化，我们假设集群有5个节点，所以需要3个节点的多数
	majorityIndex := matches[len(matches)/2+1]
	// 检查majorityIndex对应的日志条目的任期是否等于当前任期
	// 并且确保majorityIndex大于当前的commitIndex
	if majorityIndex > rf.commitIndex && rf.logs[majorityIndex-rf.includedIndex-1].Term == rf.curTerm {
		// 更新commitIndex为新的majorityIndex
		rf.commitIndex = majorityIndex
		DPrintf("commitindex 更新为 %d\n", rf.commitIndex)
		// 这里可以添加将日志应用到状态机的逻辑
	}
}

func (rf *Raft) resetHeartTime() {
	rf.heartBeatTime = time.Now()
}

func (rf *Raft) SendAppendLogEntry(server int, args *AppendEntryArgs, reply *AppendEntryReply) bool {
	ok := rf.peers[server].Call("Raft.AppendLogEntry", args, reply)
	return ok
}

func (rf *Raft) setFollower(Term int, leaderId int) {
	rf.role = follower
	rf.curTerm = Term
	rf.voteFor = leaderId
	rf.resetElectionTimeout()
}
func (rf *Raft) AppendLogEntry(args *AppendEntryArgs, reply *AppendEntryReply) {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	//5.1收到任期不对的心跳或者日志
	if rf.curTerm > args.Term {
		reply.Term = rf.curTerm
		if len(args.Entries) > 0 {
			DPrintf("4444 节点%d 收到leader%d 日志的log %v,但是leader过期", rf.me, args.LeaderId, args.Entries)
		} else {
			DPrintf("1111节点%d, 收到leader%d 心跳，但是leader过期 ", rf.me, args.LeaderId)
		}
		return
	}
	//任期对的，leader合理，此时先转变角色
	rf.setFollower(args.Term, args.LeaderId)
	rf.persist()
	reply.Term = rf.curTerm
	//匹配日志
	//
	//不会出现自己的日志为空但对面节点日志不空的情况（选举不通过
	//假设是刚选出来的leader挂了重连也不行，会在发送心跳的term检查出挂掉
	//日志冲突的原因 1缺少日志 (len(rf.logs) > 0 && len(rf.logs) < args.PrevLogIndex)
	// 2下标处任期不符rf.logs[args.PrevLogIndex-1].Term != args.PrevLogTerm其中1已经确保不会缺少日志
	if args.PrevLogIndex > 0 && rf.getLastIndex() > 0 && (rf.getLastIndex() < args.PrevLogIndex ||
		rf.getSnapedLogTerm(args.PrevLogIndex) != args.PrevLogTerm) {
		reply.NextIndex = rf.lastApplied + 1
		if len(args.Entries) > 0 {
			DPrintf("4444 节点%d preIndex%d Term%d 收到leader%d 日志的参数 %v,但是当前日志缺失或者prev不匹配", rf.me, rf.getLastIndex(), rf.getLastTerm(), args.LeaderId, args)
		} else {
			DPrintf("1111节点%d, 收到leader%d 心跳，但是当前心跳后发现日志缺失或者prev不匹配，参数%v,返回参数%v ", rf.me, args.LeaderId, args, reply)
		}
		return
	}

	//pre日志匹配，但即将添加的日志term不正确   && rf.logs[args.PrevLogIndex].Term != args.Entries[0].Term
	if rf.getLastIndex() > args.PrevLogIndex {
		DPrintf("server %v 的log长度%v与参数term%d id%d preInd%d preTerm%d commit%d 发生冲突, 进行移除,并更改持久化\n", rf.me, len(rf.logs),
			args.Term, args.LeaderId, args.PrevLogIndex, args.PrevLogTerm, args.LeaderCommit)

		rf.logs = rf.logs[:args.PrevLogIndex-rf.includedIndex]
		rf.persist()
	}
	if len(args.Entries) > 0 {
		rf.logs = append(rf.logs, args.Entries[:]...)
		rf.persist()
		DPrintf("server %v 成功进行apeend log长度%v，持久化\n", rf.me, (rf.logs))
	}
	reply.Success = true
	reply.NextIndex = len(rf.logs) + rf.includedIndex
	if args.LeaderCommit > rf.commitIndex {
		rf.commitIndex = int(math.Min(float64(args.LeaderCommit), float64(len(rf.logs)+rf.includedIndex)))
		DPrintf("节点 %v commitindex%v\n", rf.me, rf.commitIndex)
	}
	if len(args.Entries) == 0 {
		DPrintf("节点%d, 收到leader%d 正确心跳，参数%v,返回参数%v ", rf.me, args.LeaderId, args, reply)
	}
}

func (rf *Raft) heartTimePast() bool {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	return time.Since(rf.heartBeatTime) > time.Duration(baseheartBeatTime)*time.Millisecond
}

func (rf *Raft) isNeedAppendEntry() {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	if rf.commitIndex < len(rf.logs)+rf.includedIndex {
		DPrintf("4444 leader节点 %d 检测到需要发送日志 log %v 实际下标为index:%d", rf.me, rf.logs, rf.lastApplied)
		go rf.Heartbeat()
	}
}

func (rf *Raft) checkCommit() {
	for !rf.killed() {
		time.Sleep(commitInterval)
		rf.mu.Lock()
		//DPrintf("5555节点 %d commiter定时器到 commit%v,lastapplied%v", rf.me, rf.commitIndex, rf.lastApplied)
		if rf.lastApplied < rf.commitIndex {
			msg := ApplyMsg{CommandValid: true, Command: rf.logs[rf.lastApplied-rf.includedIndex].Command, CommandIndex: rf.lastApplied + 1}
			DPrintf("5555节点 %d 开始提交%v", rf.me, msg)
			rf.lastApplied++
			rf.mu.Unlock()
			rf.applyCh <- msg
			DPrintf("5555节点 %d lastApplied%v", rf.me, rf.lastApplied)
		} else {
			rf.mu.Unlock()
		}
	}
}

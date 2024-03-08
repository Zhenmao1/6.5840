package raft

import (
	"math/rand"
	"time"
)

func (rf *Raft) electionTimePast() bool {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	return time.Since(rf.electionTime) > rf.electionTimeout
}

// 加锁
func (rf *Raft) resetElectionTimeout() {
	rf.electionTimeout = time.Duration(((rand.Int63() % (baseElectionTime / 2)) + baseElectionTime)) * time.Millisecond
	rf.electionTime = time.Now()
	DPrintf("0000节点%d重置选举时间", rf.me)
}
func (rf *Raft) election() {
	rf.mu.Lock()
	if rf.role == leader {
		return
	}
	rf.curTerm++
	rf.voteFor = rf.me
	rf.role = candidate
	rf.persist()
	DPrintf("1111节点%d, 开始选举 %d 并写入持久化", rf.me, rf.curTerm)
	args := RequestVoteArgs{
		Term:         rf.curTerm,
		CandidateId:  rf.me,
		LastLogIndex: 0,
		LasstLogTerm: 0,
	}

	args.LastLogIndex = rf.getLastIndex()
	args.LasstLogTerm = rf.getLastTerm()

	rf.mu.Unlock()
	countVote := 0

	for i := 0; i < len(rf.peers); i++ {
		if i != rf.me {
			go func(server int) {
				DPrintf("1111节点 %d 向 节点 %d 发起求票 term%d", rf.me, server, args.Term)
				reply := RequestVoteReply{}
				ok := rf.sendRequestVote(server, &args, &reply)
				//如果发起请票的不是任期最大的，变更到follower的状态
				if ok {
					DPrintf("1111节点 %d 收到节点 %d 在term%d 的投票结果%v", args.CandidateId, server, args.Term, reply.VoteGranted)
				} else {
					DPrintf("1111节点 %d 没有收到节点 %d 在term%d 的反票", args.CandidateId, server, args.Term)
				}

				rf.mu.Lock()
				defer rf.mu.Unlock()
				if reply.Term > rf.curTerm {
					rf.curTerm = reply.Term
					DPrintf("1111节点%d, 反票表明对方任期%d更大, 变为follower，重置选举时间,持久化 ", rf.me, reply.Term)
					rf.role = follower
					rf.voteFor = -1
					rf.persist()
					rf.resetElectionTimeout()
					return
				}
				if reply.VoteGranted {
					countVote++
					if countVote == len(rf.peers)/2 {
						rf.RoleChange()
						//立即开始心跳
						rf.persist()
						for i := 0; i < len(rf.nextIndex); i++ {
							if i != rf.me {
								rf.nextIndex[i] = rf.getLastIndex() + 1
								rf.matchIndex[i] = 0
							}
						}
						go rf.Heartbeat()
					}
					return
				}
			}(i)
		}
	}
}

func (rf *Raft) sendRequestVote(server int, args *RequestVoteArgs, reply *RequestVoteReply) bool {
	ok := rf.peers[server].Call("Raft.RequestVote", args, reply)
	return ok
}

// 示例RequestVote RPC处理程序。
func (rf *Raft) RequestVote(args *RequestVoteArgs, reply *RequestVoteReply) {
	if rf.killed() {
		return
	}
	rf.mu.Lock()
	defer rf.mu.Unlock()
	// 您的代码在这里（2A, 2B）。
	//DPrintf("2222节点 %d 收到节点 %d 求票", rf.me, args.CandidateId)
	//任期不符合要求
	if args.Term <= rf.curTerm {
		reply.Term = rf.curTerm
		reply.VoteGranted = false
		DPrintf("2222请求节点 %d 的任期%d 小于当前节点%d的term%d,拒绝投票", args.CandidateId, args.Term, rf.me, rf.curTerm)
		return
	}
	//任期大于等于发起者的term,先把自己的角色转变为follower,还原自身的状态
	if args.Term > rf.curTerm {
		DPrintf("2222当前节点 %d 的任期%d 小于求票节点%d的term%d,转为follower,并持久化", rf.me, rf.curTerm, args.CandidateId, args.Term)
		rf.voteFor = -1
		rf.curTerm = args.Term
		rf.role = follower
		rf.persist()
	}

	reply.Term = rf.curTerm

	//进入投票，此时节点的term一定等于发起请求的节点的term一样
	//假如没有投过票
	if rf.voteFor == -1 || rf.voteFor == args.CandidateId {
		//计算当前节点的最后日志情况
		curLastlogIndex := rf.getLastIndex()
		curLastlogTerm := rf.getLastTerm()
		//如果对方日志更长或者日志一样长，但最终的日志term更新，投票?????逻辑正确吗
		//查看最后日志的任期，任期更大认为日志合理，相同看条数？？？？逻辑正确吗
		DPrintf("2222节点 %d 任期为%d，curlastlogindex%d Term%d  argsindex%d Term%d", rf.me, rf.curTerm, curLastlogIndex,
			curLastlogTerm, args.LastLogIndex, args.LasstLogTerm)
		if (curLastlogTerm < args.LasstLogTerm) || ((curLastlogTerm == args.LasstLogTerm) && (curLastlogIndex <= args.LastLogIndex)) {
			reply.VoteGranted = true
			//投出同意票之后，更新自己的选举时间，记录投票的节点
			DPrintf("2222节点 %d 向%d 投同意票，term%d votefor可能变化，持久化", rf.me, args.CandidateId, args.Term)
			rf.persist()
			rf.voteFor = args.CandidateId
			rf.electionTimeout = time.Duration(((rand.Int63() % (baseElectionTime / 3)) + baseElectionTime)) * time.Millisecond
			rf.electionTime = time.Now()
			DPrintf("0000节点 %d 投出同意后，重置选举时间", rf.me)
		} else {
			reply.VoteGranted = false
			DPrintf("2222节点 %d 向投出%d 投拒绝票，任期为%d，curlastlogindex%d Term%d  argsindex%d Term%d", rf.me, args.CandidateId, args.Term, curLastlogIndex,
				curLastlogTerm, args.LastLogIndex, args.LasstLogTerm)
		}
	} else {
		reply.VoteGranted = false
		DPrintf("2222节点 %d 向投出%d 投拒绝票，原因投过票，任期为%d,投给了%d", rf.me, args.CandidateId, args.Term, rf.voteFor)
	}

}

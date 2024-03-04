package raft

//
// 这是Raft必须向服务（或测试器）公开的API大纲。有关更多详细信息，请参见下面
// 每个这些函数的注释。
//
// rf = Make(...)
//   创建一个新的Raft服务器。
// rf.Start(command interface{}) (index, term, isleader)
//   开始就一个新的日志条目达成一致
// rf.GetState() (term, isLeader)
//   请求Raft当前的任期，以及它是否认为自己是领导者
// ApplyMsg
//   每当新条目被提交到日志中，每个Raft对等节点
//   应该通过传递给Make()的applyCh向服务（或测试器）
//   发送一个ApplyMsg，以在同一服务器上发送。
//

import (
	//	"bytes"

	"math/rand"
	"sync"
	"time"

	//	"6.5840/labgob"

	"6.5840/labrpc"
)

// 当每个Raft对等节点意识到连续的日志条目已经
// 被提交，该节点应该通过传递给Make()的applyCh向服务（或
// 测试器）发送一个ApplyMsg，设置CommandValid为true表示
// ApplyMsg包含一个新提交的日志条目。
//
// 在第2D部分，您可能希望发送其他类型的消息（例如，
// 快照）到applyCh，但是对于这些其他用途，将CommandValid设置为false。
type ApplyMsg struct {
	CommandValid bool
	Command      interface{}
	CommandIndex int

	// 对于2D：
	SnapshotValid bool
	Snapshot      []byte
	SnapshotTerm  int
	SnapshotIndex int
}

type RequstSnapshotArgs struct {
	Term         int
	LeaderId     int
	IncludeIndex int
	IncludeTerm  int
	Data         []byte
}
type RequstSnapshotReply struct {
	Term int
}

const (
	commitInterval = time.Duration(100) * time.Millisecond
)

type role int

const (
	follower = iota
	candidate
	leader
)

type AppendEntryArgs struct {
	Term         int
	LeaderId     int
	PrevLogIndex int
	PrevLogTerm  int
	Entries      []entry
	LeaderCommit int
}
type AppendEntryReply struct {
	Term      int
	Success   bool
	NextIndex int
}

type entry struct {
	Command interface{}
	Term    int
}

const baseElectionTime = 300
const baseheartBeatTime = 110

// 实现单个Raft对等节点的Go对象。
type Raft struct {
	mu        sync.Mutex          // 用于保护对这个对等节点状态的共享访问的锁
	peers     []*labrpc.ClientEnd // 所有对等节点的RPC端点
	persister *Persister          // 用于保存这个对等节点持久状态的对象
	me        int                 // 这个对等节点在peers[]中的索引
	dead      int32               // 由Kill()设置为1

	// 您的数据在这里（2A, 2B, 2C）。
	// 查看论文的图2，了解Raft服务器必须维护的状态描述。
	//持久化数据
	curTerm int
	voteFor int
	logs    []entry
	//非持久
	role        role
	commitIndex int
	lastApplied int
	//对于leader
	nextIndex  []int
	matchIndex []int

	//对于选举和心跳
	electionTimeout time.Duration
	electionTime    time.Time
	heartBeatTime   time.Time

	//对于日志提交到applych
	applyCh chan ApplyMsg

	//对于2D
	includedIndex int
	includedTerm  int
}

// 示例RequestVote RPC参数结构。
// 字段名称必须以大写字母开头！
type RequestVoteArgs struct {
	// 您的数据在这里（2A, 2B）。
	Term         int
	CandidateId  int
	LastLogIndex int
	LasstLogTerm int
}

// 示例RequestVote RPC回复结构。
// 字段名称必须以大写字母开头！
type RequestVoteReply struct {
	// 您的数据在这里（2A）。
	Term        int
	VoteGranted bool
}

// 示例代码发送一个RequestVote RPC到一个服务器。
// 服务器是rf.peers[]中目标服务器的索引。
// 期望在args中传递RPC参数。
// 填充*reply与RPC回复，所以调用者应该
// 传递&reply。
// 传递给Call()的args和reply的类型必须是
// 与处理程序函数中声明的参数的类型相同（包括它们是否为指针）。
//
// labrpc包模拟了一个有丢失的网络，其中服务器
// 可能无法访问，并且可能会丢失请求和回复。
// Call()发送一个请求并等待回复。如果在超时时间内收到了回复，
// Call()返回true；否则，Call()返回false。因此，Call()可能不会立即返回。
// 一个false的返回可以由死服务器、无法访问的活动服务器、丢失的请求或丢失的回复引起。
//
// 保证Call()会返回（也许会有延迟），除非
// 服务器端的处理函数不返回。因此有
// 不需要在Call()周围实现自己的超时。
//
// 要是您无法使RPC正常工作，请检查您是否
// 在通过RPC传递的结构体中大写了所有字段名称，并且
// 调用者使用&而不是结构体本身传递回复结构的地址。

// 服务或测试器想要创建一个Raft服务器。所有Raft服务器（包括这一个）
// 的端口都在peers[]中。此服务器的端口是peers[me]。所有服务器的peers[]数组
// 顺序相同。persister是一个地方，用于这个服务器保存它的持久状态，
// 并且最初保存了最近保存的状态（如果有的话）。applyCh是一个通道，
// 测试器或服务期望Raft发送ApplyMsg消息到该通道。
// Make()必须快速返回，因此它应该为任何长时间运行的工作启动goroutines。
func Make(peers []*labrpc.ClientEnd, me int,
	persister *Persister, applyCh chan ApplyMsg) *Raft {
	rf := &Raft{}
	rf.peers = peers
	rf.persister = persister
	rf.me = me
	//持久化变量初始化
	rf.curTerm = 0
	rf.voteFor = -1
	rf.role = follower
	//选举和日志变量初始化
	rf.commitIndex = 0
	rf.lastApplied = 0
	//初始化选举间隔时间和心跳间隔时间
	rf.electionTime = time.Now()
	rf.electionTimeout = time.Duration((rand.Intn(100) + baseElectionTime)) * (time.Millisecond)
	rf.heartBeatTime = time.Now()
	//日志压缩部分的代码
	rf.includedIndex = 0
	rf.includedTerm = 0
	// 您的初始化代码在这里（2A, 2B, 2C）。
	rf.nextIndex = make([]int, len(peers))
	for i := range rf.nextIndex {
		rf.nextIndex[i] = 1
	}
	rf.matchIndex = make([]int, len(peers))
	// 从崩溃前持久状态初始化
	rf.readPersist(persister.ReadRaftState())
	rf.applyCh = applyCh
	// 启动ticker goroutine来开始选举
	go rf.ticker()
	go rf.checkCommit()

	return rf
}

// 需要加锁才能使用
func (rf *Raft) RoleChange() {
	rf.role = leader
	DPrintf("0000节点 %d 转为leader", rf.me)
}

func (rf *Raft) ticker() {
	for !rf.killed() {

		// 您的代码在这里（2A）
		// raft的守护进程，50ms检查一次是否需要进行role切换并执行相应的操作
		//follower、candidate 可能会发起选举
		//leader会发起 心跳或者日志
		ms := 50
		time.Sleep(time.Duration(ms) * time.Millisecond)
		rf.mu.Lock()
		role := rf.role
		rf.mu.Unlock()
		switch role {
		case follower:
			fallthrough
		case candidate:
			if rf.electionTimePast() {
				rf.mu.Lock()
				rf.resetElectionTimeout()
				DPrintf("0000节点 %d 在term%d选举超时后重置选举时间", rf.me, rf.curTerm)
				rf.mu.Unlock()
				rf.election()
			}
		case leader:
			//心跳时间到
			if rf.heartTimePast() {
				go rf.Heartbeat()
			}
			go rf.isNeedAppendEntry()
			//假设有新的日志要发送

		}
		//DPrintf("rf.me %d rf的定时器进程%d", rf.me, rf.curTerm)
	}
}

// 服务使用Raft（例如k/v服务器）想要启动
// 对下一个要追加到Raft日志的命令达成一致。如果这个
// 服务器不是领导者，则返回false。否则，开始协议并立即返回。
// 没有保证这个命令将被提交到Raft日志，因为领导者
// 可能会失败或失去选举。即使Raft实例已被关闭，
// 这个函数也应该优雅地返回。
//
// 第一个返回值是命令将出现的索引
// 如果它被提交。第二个返回值是当前的任期。
// 第三个返回值为true，如果该服务器认为自己是领导者。
func (rf *Raft) Start(command interface{}) (int, int, bool) {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	index := -1
	isLeader := false
	term := rf.curTerm
	if rf.role != leader {
		return index, term, isLeader
	}
	isLeader = true
	rf.logs = append(rf.logs, entry{Command: command, Term: rf.curTerm})
	index = len(rf.logs) + rf.includedIndex
	rf.persist()
	DPrintf("4444 leader节点 %d 新增一条log %v,并写入持久化", rf.me, rf.logs[len(rf.logs)-1])
	return index, term, isLeader
}

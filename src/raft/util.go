package raft

import (
	"bytes"
	"log"
	"sync/atomic"

	"6.5840/labgob"
)

// Debugging
const Debug = true

func DPrintf(format string, a ...interface{}) (n int, err error) {
	if Debug {
		log.Printf(format, a...)
	}
	return
}

// 加锁使用，获得有快照的最后日志的下标
func (rf *Raft) getLastIndex() int {
	return len(rf.logs) + rf.includedIndex
}

// 加锁使用，获得有快照的最后日志的term
func (rf *Raft) getLastTerm() int {
	if len(rf.logs) == 0 {
		return rf.includedTerm
	} else {
		return rf.logs[len(rf.logs)-1].Term
	}
}

// 加锁使用，获得有快照的某条日志
func (rf *Raft) getSnapedLogTerm(index int) int {
	if index == rf.includedIndex {
		return rf.includedTerm
	} else if index > len(rf.logs)+rf.includedIndex {
		DPrintf("要获取的日志index%d不合理，目前最长的index%d", index, len(rf.logs)+rf.includedIndex)
		return -1
	} else {
		return rf.logs[index-rf.includedIndex-1].Term
	}
}

func max(a, b int) int {
	if a > b {
		return a
	}
	return b
}

func min(a, b int) int {
	if a > b {
		return b
	}
	return a
}

// 加锁使用，获得有快照的最后一条日志的下标和任期
// preLog是根据nextIndex找到的，但是直接next可能找不到
func (rf *Raft) getPrevLogIndex(server int) (int, int) {
	//prevLogIndex是第几条要匹配的项目
	prevLogIndex := rf.nextIndex[server] - 1
	//初始情况和刚刚压缩完所有日志的情况（不能通过log找到
	if prevLogIndex-rf.includedIndex == 0 {
		return rf.includedIndex, rf.includedTerm
	} else {
		//有日志
		if prevLogIndex-rf.includedIndex-1 < 0 {
			DPrintf("获取prev信息错误 next%d include%d", rf.nextIndex[server], rf.includedIndex)
		}

		prevLogTerm := rf.logs[prevLogIndex-rf.includedIndex-1].Term

		return prevLogIndex, prevLogTerm
	}

}

// 返回当前任期和该服务器是否认为自己是领导者。
func (rf *Raft) GetState() (int, bool) {
	// 您的代码在这里（2A）。
	rf.mu.Lock()
	term := rf.curTerm
	role := rf.role
	rf.mu.Unlock()
	return term, role == leader
}

// 测试器不会在每个测试后停止Raft创建的goroutines，
// 但它确实调用了Kill()方法。您的代码可以使用killed()来
// 检查是否调用了Kill()。使用atomic可以避免锁的需要。
//
// 问题是长时间运行的goroutines使用内存并可能消耗
// CPU时间，这可能导致后续测试失败并生成
// 令人困惑的调试输出。任何具有长时间循环的goroutine
// 应该调用killed()来检查是否应该停止。
func (rf *Raft) Kill() {
	atomic.StoreInt32(&rf.dead, 1)
	// 您的代码在这里，如果需要的话。
}

func (rf *Raft) killed() bool {
	z := atomic.LoadInt32(&rf.dead)
	return z == 1
}

// 将Raft的持久状态保存到稳定存储中，
// 在崩溃和重启之后可以检索到。参见论文的图2，了解应该持久保存的内容。
// 在您实现快照之前，您应该将nil作为第二个参数传递给persister.Save()。
// 在您实现快照之后，传递当前快照（如果尚未有快照，则为nil）。
func (rf *Raft) persist() {
	w := new(bytes.Buffer)
	e := labgob.NewEncoder(w)
	e.Encode(rf.curTerm)
	e.Encode(rf.voteFor)
	e.Encode(rf.logs)
	e.Encode(rf.includedIndex)
	e.Encode(rf.includedTerm)
	raftState := w.Bytes()
	rf.persister.Save(raftState, nil)
	//DPrintf("persister存储%v\n", raftState)
	//DPrintf("节点%d存储lastapply%d和commitindex%d term%d log%v", rf.me, rf.lastApplied, rf.commitIndex, rf.curTerm, rf.logs)
	//存了立即取出来读一下
	//rf.readPersist(rf.persister.raftstate)
}

// 恢复以前持久化的状态。
// 不能加锁，不管是持久化还是解持久化
func (rf *Raft) readPersist(data []byte) {
	if data == nil || len(data) < 1 { // 没有任何状态的引导？
		return
	}
	r := bytes.NewBuffer(data)
	d := labgob.NewDecoder(r)

	var curTerm int
	var voteFor int
	var logs []entry
	var includedTerm int
	var includedIndex int
	if d.Decode(&curTerm) != nil || d.Decode(&voteFor) != nil || d.Decode(&logs) != nil ||
		d.Decode(&includedIndex) != nil || d.Decode(&includedTerm) != nil {
		DPrintf("解码错误！")
	} else {
		rf.curTerm = curTerm
		rf.voteFor = voteFor
		rf.logs = logs
		rf.includedIndex = includedIndex
		rf.includedTerm = includedTerm
		//防止重复提交日志
		rf.lastApplied = includedIndex
		rf.commitIndex = includedIndex
		DPrintf("节点%d重新加载lastapply%d和commitindex%d term%d log%v", rf.me, rf.lastApplied, rf.commitIndex, rf.curTerm, rf.logs)
		copy(rf.logs, logs)
	}
}

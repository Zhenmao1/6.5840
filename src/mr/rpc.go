package mr

//
// RPC definitions.
//
// remember to capitalize all names.
//

import (
	"os"
	"strconv"
	"time"
)

//
// example to show how to declare the arguments
// and reply for an RPC.
//

type TaskType int

const (
	MapTask = iota
	ReduceTask
	EmptyTask
	Break
)

type TaskPhase int

const (
	MapPhase = iota
	ReducePhase
	Stop
)

type TaskState int

const (
	Waiting = iota
	Running
	Done
)

type Task struct {
	TaskName TaskType
	//可能不止分配一个文件
	FileName []string

	//中间结果文件"mr-tmp-x-y",所以需要两个id
	//给出workerID，是map的话taskId对应x，reduce对应y**************修正，这里是任务id用来反馈哪个任务
	TaskId int
	//一共有多少个reducer
	ReduceNum int
}

type TaskInfo struct {
	StartTime time.Time
	TaskState TaskState
	Task      *Task
}

type Args struct {
	RandIdnex int
	TaskId    int
}

// Add your RPC definitions here.

// Cook up a unique-ish UNIX-domain socket name
// in /var/tmp, for the coordinator.
// Can't use the current directory since
// Athena AFS doesn't support UNIX-domain sockets.
func coordinatorSock() string {
	s := "/var/tmp/5840-mr-"
	s += strconv.Itoa(os.Getuid())
	return s
}

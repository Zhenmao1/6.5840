package mr

import (
	"fmt"
	"log"
	"net"
	"net/http"
	"net/rpc"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"sync"
	"time"
)

type Coordinator struct {
	// 这里定义协调器的属性
	//两个管道做任务管理
	MapTaskChan    chan *Task
	ReduceTaskChan chan *Task
	MapNum         int
	ReduceNum      int
	TaskId         int //用来记录当前的任务编号，产生任务后自加

	//新增加阶段，worker据此拿取不同的任务(map/reduce),借用了map/reduce Task
	TaskPhase TaskPhase
	//新增加管理字段，用来管理所有的Tasks,使用切片、还是map、还是LRU呢？
	//最好使用map，分发给worker的有一个id，任务完成返回任务id，管理器根据id快速找到任务的元数据
	Mu             sync.Mutex // 用于保证有些操作之间互斥
	TasksMetainfos map[int]*TaskInfo
}

func (c *Coordinator) MakeMapTasks(files []string) {
	for _, filename := range files {
		//
		c.Mu.Lock()
		taskid := c.TaskId
		c.TaskId++
		c.Mu.Unlock()
		task := Task{
			TaskName:  MapTask,
			TaskId:    taskid,
			FileName:  []string{filename},
			ReduceNum: c.ReduceNum,
		}
		c.PutTask(&task)
	}
}
func (c *Coordinator) MakeReduceTasks() {
	c.Mu.Lock()
	defer c.Mu.Unlock()
	//reduce任务需要重新开始计数
	c.TaskId = 0

	for i := 0; i < c.ReduceNum; i++ {
		// 使用 Glob 函数匹配文件名格式
		filename := "mr-*-" + strconv.Itoa(c.TaskId) + ".json"
		files, err := filepath.Glob(filename)
		if err != nil {
			fmt.Println("Error:", err)
			return
		}
		taskid := c.TaskId
		c.TaskId++
		task := Task{
			TaskName:  ReduceTask,
			TaskId:    taskid,
			FileName:  files,
			ReduceNum: c.ReduceNum,
		}
		c.PutTask(&task)
	}
}

func (c *Coordinator) PutTask(task *Task) {

	taskInfo := TaskInfo{
		TaskState: Waiting,
		Task:      task,
		StartTime: time.Now(),
	}
	/* meta, _ := c.TasksMetainfos[Task.TaskId]
	if meta != nil {
		log.Fatalf("这个任务id已经被分配%v", meta)
	} */
	c.TasksMetainfos[task.TaskId] = &taskInfo
	if task.TaskName == MapTask {
		c.MapTaskChan <- task
	} else {
		c.ReduceTaskChan <- task
	}

	//fmt.Printf("%d task make success%v\n", task.TaskName, task)

}

func (c *Coordinator) FireTask(taskId int) {
	c.Mu.Lock()
	defer c.Mu.Unlock()
	c.TasksMetainfos[taskId].StartTime = time.Now()
	c.TasksMetainfos[taskId].TaskState = Running
	//fmt.Printf("task meta info%v\n", c.TasksMetainfos[taskId])
}

func (c *Coordinator) FindCrashTask() int {
	c.Mu.Lock()
	defer c.Mu.Unlock()
	for _, info := range c.TasksMetainfos {
		if time.Since(info.StartTime) > 10*time.Second && info.TaskState != Done {
			fmt.Printf("找到一个crash任务%v\n", info)
			//重新分配crash任务，需要更新时间
			info.StartTime = time.Now()
			return info.Task.TaskId
		}
	}
	return -1

}

func (c *Coordinator) Distribute(args *Args, reply *Task) error {
	//map阶段请求任务
	if c.TaskPhase == MapPhase {
		//如果有任务，从管道拿到一个任务
		if len(c.MapTaskChan) > 0 {
			*reply = *<-c.MapTaskChan
			c.FireTask(reply.TaskId)
			//fmt.Printf("分发任务成功%v\n", reply.TaskId)
		} else {
			//管道虽然可能没有任务，但可能有任务worker已经 crash了
			//需要去检查任务的控制信息，检查是否有任务超时
			taskId := c.FindCrashTask()
			//没有找到
			if taskId == -1 {
				reply.TaskName = EmptyTask
				//fmt.Printf("分发空任务%v\n", reply.TaskName)
			} else {
				*reply = *c.TasksMetainfos[taskId].Task
			}

		}

	} else if c.TaskPhase == ReducePhase {
		//如果有任务，从管道拿到一个任务
		if len(c.ReduceTaskChan) > 0 {
			*reply = *<-c.ReduceTaskChan
			c.FireTask(reply.TaskId)
			//fmt.Printf("分发reduce任务成功 %v \n", reply)
		} else {
			//管道虽然可能没有任务，但可能有任务worker已经 crash了
			//需要去检查任务的控制信息，检查是否有任务超时
			taskId := c.FindCrashTask()
			//没有找到
			if taskId == -1 {
				reply.TaskName = Break
				fmt.Printf("分发停止任务%v\n", reply.TaskName)
			} else {
				*reply = *c.TasksMetainfos[taskId].Task
			}
		}

	} else {
		reply.TaskName = Break
	}
	return nil
}

func (c *Coordinator) TaskDoneNotify(args *Args, reply *Task) error {
	//任务返回，但是可能会存在超时被分配给其他worker
	c.Mu.Lock()
	defer c.Mu.Unlock()
	elapsed := time.Since(c.TasksMetainfos[args.TaskId].StartTime)
	//超时或者被其他woker完成，返回空任务
	if elapsed > 10*time.Second || c.TasksMetainfos[args.TaskId].TaskState == Done {
		if elapsed > 10*time.Second {
			fmt.Printf("ID %v 的任务超时 time：%v\n", args.TaskId, elapsed)
		} else {
			fmt.Printf("ID %v 的任务被完成\n", args.TaskId)
		}

		reply.TaskName = EmptyTask
	} else {
		//fmt.Printf("task 成功返回的内容%v\n", args)
		c.TaskDoneOperation(args)
		if c.IsNeedChangePhase() {
			if c.TaskPhase == MapPhase {
				c.TaskPhase = ReducePhase
				// 清空 map
				c.TasksMetainfos = make(map[int]*TaskInfo)
				fmt.Println("转变阶段到reduce阶段")
				go c.MakeReduceTasks()
			} else {
				fmt.Println("转变阶段到Stop阶段")
				c.TaskPhase = Stop
			}

		}
	}
	return nil

}
func (c *Coordinator) TaskDoneOperation(args *Args) {
	// 把任务标记为完成
	c.TasksMetainfos[args.TaskId].TaskState = Done
	//fmt.Printf("任务%d标记为完成\n", args.TaskId)
	if c.TaskPhase != MapPhase {
		return
	}

	// 构建需要匹配的文件名模式
	pattern := fmt.Sprintf("mr-tmp%d-%d-*.json", args.RandIdnex, args.TaskId)

	// 获取目录下所有文件
	files, err := filepath.Glob("*")
	if err != nil {
		log.Printf("获取文件列表错误: %v", err)
		return
	}

	//fmt.Printf("重命名文件：%s \n", pattern)

	// 循环遍历文件列表，匹配文件名并重命名
	for _, oldName := range files {
		match, err := filepath.Match(pattern, oldName)
		if err != nil {
			fmt.Printf("匹配文件名错误: %v", err)
			continue
		}
		if match {
			// 提取文件名
			oldBase := filepath.Base(oldName)
			// 将文件名中的 "mr-tmp" 替换为 "mr"
			newBase := strings.Replace(oldBase, fmt.Sprintf("mr-tmp%d", args.RandIdnex), "mr", 1)
			//fmt.Printf("重命名文件：%s -> %s\n", oldBase, newBase)
			// 构建新的文件路径
			newName := filepath.Join(filepath.Dir(oldName), newBase)
			// 执行重命名操作
			err := os.Rename(oldName, newName)
			if err != nil {
				fmt.Println("重命名文件错误:", err)
			}
		}
	}
	//fmt.Println("临时文件处理完成")
}

func (c *Coordinator) IsNeedChangePhase() bool {
	for _, task := range c.TasksMetainfos {
		if task.TaskState != Done {
			return false
		}
	}
	return true
}

// 启动一个线程监听来自 worker.go 的 RPC 请求
func (c *Coordinator) server() {
	rpc.Register(c)
	rpc.HandleHTTP()
	//l, e := net.Listen("tcp", ":1234")
	sockname := coordinatorSock()
	os.Remove(sockname)
	l, e := net.Listen("unix", sockname)
	if e != nil {
		log.Fatal("listen error:", e)
	}
	go http.Serve(l, nil)
}

// main/mrcoordinator.go 定期调用 Done() 方法，以确定作业是否已完成
func (c *Coordinator) Done() bool {
	return c.TaskPhase == Stop
}

// 创建一个 Coordinator
func MakeCoordinator(files []string, nReduce int) *Coordinator {
	c := Coordinator{}
	// 这里放置初始化协调器的逻辑
	c.ReduceNum = nReduce
	c.TaskPhase = MapPhase
	c.TaskId = 0
	c.MapTaskChan = make(chan *Task, len(files))
	c.ReduceTaskChan = make(chan *Task, nReduce)
	c.TasksMetainfos = make(map[int]*TaskInfo)
	go c.MakeMapTasks(files)
	c.server()
	return &c
}

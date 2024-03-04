package mr

import (
	"encoding/json"
	"fmt"
	"hash/fnv"
	"io"
	"log"
	"math/rand"
	"net/rpc"
	"os"
	"sort"
	"strconv"
	"time"
)

// for sorting by key.
type ByKey []KeyValue

// for sorting by key.
func (a ByKey) Len() int           { return len(a) }
func (a ByKey) Swap(i, j int)      { a[i], a[j] = a[j], a[i] }
func (a ByKey) Less(i, j int) bool { return a[i].Key < a[j].Key }

// KeyValue 结构用于存储键值对.
type KeyValue struct {
	Key   string
	Value string
}

// 使用 ihash(key) % NReduce 来选择每个 KeyValue 由 Map 发出的 reduce 任务编号.
// 返回一个 uint32 类型的值，表示通过 FNV-1a 哈希算法对输入数据进行哈希后得到的结果
func ihash(key string) int {
	h := fnv.New32a()
	h.Write([]byte(key))
	return int(h.Sum32() & 0x7fffffff)
}

// Worker 函数用于实现工作节点.
func Worker(mapf func(string, string) []KeyValue,
	reducef func(string, []string) string) {
	args := Args{}
	reply := Task{}
	//fmt.Printf("worker请求任务\n")
	GetTask(&args, &reply)
	switch reply.TaskName {
	case MapTask:
		{
			tmpIndex := Map(&reply, mapf)
			NotifyArgs := Args{}
			NotifyArgs.TaskId = reply.TaskId
			NotifyArgs.RandIdnex = tmpIndex
			TaskDoneNotify(&NotifyArgs, &reply)
			//fmt.Printf("临时id%v \n", args)
		}
	case ReduceTask:
		{
			Reduce(&reply, reducef)
			NotifyArgs := Args{}
			NotifyArgs.TaskId = reply.TaskId
			TaskDoneNotify(&NotifyArgs, &reply)
		}
	case EmptyTask:
		{
		}
	case Break:
		{
			return
		}
	}
	time.Sleep(3 * time.Second)

	Worker(mapf, reducef)
}

// CallExample 函数展示如何向协调器发起 RPC 调用的示例.
func GetTask(args *Args, reply *Task) {
	// "Coordinator.Example" 告诉接收服务器我们想调用 Coordinator 结构体的 Example() 方法.
	ok := call("Coordinator.Distribute", args, reply)
	if ok {
		//fmt.Printf("GetTask %v\n", reply.FileName)
	} else {
		fmt.Printf("call failed!\n")
	}
}

// 向协调器发起 RPC 通知任务完成
func TaskDoneNotify(args *Args, reply *Task) {
	// "Coordinator.TaskDoneNotify" 告诉接收服务器我们想调用 Coordinator 结构体的 Example() 方法.
	//fmt.Printf("任务完成，发起通知  %d\n", reply.TaskId)
	ok := call("Coordinator.TaskDoneNotify", args, reply)
	if ok {
		//fmt.Printf("TaskDoneNotify reply %v\n", reply.TaskName)
	} else {
		fmt.Printf("call failed!\n")
	}
}

func Map(task *Task, mapf func(string, string) []KeyValue) int {
	intermediate := []KeyValue{}
	for _, filename := range task.FileName {
		file, err := os.Open(filename)
		if err != nil {
			log.Fatalf("打开文件错误%s", filename)
		}
		content, err := io.ReadAll(file)
		if err != nil {
			log.Fatalf("读取文件错误%s", filename)
		}
		file.Close()
		kva := mapf(filename, string(content))
		intermediate = append(intermediate, kva...)
	}

	//接下来定义reduceNum个编码器
	//循环处理每一个键值对，加入编码器
	//写入tmp文件(假设中断或者未完成怎么办？)
	tmpKV := make([][]KeyValue, task.ReduceNum)
	for i := 0; i < task.ReduceNum; i++ {
		tmpKV[i] = make([]KeyValue, 0)
	}
	for _, kv := range intermediate {
		index := ihash(kv.Key) % task.ReduceNum
		tmpKV[index] = append(tmpKV[index], kv)
	}
	randTmpIndex := rand.Intn(10000)
	for i := 0; i < task.ReduceNum; i++ {
		sort.Sort(ByKey(tmpKV[i]))
		tmpFileName := "mr-tmp" + strconv.Itoa(randTmpIndex) + "-" + strconv.Itoa(task.TaskId) + "-" + strconv.Itoa(i) + ".json"
		file, err := os.Create(tmpFileName)
		if err != nil {
			log.Fatalf("创建文件错误%s", err)
		}
		defer file.Close()
		encoder := json.NewEncoder(file)
		err = encoder.Encode(tmpKV[i])
		if err != nil {
			log.Fatalf("编码错误%s", err)
		}
	}
	//fmt.Println("JSON 数据写入完成！")
	return randTmpIndex
}

func Reduce(task *Task, reducef func(string, []string) string) {
	kv := []KeyValue{}
	for _, file := range task.FileName {
		file, err := os.Open(file)
		if err != nil {
			log.Fatalf("打开json文件错误%v", err)
		}
		decoder := json.NewDecoder(file)
		tmpKv := []KeyValue{}
		err = decoder.Decode(&tmpKv)
		if err != nil {
			log.Fatalf("解码错误%s", err)
		}
		kv = append(kv, tmpKv...)
	}
	sort.Sort(ByKey(kv))

	oname := "mr-out-" + strconv.Itoa(task.TaskId) + ".txt"
	ofile, err := os.Create(oname)
	if err != nil {
		log.Fatalf("创建文件错误%v", err)
	}
	i := 0
	for i < len(kv) {
		j := i + 1
		for j < len(kv) && kv[j].Key == kv[i].Key {
			j++
		}
		values := []string{}
		for k := i; k < j; k++ {
			values = append(values, kv[k].Value)
		}
		output := reducef(kv[i].Key, values)
		// this is the correct format for each line of Reduce output.
		fmt.Fprintf(ofile, "%v %v\n", kv[i].Key, output)
		i = j

	}
}

// 向协调器发送一个 RPC 请求，并等待响应.
// 通常返回 true.
// 如果出现问题，则返回 false.
func call(rpcname string, args interface{}, reply interface{}) bool {
	// c, err := rpc.DialHTTP("tcp", "127.0.0.1"+":1234")
	sockname := coordinatorSock()
	c, err := rpc.DialHTTP("unix", sockname)
	if err != nil {
		log.Fatal("dialing:", err)
	}
	defer c.Close()

	err = c.Call(rpcname, args, reply)
	if err == nil {
		return true
	}

	fmt.Println(err)
	return false
}

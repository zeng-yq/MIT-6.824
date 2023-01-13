package mr

import (
	"encoding/json"
	"fmt"
	"hash/fnv"
	"io/ioutil"
	"log"
	"net/rpc"
	"os"
	"sort"
	"strconv"
	"sync"
)

// Map functions return a slice of KeyValue.
type KeyValue struct {
	Key   string
	Value string
}

// for sorting by key.
type ByKey []KeyValue

// for sorting by key.
func (a ByKey) Len() int           { return len(a) }
func (a ByKey) Swap(i, j int)      { a[i], a[j] = a[j], a[i] }
func (a ByKey) Less(i, j int) bool { return a[i].Key < a[j].Key }

// 全局锁，避免多个 Worker 同时向 Master 申请任务
var mu_worker sync.Mutex

// use ihash(key) % NReduce to choose the reduce
// task number for each KeyValue emitted by Map.
func ihash(key string) int {
	h := fnv.New32a()
	h.Write([]byte(key))
	return int(h.Sum32() & 0x7fffffff)
}

// main/mrworker.go calls this function.
func Worker(mapf func(string, string) []KeyValue,
	reducef func(string, []string) string) {

	// Your worker implementation here.
	for {
		task := AskTask()
		switch task.Phase {
		case "map":
			DoMapTask(task, mapf)
		case "reduce":
			// fmt.Println("已获取到 reduce 任务：", task)
			DoReduceTask(task, reducef)
		}
	}
	// uncomment to send the Example RPC to the master.
	// CallExample()
}

// 执行 reduce 任务，输出结果到文件中
func DoReduceTask(task TaskInfo, reducef func(string, []string) string) {
	nMap := task.MapNum
	taskId := task.TaskId
	oname := "mr-out-" + strconv.Itoa(taskId)
	ofile, _ := os.Create(oname)

	kva := []KeyValue{}
	for j := 0; j < nMap; j++ {
		fileName := "mr-" + strconv.Itoa(j) + "-" + strconv.Itoa(taskId)
		file, err := os.Open(fileName)
		if err != nil {
			fmt.Println("cannot open file: ", fileName)
			continue
		}

		dec := json.NewDecoder(file)
		for {
			var kv KeyValue
			if err := dec.Decode(&kv); err != nil {
				break
			}
			kva = append(kva, kv)
		}
		file.Close()
	}
	sort.Sort(ByKey(kva))

	i := 0
	for i < len(kva) {
		j := i + 1
		for j < len(kva) && kva[j].Key == kva[i].Key {
			j++
		}
		values := []string{}
		for k := i; k < j; k++ {
			values = append(values, kva[k].Value)
		}
		output := reducef(kva[i].Key, values)

		fmt.Fprintf(ofile, "%v %v\n", kva[i].Key, output)

		i = j
	}

	ofile.Close()

	// fmt.Println(">>>>>>>> reduce 任务 ", task.TaskId, " 执行成功!!!")

	//通过 RPC 调用，通知 Master 该 reduce 任务已完成，修改其状态
	ModifyTaskStatus(task.TaskId)
}

// 执行指定的 map 任务
func DoMapTask(task TaskInfo, mapf func(string, string) []KeyValue) {
	fileName := task.InputFile
	intermediate := []KeyValue{}
	nReduce := task.ReduceNum

	file, err := os.Open(fileName)
	if err != nil {
		fmt.Println("cannot open file: ", fileName)
		return
	}

	content, err := ioutil.ReadAll(file)
	if err != nil {
		fmt.Println("cannot read file: ", fileName)
		return
	}

	file.Close()
	kva := mapf(fileName, string(content))
	intermediate = append(intermediate, kva...)

	sort.Sort(ByKey(intermediate))

	reduceData := [][]KeyValue{}
	for i := 0; i < nReduce; i++ {
		reduceData = append(reduceData, []KeyValue{})
	}

	for i := 0; i < len(intermediate); i++ {
		reduceId := ihash(intermediate[i].Key) % nReduce
		reduceData[reduceId] = append(reduceData[reduceId], intermediate[i])
	}

	// 将 map 任务的结果转化为 json 格式，输出到文件
	GenerateFile(nReduce, reduceData, task.TaskId)

	// fmt.Println(">>>>>>>> map 任务 ", task.TaskId, " 执行成功!!!")

	//通过 RPC 调用，通知 Master 该 map 任务已完成，修改其状态
	ModifyTaskStatus(task.TaskId)
}

// 修改指定任务的状态
func ModifyTaskStatus(taskId int) {
	mu_worker.Lock()
	defer mu_worker.Unlock()

	ok := call("Master.ModifyTask", &taskId, nil)
	if !ok {
		fmt.Println("RPC Master.ModifyTask err......")
	}
}

// 将map任务的结果以json格式输出到文件
func GenerateFile(nReduce int, reduceData [][]KeyValue, taskId int) {
	for i := 0; i < nReduce; i++ {
		oname := "mr-" + strconv.Itoa(taskId) + "-" + strconv.Itoa(i)
		ofile, _ := os.Create(oname)

		// 创建encoder，数据输出到指定文件中
		encoder := json.NewEncoder(ofile)
		// 把数据encode到文件中
		for _, kv := range reduceData[i] {
			encoder.Encode(kv)
		}

		ofile.Close()
	}
}

// 通过RPC向Master请求一个任务
func AskTask() TaskInfo {
	mu_worker.Lock()
	defer mu_worker.Unlock()

	args := TaskArgs{}
	reply := TaskReply{}

	ok := call("Master.AllocateTask", &args, &reply)
	if !ok {
		fmt.Println("RPC Master.AllocateTask err......")
	}

	return reply.Task
}

// example function to show how to make an RPC call to the master.
//
// the RPC argument and reply types are defined in rpc.go.
func CallExample() {

	// declare an argument structure.
	args := ExampleArgs{}

	// fill in the argument(s).
	args.X = 99

	// declare a reply structure.
	reply := ExampleReply{}

	// send the RPC request, wait for the reply.
	call("Master.Example", &args, &reply)

	// reply.Y should be 100.
	fmt.Printf("reply.Y %v\n", reply.Y)
}

// send an RPC request to the master, wait for the response.
// usually returns true.
// returns false if something goes wrong.
func call(rpcname string, args interface{}, reply interface{}) bool {
	// c, err := rpc.DialHTTP("tcp", "127.0.0.1"+":1234")
	sockname := masterSock()
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

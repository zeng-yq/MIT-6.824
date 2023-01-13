package mr

import (
	"fmt"
	"log"
	"net"
	"net/http"
	"net/rpc"
	"os"
	"time"
)

type Master struct {
	// Your definitions here.
	TaskList  []TaskInfo //任务的列表
	ReduceNum int        // reduce 任务的个数
	Phase     string     // 任务的阶段（map、reduce、done）
}

// Your code here -- RPC handlers for the worker to call.
// RPC: 选择一个任务来响应 Worker 的请求
func (m *Master) AllocateTask(args *TaskArgs, reply *TaskReply) error {
	taskId := m.choiceTask()

	// 该阶段的所有任务均已经完成
	if taskId == -1 {
		m.dealTaskFinised()

		// 若是map阶段结束，则把任务列表里面的第一个 reduce 任务返回
		taskId = 0

	}

	m.TaskList[taskId].StartTime = time.Now() // 设置任务开始时间为被分配的时间
	reply.Task = m.TaskList[taskId]
	// fmt.Println("worker 来请求任务！！！")

	return nil
}

// RPC: worker完成任务后，修改该任务的状态为finished
func (m *Master) ModifyTask(args *int, reply *TaskReply) error {
	m.TaskList[*args].TaskStatus = "finished"

	return nil
}

// 所有任务均已经完成，根据所处的阶段执行不同的操作
func (m *Master) dealTaskFinised() {
	switch m.Phase {
	// 如果 map 阶段结束，则进入 reduce 阶段
	case "map":
		m.Phase = "reduce"

		// 生成一组reduce任务
		m.generateReduceTasks()

	// 如果 reduce 阶段结束，则 Master 停止运行
	case "reduce":
		m.Phase = "done"
		for {
			for _, r := range `-\|/` {
				fmt.Printf("\r%c", r)
				time.Sleep(time.Millisecond * 100)
			}
		}
	}
}

// map阶段结束后需要生成一组reduce任务，覆盖掉原来任务列表里面的任务
func (m *Master) generateReduceTasks() {
	taskInfo := []TaskInfo{}
	for i := 0; i < m.ReduceNum; i++ {
		task := TaskInfo{"reduce", i, "", "waiting", time.Now(), m.ReduceNum, len(m.TaskList)}
		taskInfo = append(taskInfo, task)
	}
	m.TaskList = taskInfo
}

// 选择一个处于 waiting 状态的任务，返回其 TaskId，
// 若所有任务均已完成，返回 -1
func (m *Master) choiceTask() int {
	for {
		var allFinished = true
		for i := 0; i < len(m.TaskList); i++ {
			if m.TaskList[i].TaskStatus == "waiting" {
				m.TaskList[i].TaskStatus = "allocated"
				allFinished = false
				return i
			} else if m.TaskList[i].TaskStatus == "allocated" { // 若有任务没有完成，且没有任务处于waiting状态，则循环等待allocated的任务结束或超时
				m.moniterTimeOut()
				allFinished = false
			}
		}

		if allFinished {
			return -1
		}
	}
}

// an example RPC handler.
//
// the RPC argument and reply types are defined in rpc.go.
func (m *Master) Example(args *ExampleArgs, reply *ExampleReply) error {
	reply.Y = args.X + 1
	return nil
}

// start a thread that listens for RPCs from worker.go
func (m *Master) server() {
	rpc.Register(m)
	rpc.HandleHTTP()
	//l, e := net.Listen("tcp", ":1234")
	sockname := masterSock()
	os.Remove(sockname)
	l, e := net.Listen("unix", sockname)
	if e != nil {
		log.Fatal("listen error:", e)
	}
	go http.Serve(l, nil)
}

// main/mrmaster.go calls Done() periodically to find out
// if the entire job has finished.
func (m *Master) Done() bool {
	ret := false

	// Your code here.
	if m.Phase == "done" {
		fmt.Println("------------ 所有任务执行结束,退出 --------")
		return true
	}

	return ret
}

// create a Master.
// main/mrmaster.go calls this function.
// nReduce is the number of reduce tasks to use.
func MakeMaster(files []string, nReduce int) *Master {
	m := Master{}

	// Your code here.
	m.initMaster(files, nReduce)

	m.server()

	return &m
}

// 根据输入的 files 和 nReduce 生成一组 map 任务，并初始化 Master
func (m *Master) initMaster(files []string, nReduce int) {
	m.Phase = "map"
	m.ReduceNum = nReduce

	for index, file := range files {
		task := TaskInfo{"map", index, file, "waiting", time.Now(), nReduce, len(m.TaskList)}
		m.TaskList = append(m.TaskList, task)
	}
}

// 检测任务的状态，若已被分配的任务超过10秒还没执行结束，
// 则将该任务的状态设置为waiting，以便重新分配
func (m *Master) moniterTimeOut() {
	for i := 0; i < len(m.TaskList); i++ {
		if m.TaskList[i].TaskStatus == "allocated" && time.Since(m.TaskList[i].StartTime) > time.Second*10 {
			m.TaskList[i].TaskStatus = "waiting"
		}
	}
}

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

type ExampleArgs struct {
	X int
}

type ExampleReply struct {
	Y int
}

// Add your RPC definitions here.

// 任务信息
type TaskInfo struct {
	Phase      string    // 任务阶段：map、reduce
	TaskId     int       // 任务ID，即在Master任务列表中的下标
	InputFile  string    // 文件名
	TaskStatus string    // 任务状态：waiting、allocated、finished
	StartTime  time.Time // 任务的开始时间，用于超时判断
	ReduceNum  int       // 记录reduce任务的数量
	MapNum     int       // 记录map任务的数量
}

// worker 通过 RPC 请求任务所传的参数，为空即可
type TaskArgs struct{}

// worker 通过 RPC 请求任务，Master 返回一个任务
type TaskReply struct {
	Task TaskInfo
}

// Cook up a unique-ish UNIX-domain socket name
// in /var/tmp, for the master.
// Can't use the current directory since
// Athena AFS doesn't support UNIX-domain sockets.
func masterSock() string {
	s := "/var/tmp/824-mr-"
	s += strconv.Itoa(os.Getuid())
	return s
}

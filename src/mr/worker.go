package mr

import (
	"fmt"
	"time"
)
import "log"
import "net/rpc"

type keyValues []KeyValue

func (a keyValues) Len() int           { return len(a) }
func (a keyValues) Swap(i, j int)      { a[i], a[j] = a[j], a[i] }
func (a keyValues) Less(i, j int) bool { return a[i].Key < a[j].Key }

type KeyValue struct {
	Key   string
	Value string
}

type workerContext struct {
	state int

	workerUID int

	nReduce  int
	fileName string
	jobUID   int
}

func Worker(mapF func(string, string) []KeyValue,
	reduceF func(string, []string) string) {
	//初始化
	c := &workerContext{}
	c.init()

	//请求
	for {
		c.assign()
		switch c.state {
		case AsMap:
			err := c.mapJob(mapF)
			if err != nil {
				c.errorHandel()
			}

		case AsReduce:
			err := c.reduceJob(reduceF)
			if err != nil {
				c.errorHandel()
			}

		case Wait:
			time.Sleep(1 * time.Second)

		case Finish:
			return

		default:
			break
		}
	}
}

func (wContext *workerContext) init() {
	replay := &InitResponse{}
	call("Coordinator.Init", &InitRequest{}, replay)

	wContext.state = Wait
	wContext.nReduce = replay.NReduce
	wContext.workerUID = replay.WorkerUID
}

func (wContext *workerContext) assign() {
	replay := &AssignResponse{}
	call("Coordinator.Assign", &AssignRequest{wContext.workerUID, wContext.state, wContext.jobUID}, replay)

	wContext.state = replay.JobType
	wContext.jobUID = replay.JobUID
	wContext.fileName = replay.FileName
}

func (wContext *workerContext) errorHandel() {
	call("Coordinator.Error", &ErrorRequest{wContext.workerUID, wContext.state, wContext.jobUID, wContext.fileName}, &ErrorResponse{})
	wContext.state = Fail
}

func call(rpcname string, args interface{}, reply interface{}) bool {
	sockName := coordinatorSock()
	c, err := rpc.DialHTTP("unix", sockName)
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

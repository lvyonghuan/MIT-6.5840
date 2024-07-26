package mr

import (
	"log"
	"sync"
	"sync/atomic"
	"time"
)
import "net"
import "os"
import "net/rpc"
import "net/http"

type job struct {
	workerUID int
	jobUID    int
	jobType   int
	fileName  string
}

type workerInfo struct {
	uid       int
	state     int
	job       job
	finishJob chan struct{}
}

type Coordinator struct {
	fileNames []string
	nReduce   int

	inputFilesNum int

	mapJobChan    chan job
	reduceJobChan chan job
	mapJobWg      sync.WaitGroup
	reduceJobWg   sync.WaitGroup

	workers         sync.Map
	doingJobWorkers sync.Map
	workersNum      int32

	isTaskFinish bool
}

func (c *Coordinator) Init(args *InitRequest, reply *InitResponse) error {
	worker := c.makeWorker()
	c.workers.Store(worker.uid, &worker)

	reply.NReduce = c.nReduce
	reply.WorkerUID = worker.uid

	return nil
}

func (c *Coordinator) Assign(args *AssignRequest, reply *AssignResponse) error {
	//获取worker
	workerInterface, _ := c.workers.Load(args.WorkerUID)
	worker := workerInterface.(*workerInfo)

	//查阅是否正在执行任务
	if _, ok := c.doingJobWorkers.Load(args.WorkerUID); ok {
		switch args.LastJobType {
		case AsMap:
			worker.finishJob <- struct{}{}
			c.mapJobWg.Done()
			c.doingJobWorkers.Delete(args.WorkerUID)

		case AsReduce:
			worker.finishJob <- struct{}{}
			c.reduceJobWg.Done()
			c.doingJobWorkers.Delete(args.WorkerUID)

		default:
			break
		}
	}

	//分配任务
	select {
	case mapJob := <-c.mapJobChan:
		reply.JobType = AsMap
		reply.JobUID = mapJob.jobUID
		reply.FileName = mapJob.fileName

		worker.state = AsMap
		worker.job = mapJob
		c.doingJobWorkers.Store(args.WorkerUID, worker)
		go c.timer(*worker, mapJob.jobType, mapJob.jobUID, mapJob.fileName)

	case reduceJob := <-c.reduceJobChan:
		reply.JobType = AsReduce
		reply.JobUID = reduceJob.jobUID

		worker.state = AsReduce
		worker.job = reduceJob
		c.doingJobWorkers.Store(args.WorkerUID, worker)
		go c.timer(*worker, reduceJob.jobType, reduceJob.jobUID, "")

	default:
		if c.isTaskFinish {
			reply.JobType = Finish
		} else {
			reply.JobType = Wait
		}
	}

	return nil
}

func (c *Coordinator) ErrorHandel(args *ErrorRequest, reply *ErrorResponse) error {

	c.doingJobWorkers.Delete(args.WorkerUID)
	switch args.LastJobType {
	case AsMap:
		c.mapJobChan <- job{
			jobUID:   args.LastJobUID,
			jobType:  AsMap,
			fileName: args.FileName,
		}

	case AsReduce:
		c.reduceJobChan <- job{
			jobUID:  args.LastJobUID,
			jobType: AsReduce,
		}

	default:
		break
	}

	return nil
}

func (c *Coordinator) server() {
	err := rpc.Register(c)
	if err != nil {
		panic(err)
	}

	rpc.HandleHTTP()
	sockName := coordinatorSock()

	os.Remove(sockName)

	l, e := net.Listen("unix", sockName)

	if e != nil {
		log.Fatal("listen error:", e)
	}
	go http.Serve(l, nil)
}

func (c *Coordinator) Done() bool {
	c.mapJobWg.Wait()
	c.fillReduceJobChan()
	c.reduceJobWg.Wait()
	c.isTaskFinish = true
	time.Sleep(2 * time.Second)

	return true
}

func (c *Coordinator) makeWorker() workerInfo {
	atomic.AddInt32(&c.workersNum, 1)
	workerUID := int(c.workersNum)

	return workerInfo{
		uid:       workerUID,
		state:     Wait,
		job:       job{},
		finishJob: make(chan struct{}),
	}
}

func MakeCoordinator(files []string, nReduce int) *Coordinator {
	c := Coordinator{
		fileNames:     files,
		nReduce:       nReduce,
		inputFilesNum: len(files),
		mapJobChan:    make(chan job, len(files)),
		reduceJobChan: make(chan job, nReduce),
	}
	atomic.StoreInt32(&c.workersNum, 0)

	c.fillMapJobChan()

	c.server()
	return &c
}

func (c *Coordinator) fillMapJobChan() {
	for k, v := range c.fileNames {
		c.mapJobWg.Add(1)
		c.mapJobChan <- job{
			jobUID:   k,
			jobType:  AsMap,
			fileName: v,
		}
	}
}

func (c *Coordinator) fillReduceJobChan() {
	for i := 0; i < c.nReduce; i++ {
		c.reduceJobWg.Add(1)
		c.reduceJobChan <- job{
			jobUID:  i,
			jobType: AsReduce,
		}
	}
}

// 超时器
func (c *Coordinator) timer(worker workerInfo, lastJobType, jobUID int, fileName string) {
	select {
	//10s超时
	case <-time.After(10 * time.Second):
		c.ErrorHandel(&ErrorRequest{
			WorkerUID:   worker.uid,
			LastJobType: lastJobType,
			LastJobUID:  jobUID,
			FileName:    fileName,
		}, &ErrorResponse{})

		return

	case <-worker.finishJob:
		return
	}
}

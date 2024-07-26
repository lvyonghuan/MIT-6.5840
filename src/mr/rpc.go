package mr

import "os"
import "strconv"

const (
	Wait = iota
	AsMap
	AsReduce
	Finish
	Fail
)

type InitRequest struct{}
type InitResponse struct {
	NReduce   int
	WorkerUID int
}

type AssignRequest struct {
	WorkerUID   int
	LastJobType int
	LastJobUID  int
}
type AssignResponse struct {
	JobType  int
	JobUID   int
	FileName string
}

type ErrorRequest struct {
	WorkerUID   int
	LastJobType int
	LastJobUID  int
	FileName    string
}
type ErrorResponse struct{}

func coordinatorSock() string {
	s := "/var/tmp/5840-mr-"
	s += strconv.Itoa(os.Getuid())
	return s
}

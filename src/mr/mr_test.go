package mr_test

import (
	"testing"

	"6.5840/mr"
)

func TestAssignWork(t *testing.T) {
	start(nil, 1)
	mr.Worker(nil, nil)
}

func start(files []string, nReduce int) *mr.Coordinator {
	return mr.MakeCoordinator(files, nReduce)
}

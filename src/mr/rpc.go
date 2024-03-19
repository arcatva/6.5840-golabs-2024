package mr

//
// RPC definitions.
//
// remember to capitalize all names.
//

import (
	"os"
	"strconv"
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
type TaskRequest struct {
	WorkerName string
}
type TaskReceive struct {
	CoordinatorPhase CoordinatorPhase
	*MapTask
	*ReduceTask
}

type WorkType int

const (
	MAPWORK WorkType = iota
	REDUCEWORK
)

type WorkStatus int

const (
	PENDING WorkStatus = iota
	ONGOING
	DONE
)

type Result struct {
	WorkerName string
	WorkType
	WorkStatus
	FileId int
}

type ResultSendBack struct {
	*Result
}

type ResultAcknowledge struct {
	*Result
}

type MapTask struct {
	FileName string
	FileId   int
	NReduce  int
}
type ReduceTask struct {
	FileNames []string
	FileId    int
}

// Cook up a unique-ish UNIX-domain socket name
// in /var/tmp, for the coordinator.
// Can't use the current directory since
// Athena AFS doesn't support UNIX-domain sockets.
func coordinatorSock() string {
	s := "/var/tmp/5840-mr-"
	s += strconv.Itoa(os.Getuid())
	return s
}

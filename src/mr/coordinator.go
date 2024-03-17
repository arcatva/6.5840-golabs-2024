package mr

import (
	"log"
	"net"
	"net/http"
	"net/rpc"
	"os"
	"sync"
)

type coordinatorPhase int

const (
	mapping coordinatorPhase = iota
	reducing
)

type Coordinator struct {
	// Your definitions here.
	mappingQueue  []string
	reducingQueue []string
	coordinatorPhase
	nReduce      int
	callTaskLock sync.Mutex
}

// Your code here -- RPC handlers for the worker to call.

// an example RPC handler.
//
// the RPC argument and reply types are defined in rpc.go.
func (c *Coordinator) Example(args *ExampleArgs, reply *ExampleReply) error {
	reply.Y = args.X + 1
	return nil
}

func (c *Coordinator) CallTask(args *TaskRequest, reply *TaskReceive) error {
	c.callTaskLock.Lock()
	log.Printf("Calling from %s in %v mode", args.WorkerName, c.coordinatorPhase)
	reply.CoordinatorPhase = c.coordinatorPhase

	switch c.coordinatorPhase {
	case mapping:
		reply.MapTask = c.provideMapTask()
		break
	case reducing:
		reply.ReduceTask = c.provideReduceTask()
		break
	}
	c.callTaskLock.Unlock()
	return nil
}

func (c *Coordinator) provideMapTask() *MapTask {
	file := c.mappingQueue[len(c.mappingQueue)-1]
	fileOrder := len(c.mappingQueue) - 1
	c.mappingQueue = c.mappingQueue[0 : len(c.mappingQueue)-1]

	if len(c.mappingQueue) == 0 {
		c.coordinatorPhase = reducing
		log.Println("Entering reducing phase")
	}
	log.Printf("Remain %v in queue", len(c.mappingQueue))
	return &MapTask{
		FileName:  file,
		FileOrder: fileOrder,
		NReduce:   c.nReduce,
	}
}
func (c *Coordinator) provideReduceTask() *ReduceTask {
	return &ReduceTask{}
}

// start a thread that listens for RPCs from worker.go
func (c *Coordinator) server() {
	rpc.Register(c)
	rpc.HandleHTTP()
	// l, e := net.Listen("tcp", ":1234")
	sockname := coordinatorSock()
	os.Remove(sockname)
	l, e := net.Listen("unix", sockname)
	if e != nil {
		log.Fatal("listen error:", e)
	}
	go http.Serve(l, nil)
}

// main/mrcoordinator.go calls Done() periodically to find out
// if the entire job has finished.
func (c *Coordinator) Done() bool {
	ret := false

	// Your code here.

	return ret
}

// create a Coordinator.
// main/mrcoordinator.go calls this function.
// nReduce is the number of reduce tasks to use.
func MakeCoordinator(files []string, nReduce int) *Coordinator {
	c := Coordinator{}

	// Your code here.
	c.mappingQueue = make([]string, len(files))
	c.reducingQueue = make([]string, len(files))
	c.coordinatorPhase = mapping
	c.nReduce = nReduce
	for i, fileName := range files {
		c.mappingQueue[i] = fileName
	}
	c.server()
	return &c
}

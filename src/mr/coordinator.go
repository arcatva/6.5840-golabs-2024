package mr

import (
	"log"
	"net"
	"net/http"
	"net/rpc"
	"os"
	"sync"
)

type fileStatus int

const (
	pending fileStatus = iota
	working
	finished
)

type Coordinator struct {
	// Your definitions here.
	files           map[string]fileStatus
	pendingFiles    []string
	callMapTaskLock sync.Mutex
}

// Your code here -- RPC handlers for the worker to call.

// an example RPC handler.
//
// the RPC argument and reply types are defined in rpc.go.
func (c *Coordinator) Example(args *ExampleArgs, reply *ExampleReply) error {
	reply.Y = args.X + 1
	return nil
}

func (c *Coordinator) CallMapTask(args *MapTaskRequest, reply *MapTaskReceive) error {
	c.callMapTaskLock.Lock()

	file := c.pendingFiles[len(c.pendingFiles)-1]
	c.pendingFiles = c.pendingFiles[0 : len(c.pendingFiles)-1]
	c.files[file] = working
	reply.FileName = file
	c.callMapTaskLock.Unlock()

	return nil
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
	c.files = make(map[string]fileStatus)
	c.pendingFiles = make([]string, len(files))
	for i, fileName := range files {
		c.pendingFiles[i] = fileName
		c.files[fileName] = pending
	}
	c.server()
	return &c
}

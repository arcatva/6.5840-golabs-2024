package mr

import (
	"fmt"
	"log"
	"net"
	"net/http"
	"net/rpc"
	"os"
	"sync"
	"time"
)

type CoordinatorPhase int

const (
	MAPPING CoordinatorPhase = iota
	WAITING
	REDUCING
	FINISHING
)

type workerRole int

const (
	MAPPER workerRole = iota
	REDUCER
)

type Mapper struct {
}

type Coordinator struct {
	// Your definitions here.

	mapTasks                   map[int]string
	mapTasksPending            []string
	mapTasksFinished           []string
	reduceTasksPendingCounter  int
	reduceTasksFinishedCounter int
	workerLists                []string
	CoordinatorPhase
	nMap         int
	nReduce      int
	sendTaskLock sync.Mutex
	waitingStart time.Time
	waitingEnd   time.Time
}

// Your code here -- RPC handlers for the worker to call.

// an example RPC handler.
//
// the RPC argument and reply types are defined in rpc.go.
func (c *Coordinator) Example(args *ExampleArgs, reply *ExampleReply) error {
	reply.Y = args.X + 1
	return nil
}

func (c *Coordinator) SendTask(args *TaskRequest, reply *TaskReceive) error {
	c.sendTaskLock.Lock()
	//	log.Printf("Calling from %s in %v mode \n", args.WorkerName, c.CoordinatorPhase)
	reply.CoordinatorPhase = c.CoordinatorPhase

	switch c.CoordinatorPhase {
	case MAPPING:
		reply.MapTask = c.provideMapTask()
	case REDUCING:
		reply.ReduceTask = c.provideReduceTask()
	case WAITING:
		if c.waitingStart.IsZero() {
			c.waitingStart = time.Now()
		} else {
			c.waitingEnd = time.Now()
			duration := c.waitingEnd.Sub(c.waitingStart)
			if duration > 3*time.Second {
				fmt.Println("timeout more than 3 seconds")
				missing := c.checkUnfinished()
				c.mapTasksPending = append(c.mapTasksPending, missing)
				c.CoordinatorPhase = MAPPING
			} else {
				fmt.Println("timeout less than 3 seconds")
			}
			time.Sleep(1 * time.Second)
		}
	}
	c.sendTaskLock.Unlock()
	return nil
}

func (c *Coordinator) checkUnfinished() string {
	temp := make(map[string]bool)

	for _, v := range c.mapTasksFinished {
		temp[v] = true
	}
	var missing string
	for _, v := range c.mapTasks {
		if !temp[v] {
			log.Printf("missing %s in finished queue \n", v)
			missing = v
		}
	}
	return missing
}

func (c *Coordinator) ReceiveResult(arg *ResultSendBack, reply *ResultAcknowledge) error {
	switch arg.WorkType {
	case MAPWORK:
		c.mapTasksFinished = append(c.mapTasksFinished, c.mapTasks[arg.FileId])
	case REDUCEWORK:
		c.reduceTasksFinishedCounter += 1
	}
	reply.Result = &Result{
		arg.Result.WorkerName,
		arg.Result.WorkType,
		DONE,
		arg.FileId,
	}
	if len(c.mapTasksFinished) == c.nMap {
		c.CoordinatorPhase = REDUCING
	}
	if c.reduceTasksFinishedCounter == c.nReduce {
		c.CoordinatorPhase = FINISHING
	}
	return nil
}

func (c *Coordinator) provideMapTask() *MapTask {
	file := c.mapTasksPending[len(c.mapTasksPending)-1]
	fileId := len(c.mapTasksPending) - 1
	c.mapTasksPending = c.mapTasksPending[0 : len(c.mapTasksPending)-1]

	if len(c.mapTasksPending) == 0 {
		c.CoordinatorPhase = WAITING
		log.Println("Entering waiting phase")
	}
	log.Printf("Remain %v in queue", len(c.mapTasksPending))
	return &MapTask{
		FileName: file,
		FileId:   fileId,
		NReduce:  c.nReduce,
	}
}
func (c *Coordinator) provideReduceTask() *ReduceTask {
	fileID := c.reduceTasksPendingCounter - 1
	c.reduceTasksPendingCounter -= 1
	var fileNames []string
	for i, _ := range c.mapTasksFinished {
		fileName := fmt.Sprintf("mr-%v-%v", i, fileID)
		// log.Printf("Providing reduce tasks for intermediate files: %s \n", fileName)
		fileNames = append(fileNames, fileName)
	}
	return &ReduceTask{
		fileNames,
		fileID,
	}
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

	// Your code here.

	return c.CoordinatorPhase == FINISHING
}

// create a Coordinator.
// main/mrcoordinator.go calls this function.
// nReduce is the number of reduce tasks to use.
func MakeCoordinator(files []string, nReduce int) *Coordinator {
	c := Coordinator{}

	// Your code here.
	c.mapTasks = make(map[int]string, len(files))
	c.mapTasksPending = make([]string, len(files))
	c.reduceTasksFinishedCounter = 0
	c.reduceTasksPendingCounter = nReduce
	c.CoordinatorPhase = MAPPING
	c.nReduce = nReduce
	for i, fileName := range files {
		c.mapTasks[i] = fileName
		c.mapTasksPending[i] = fileName
		c.nMap += 1
	}
	c.server()
	return &c
}

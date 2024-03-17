package mr

import (
	"encoding/json"
	"fmt"
	"github.com/goombaio/namegenerator"
	"hash/fnv"
	"io"
	"log"
	"net/rpc"
	"os"
	"time"
)

// Map functions return a slice of KeyValue.
type KeyValue struct {
	Key   string
	Value string
}

// for sorting by key.
type ByKey []KeyValue

// for sorting by key.
func (a ByKey) Len() int           { return len(a) }
func (a ByKey) Swap(i, j int)      { a[i], a[j] = a[j], a[i] }
func (a ByKey) Less(i, j int) bool { return a[i].Key < a[j].Key } // for sorting by key.

// use ihash(key) % NReduce to choose the reduce
// task number for each KeyValue emitted by Map.
func ihash(key string) int {
	h := fnv.New32a()
	h.Write([]byte(key))
	log.Printf("ihash s%v \n", int(h.Sum32()&0x7fffffff))
	return int(h.Sum32() & 0x7fffffff)
}

// main/mrworker.go calls this function.
func Worker(mapf func(string, string) []KeyValue,
	reducef func(string, []string) string) {

	// Your worker implementation here.
	workerName := namegenerator.NewNameGenerator(time.Now().UTC().UnixNano()).Generate()
	task := CallCoordinator(workerName)
	if a, ok := task.(*MapTask); ok {
		log.Println("Receiving map task...")
		log.Println(a.FileName)
		doMap(mapf, a)
		return
	}

	if _, ok := task.(*ReduceTask); ok {
		log.Println("Receiving reduce task...")
	}

}

func doMap(mapf func(string, string) []KeyValue, task *MapTask) {

	// read map task content to memory
	file, err := os.Open(task.FileName)
	if err != nil {
		log.Fatalf("cannot open %v \n", task.FileName)
	}
	content, err := io.ReadAll(file)
	if err != nil {
		log.Fatalf("cannot read %v", task.FileName)
	}
	if err := file.Close(); err != nil {
		log.Fatalf("cannot close %v", task.FileName)
	}

	// create key-value array
	kva := mapf(task.FileName, string(content))

	// create temp files
	if err := os.Mkdir("tmp", 0750); err != nil {
		log.Fatalf("cannot create tmp folder")
	}
	if err := os.Chdir("tmp"); err != nil {
		log.Fatalf("cannot cd tmp folder")
	}

	for i := 0; i < task.NReduce; i++ {
		intermediateFileName := fmt.Sprintf("mr-%v-%v", task.FileOrder, i)
		log.Printf("creating file %v", intermediateFileName)
		f, err := os.Create(intermediateFileName)
		if err != nil {
			log.Fatalf("cannot create %s", intermediateFileName)
		}
		if err := f.Close(); err != nil {
			log.Fatalf("cannot close %s", intermediateFileName)
		}
	}

	// save in intermediate files
	for _, kv := range kva {
		partitionNumber := ihash(kv.Key) % task.NReduce
		targetFileName := fmt.Sprintf("mr-%v-%v", task.FileOrder, partitionNumber)
		intermediateFile, err := os.OpenFile(targetFileName, os.O_RDWR|os.O_CREATE|os.O_APPEND, 0644)
		if err != nil {
			log.Fatalf("cannot open %s", targetFileName)
		}
		enc := json.NewEncoder(intermediateFile)
		if err := enc.Encode(&kv); err != nil {
			log.Fatalf("cannot write %s in %s", kv, targetFileName)
		}
	}

}

// example function to show how to make an RPC call to the coordinator.
//
// the RPC argument and reply types are defined in rpc.go.

func CallCoordinator(workerName string) interface{} {
	arg := TaskRequest{workerName}
	reply := TaskReceive{}
	ok := call("Coordinator.CallTask", &arg, &reply)
	if !ok {
		return nil
	}
	log.Printf("Coordinator in %v mode", reply.CoordinatorPhase)
	switch reply.CoordinatorPhase {
	case mapping:
		return reply.MapTask
	case reducing:
		return reply.ReduceTask
	}
	return nil
}

func CallExample() {

	// declare an argument structure.
	args := ExampleArgs{}

	// fill in the argument(s).
	args.X = 99

	// declare a reply structure.
	reply := ExampleReply{}

	// send the RPC request, wait for the reply.
	// the "Coordinator.Example" tells the
	// receiving server that we'd like to call
	// the Example() method of struct Coordinator.
	ok := call("Coordinator.Example", &args, &reply)
	if ok {
		// reply.Y should be 100.
		fmt.Printf("reply.Y %v\n", reply.Y)
	} else {
		fmt.Printf("call failed!\n")
	}
}

// send an RPC request to the coordinator, wait for the response.
// usually returns true.
// returns false if something goes wrong.
func call(rpcname string, args interface{}, reply interface{}) bool {
	// c, err := rpc.DialHTTP("tcp", "127.0.0.1"+":1234")
	sockname := coordinatorSock()
	c, err := rpc.DialHTTP("unix", sockname)
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

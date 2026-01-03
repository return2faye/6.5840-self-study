package mr

import (
	"encoding/json"
	"fmt"
	"hash/fnv"
	"io"
	"log"
	"net/rpc"
	"os"
	"sort"
	"time"
)

//
// Map functions return a slice of KeyValue.
//
type KeyValue struct {
	Key   string
	Value string
}

//
// use ihash(key) % NReduce to choose the reduce
// task number for each KeyValue emitted by Map.
//
func ihash(key string) int {
	h := fnv.New32a()
	h.Write([]byte(key))
	return int(h.Sum32() & 0x7fffffff)
}


//
// main/mrworker.go calls this function.
//
func Worker(mapf func(string, string) []KeyValue,
	reducef func(string, []string) string) {
	// RR: request for task
	for {
		args := MessageSend{}
		reply := MessageReply{}
		call("Coordinator.RequestTask", &args, &reply)

		switch reply.TaskType {
		case MapTask:
			HandleMapTask(&reply, mapf)
		case ReduceTask:
			HandleReduceTask(&reply, reducef)
		case Wait:
			time.Sleep(1 * time.Second)
		case Exit:
			os.Exit(0)
		default:
			time.Sleep(1 * time.Second)
		}
	}
}

func HandleMapTask(reply *MessageReply, mapf func(string, string) []KeyValue) {
	file, err := os.Open(reply.TaskFile)
	if err != nil {
		log.Fatalf("cannot open %v", reply.TaskFile)
		return
	}

	content, err := io.ReadAll(file)
	if err != nil {
		log.Fatalf("cannot read %v", reply.TaskFile)
		return
	}
	file.Close() // close file after read

	kva := mapf(reply.TaskFile, string(content))
	// create intermediate files data
	intermediate := make([][]KeyValue, reply.NReduce)
	for _, kv := range kva {
		r := ihash(kv.Key) % reply.NReduce
		intermediate[r] = append(intermediate[r], kv)
	}

	// write intermediate files
	for r, kva := range intermediate {
		oname := fmt.Sprintf("mr-%v-%v", reply.TaskID, r)
		ofile, err := os.CreateTemp("", oname)
		if err != nil {
			log.Fatalf("cannot create tempfile %v", oname)
		}
		enc := json.NewEncoder(ofile)
		for _, kv := range kva {
			enc.Encode(kv)
		}
		ofile.Close()
		// core of FT, rename is an atomic operation
		os.Rename(ofile.Name(), oname)
	}

	args := MessageSend{
		TaskID: reply.TaskID,
		TaskCompletedStatus: MapTaskCompleted,
	}
	call("Coordinator.ReportTask", &args, &MessageReply{})
}

// each reducer gets one bucket
func generateFileName(r int, NMap int) []string {
	var fileName []string
	for TaskID := 0; TaskID < NMap; TaskID++ {
		fileName = append(fileName, fmt.Sprintf("mr-%v-%v", TaskID, r))
	}
	return fileName
}

func HandleReduceTask(reply *MessageReply, reducef func(string, []string) string) {
	var intermediate []KeyValue

	intermediatefFiles := generateFileName(reply.TaskID, reply.NMap)

	for _, filename := range intermediatefFiles {
		file, err := os.Open(filename)
		if err != nil {
			log.Fatalf("cannot open %v", filename)
			return
		}

		dec := json.NewDecoder(file)
		for {
			kv := KeyValue{}
			if err := dec.Decode(&kv); err == io.EOF {
				break
			}
			intermediate = append(intermediate, kv)
		}
		file.Close()
	}

	// sort intermediate KV pairs by key
	sort.Slice(intermediate, func(i, j int) bool {
		return intermediate[i].Key < intermediate[j].Key
	})

	// write k-v pairs to output files
	oname := fmt.Sprintf("mr-out-%v", reply.TaskID)
	ofile, err := os.CreateTemp("", oname)
	if err != nil {
		log.Fatalf("cannot open %v", oname)
		return
	}
	for i := 0; i < len(intermediate); {
		j := i + 1
		for j < len(intermediate) && intermediate[j].Key == intermediate[i].Key {
			j++
		}
		var values []string
		for k := i; k < j; k++ {
			values = append(values, intermediate[k].Value)
		}

		output := reducef(intermediate[i].Key, values)

		fmt.Fprintf(ofile, "%v %v\n", intermediate[i].Key, output)

		i = j
	}
	ofile.Close()
	// rename the output file to the final output file
	os.Rename(ofile.Name(), oname)

	// send the task completion message to the coordinator
	args := MessageSend{
		TaskID:              reply.TaskID,
		TaskCompletedStatus: ReduceTaskCompleted,
	}
	call("Coordinator.ReportTask", &args, &MessageReply{})
}

//
// example function to show how to make an RPC call to the coordinator.
//
// the RPC argument and reply types are defined in rpc.go.
//
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

//
// send an RPC request to the coordinator, wait for the response.
// usually returns true.
// returns false if something goes wrong.
//
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

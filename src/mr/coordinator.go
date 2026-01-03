package mr

import (
	"log"
	"net"
	"net/http"
	"net/rpc"
	"os"
	"sync"
	"time"
)

// Coordinator needs to manage task status info
type TaskStatus int
const (
	Unassigned = iota
	Assigned
	Completed
	Failed
)

type TaskInfo struct {
	TaskStatus TaskStatus
	TaskFile string
	TimeStamp time.Time // indicating running time of Task
}


type Coordinator struct {
	NMap int
	NReduce int
	MapTasks []TaskInfo // store info of map tasks
	ReduceTasks []TaskInfo // store info of reduce tasks
	AllMapTaskCompleted bool
	AllReduceTaskCompleted bool
	Mutex sync.Mutex // protect shared data
}

func (c *Coordinator) InitTask(file []string) {
	for idx := range file {
		c.MapTasks[idx] = TaskInfo{
			TaskFile: file[idx],
			TaskStatus: Unassigned,
			TimeStamp: time.Now(),
		}
	}
	
	for idx := range c.ReduceTasks {
		c.ReduceTasks[idx] = TaskInfo{
			TaskStatus: Unassigned,
		}
	}
}

// Your code here -- RPC handlers for the worker to call.

//
// an example RPC handler.
//
// the RPC argument and reply types are defined in rpc.go.
//

func (c *Coordinator) RequestTask(args *MessageSend, reply *MessageReply) error {
	// lock!
	c.Mutex.Lock()
	defer c.Mutex.Unlock()

	// assign map task before reduce
	if !c.AllMapTaskCompleted {
		NMapTaskCompleted := 0
		// linear scan all tasks
		for idx, taskInfo := range c.MapTasks {
			if taskInfo.TaskStatus == Unassigned || taskInfo.TaskStatus == Failed || 
			(taskInfo.TaskStatus == Assigned) && time.Since(taskInfo.TimeStamp) > 10 * (time.Second) {
				reply.TaskID = idx
				reply.TaskType = MapTask
				reply.TaskFile = taskInfo.TaskFile
				reply.NMap = c.NMap
				reply.NReduce = c.NReduce
				c.MapTasks[idx].TaskStatus = Assigned
				c.MapTasks[idx].TimeStamp = time.Now()
				return nil
			} else if taskInfo.TaskStatus == Completed {
				NMapTaskCompleted++
			}
		}

		if NMapTaskCompleted == len(c.MapTasks) {
			c.AllMapTaskCompleted = true
		} else {
			reply.TaskType = Wait
			return nil
		}
	}

	if !c.AllReduceTaskCompleted {
		NReduceTaskCompleted := 0
		for idx, taskInfo := range c.ReduceTasks {
			if taskInfo.TaskStatus == Unassigned || taskInfo.TaskStatus == Failed ||
			(taskInfo.TaskStatus == Assigned) && (time.Since(taskInfo.TimeStamp) > 10 * time.Second) {
				reply.TaskID = idx
				reply.TaskType = ReduceTask
				reply.NMap = c.NMap
				reply.NReduce = c.NReduce
				c.ReduceTasks[idx].TaskStatus = Assigned
				c.ReduceTasks[idx].TimeStamp = time.Now()
				return nil
			} else if taskInfo.TaskStatus == Completed {
				NReduceTaskCompleted++
			}
		}

		if NReduceTaskCompleted == c.NReduce {
			c.AllReduceTaskCompleted = true
		} else {
			reply.TaskType = Wait
			return nil
		}
	}

	// all tasks completed
	reply.TaskType = Exit
	return nil
}

func (c *Coordinator) ReportTask(args *MessageSend, reply *MessageReply) error {
	c.Mutex.Lock()
	defer c.Mutex.Unlock()

	if args.TaskCompletedStatus == MapTaskCompleted {
		c.MapTasks[args.TaskID].TaskStatus = Completed
		return nil
	} else if args.TaskCompletedStatus == MapTaskFailed {
		c.MapTasks[args.TaskID].TaskStatus = Failed
		return nil
	} else if args.TaskCompletedStatus == ReduceTaskCompleted {
		c.ReduceTasks[args.TaskID].TaskStatus = Completed
		return nil
	} else if args.TaskCompletedStatus == ReduceTaskFailed {
		c.ReduceTasks[args.TaskID].TaskStatus = Failed
		return nil
	}

	return nil
}

//
// start a thread that listens for RPCs from worker.go
//
func (c *Coordinator) server() {
	rpc.Register(c)
	rpc.HandleHTTP()
	//l, e := net.Listen("tcp", ":1234")
	sockname := coordinatorSock()
	os.Remove(sockname)
	l, e := net.Listen("unix", sockname)
	if e != nil {
		log.Fatal("listen error:", e)
	}
	go http.Serve(l, nil)
}

//
// main/mrcoordinator.go calls Done() periodically to find out
// if the entire job has finished.
//
func (c *Coordinator) Done() bool {
	// Your code here.
	for _, taskInfo := range c.MapTasks {
		if taskInfo.TaskStatus != Completed {
			return false
		}
	}

	for _, taskInfo := range c.ReduceTasks {
		if taskInfo.TaskStatus != Completed {
			return false
		}
	}

	return true
}

//
// create a Coordinator.
// main/mrcoordinator.go calls this function.
// nReduce is the number of reduce tasks to use.
//
func MakeCoordinator(files []string, nReduce int) *Coordinator {
	c := Coordinator{
		NReduce: nReduce,
		NMap: len(files),
		MapTasks: make([]TaskInfo, len(files)),
		ReduceTasks: make([]TaskInfo, nReduce),
		AllMapTaskCompleted: false,
		AllReduceTaskCompleted: false,
		Mutex: sync.Mutex{},
	}

	c.InitTask(files)


	c.server()
	return &c
}

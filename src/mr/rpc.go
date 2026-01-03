package mr

//
// RPC definitions.
//
// remember to capitalize all names.
//

import "os"
import "strconv"

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

// Task Status reported to Coordinator
type TaskCompletedStatus int
const (
	MapTaskCompleted = iota
	MapTaskFailed
	ReduceTaskCompleted
	ReduceTaskFailed
)

// TaskType replied by Coordinator
type TaskType int
const (
	MapTask = iota
	ReduceTask
	Wait
	Exit
)

type MessageSend struct {
	TaskID int
	TaskCompletedStatus TaskCompletedStatus
}

type MessageReply struct {
	TaskID int
	TaskType TaskType
	TaskFile string
	NReduce int
	NMap int
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

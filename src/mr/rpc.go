package mr

//
// RPC definitions.
//
// remember to capitalize all names.
//

import "os"
import "strconv"
import "time"

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
type TaskStatus int

const (
	NOT_STARTED TaskStatus = iota
	WAITING
	DONE
)

type TaskType int

const (
	MAP TaskType = iota
	REDUCE
)

type Task struct {
	TaskStatus TaskStatus
	TaskType TaskType
	InputFiles []string
	Index int
	TimeStamp time.Time
}

type DoJob int
const (
	BREAK DoJob = iota
	WAIT
	DO
)

type ReplyTask struct {
	DoJob DoJob
	Task Task
	NReduce int
}

type CompleteMessageCode int
const (
	FAIL CompleteMessageCode = iota
	SUCCESS
)

type TaskCompleteReply struct {
	CompleteMessageCode CompleteMessageCode
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

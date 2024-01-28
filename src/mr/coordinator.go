package mr

import "log"
import "net"
import "os"
import "net/rpc"
import "net/http"
import "strconv"
import "sync"


type Coordinator struct {
	// Your definitions here.
	mu sync.Mutex
	mapTasks []Task
	reduceTasks []Task
	mapDone bool
	reduceDone bool
}

// Your code here -- RPC handlers for the worker to call.

func (c *Coordinator) AssignTask(replyTask *Task) error {
	c.mu.Lock()
	defer c.mu.Unlock()
	replyTask.TaskType = BREAK
	if !c.mapDone {
		replyTask.TaskType = MAP
	}
	else if !c.reduceDone(){
		replyTask.TaskType = REDUCE
	}
	AssignTask(replyTask)
	return nil
}

// type Task struct {
// 	TaskStatus TaskStatus
// 	TaskType TaskType
// 	InputFiles []string
// 	Index int
// 	TimeStamp time.Time
// }

func (c *Coordinator) AssignTask(replyTask *Task){
	if replyTask.TaskType==BREAK{
		return nil
	}
	var refTasks []Task
	if replyTask.TaskType==MAP{
		refTasks = c.mapTasks
	}
	else{ // in case of REDUCE
		refTasks = c.reduceTasks
	}
	for index, task := range refTasks {
		tenSecondsAgo := time.Now().Add(-10 * time.Second)
		if task.TaskStatus==NOT_STARTED || task.TaskStatus==(WAITING && task.TimeStamp.Before(tenSecondsAgo)){
			refTasks[index].TaskStatus = WAITING
			refTasks[index].TimeStamp = time.Now()
			replyTask.InputFiles = task.InputFiles
			replyTask.Index = index
			break
		}
	}
}

//
// an example RPC handler.
//
// the RPC argument and reply types are defined in rpc.go.
//
// func (c *Coordinator) Example(args *ExampleArgs, reply *ExampleReply) error {
// 	reply.Y = args.X + 1
// 	return nil
// }


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
	ret := false

	// Your code here.


	return ret
}

//
// create a Coordinator.
// main/mrcoordinator.go calls this function.
// nReduce is the number of reduce tasks to use.
//
func MakeCoordinator(files []string, nReduce int) *Coordinator {
	c := Coordinator{}

	// Your code here.
	c.mapDone = false
	c.reduceDone = false

	var mapTasks []Task
	for i:=0; i<len(files); i++{
		var task = Task{
			TaskType: MAP,
			TaskStatus: NOT_STARTED,
			Index: i,
			InputFiles: []string{files[i]},
		}
		mapTasks = append(mapTasks, task)
	}
	c.mapTasks = mapTasks

	var reduceTasks []Task
	for j:=0; j<nReduce; j++{
		var task = Task{
			TaskType: REDUCE,
			TaskStatus: NOT_STARTED,
			Index: j,
			InputFiles: getIntermediateFiles(j, len(files)),
		}
		reduceTasks = append(reduceTasks, task)
	}
	c.reduceTasks = reduceTasks

	c.mapDone = false
	c.reduceDone = false

	c.server()
	return &c
}

func getIntermediateFiles(j int, lenFiles int) []string{
	var files []string
	for i:=0; i<lenFiles; i++{
		files = append(files, "mr-intermediate-"+strconv.Itoa(i)+"-"+strconv.Itoa(j))
	}
	return files
}

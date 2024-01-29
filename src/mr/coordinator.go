package mr

import "log"
import "net"
import "os"
import "net/rpc"
import "net/http"
import "strconv"
import "sync"
import "time"

type Coordinator struct {
	// Your definitions here.
	mu sync.Mutex
	mapTasks []Task
	reduceTasks []Task
	mapDone int
	reduceDone int
	nReduce int
}

// Your code here -- RPC handlers for the worker to call.

func (c *Coordinator) RequestTask(args *ExampleArgs, replyTask *ReplyTask) error {
	// args *ExampleArgs is not used
	c.mu.Lock()
	defer c.mu.Unlock()
	if len(c.mapTasks)!=c.mapDone {
		replyTask.Task.TaskType = MAP
	}else if len(c.reduceTasks)!=c.reduceDone{
		replyTask.Task.TaskType = REDUCE
	}else{
		replyTask.DoJob = BREAK
		return nil
	}
	c.assignTask(replyTask)
	return nil
}
func (c *Coordinator) Example(args *ExampleArgs, reply *ExampleReply) error {
	reply.Y = args.X + 1
	return nil
}

func (c *Coordinator) assignTask(replyTask *ReplyTask){
	var refTasks []Task
	if replyTask.Task.TaskType==MAP{
		refTasks = c.mapTasks
	}else{ // in case of REDUCE
		refTasks = c.reduceTasks
	}
	replyTask.NReduce = c.nReduce
	replyTask.DoJob = WAIT
	for index, task := range refTasks {
		tenSecondsAgo := time.Now().Add(-10 * time.Second)
		if task.TaskStatus==NOT_STARTED || (task.TaskStatus==WAITING && task.TimeStamp.Before(tenSecondsAgo)){
			refTasks[index].TaskStatus = WAITING
			refTasks[index].TimeStamp = time.Now()
			replyTask.Task = task
			replyTask.DoJob = DO
			break
		}
	}
}

func (c *Coordinator) TaskComplete(task *Task, taskCompleteReply *TaskCompleteReply) error {
	c.mu.Lock()
	defer c.mu.Unlock()
	var refTask []Task
	if task.TaskType==MAP{
		refTask = c.mapTasks
	}else{ // task.TaskType==REDUCE
		refTask = c.reduceTasks
	}
	if refTask[task.Index].TaskStatus==DONE{
		taskCompleteReply.CompleteMessageCode = FAIL
		return nil
	}
	refTask[task.Index].TaskStatus=DONE
	if task.TaskType==MAP{
		c.mapDone += 1
	}else{ // task.TaskType==REDUCE
		c.reduceDone += 1
	}
	taskCompleteReply.CompleteMessageCode = SUCCESS
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
	c.mu.Lock()
	defer c.mu.Unlock()
	return 	len(c.mapTasks)==c.mapDone && len(c.reduceTasks)==c.reduceDone
}

//
// create a Coordinator.
// main/mrcoordinator.go calls this function.
// nReduce is the number of reduce tasks to use.
//
func MakeCoordinator(files []string, nReduce int) *Coordinator {
	
	c := Coordinator{}

	// Your code here.

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

	c.mapDone = 0
	c.reduceDone = 0
	c.nReduce = nReduce
	c.server()
	return &c
}

func getIntermediateFiles(j int, lenFiles int) []string{
	var files []string
	for i:=0; i<lenFiles; i++{
		files = append(files, "mr-"+strconv.Itoa(i)+"-"+strconv.Itoa(j))
	}
	return files
}

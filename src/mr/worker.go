package mr

import "fmt"
import "log"
import "net/rpc"
import "hash/fnv"
import "time"
import "os"
import "io/ioutil"
import "strconv"
import "encoding/json"
import "sort"

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

// for sorting by key.
type ByKey []KeyValue

// for sorting by key.
func (a ByKey) Len() int           { return len(a) }
func (a ByKey) Swap(i, j int)      { a[i], a[j] = a[j], a[i] }
func (a ByKey) Less(i, j int) bool { return a[i].Key < a[j].Key }

//
// main/mrworker.go calls this function.
//
func Worker(mapf func(string, string) []KeyValue,
	reducef func(string, []string) string) {
	CallExample()
	// Your worker implementation here.
	for {
		replyTask := CallMaster()
		fmt.Printf("task : %v\n",replyTask)
		if replyTask.DoJob==DO{
			if replyTask.Task.TaskType==MAP{
				doMap(replyTask.Task, replyTask.NReduce, mapf)
			}else{ // replyTask.Task.TaskType==REDUCE
				doReduce(replyTask.Task, reducef)
			}
		}else if replyTask.DoJob==WAIT{
			time.Sleep(1 * time.Second) 
		}else{ // replyTask.DoJob==BREAK
			break
		}
	}
	// uncomment to send the Example RPC to the coordinator.
	// CallExample()
}

func doMap(task Task, nReduce int, mapf func(string, string) []KeyValue){
	intermediates := make([][]KeyValue, nReduce)
	for i := 0; i < nReduce; i++ {
		intermediates[i] = make([]KeyValue, 0)
	}

	file, err := os.Open(task.InputFiles[0])
	if err != nil {
		log.Fatalf("cannot open %v", task.InputFiles[0])
	}
	content, err := ioutil.ReadAll(file)
	if err != nil {
		log.Fatalf("cannot read %v", task.InputFiles[0])
	}
	file.Close()
	kva := mapf(task.InputFiles[0], string(content))
	for _, kv := range(kva){
		index := ihash(kv.Key)%nReduce
		intermediates[index] = append(intermediates[index], kv)
	}
	
	for reduceIdx, intermediate := range(intermediates){
		oname := "mr-"+strconv.Itoa(task.Index)+"-"+strconv.Itoa(reduceIdx)
		ofile, _ := os.Create(oname)
		enc := json.NewEncoder(ofile)
		err := enc.Encode(&intermediate)
		if err != nil {
			log.Fatalf("cannot encode %v", ofile)
		}
		ofile.Close()
	}
	CallTaskComplete(task)
}

func doReduce(task Task, reducef func(string, []string) string){
	var kvas []KeyValue
	for _, filename := range(task.InputFiles){
		file, _ := os.Open(filename)
		dec := json.NewDecoder(file)
		var kva []KeyValue
		if err := dec.Decode(&kva); err != nil {
			break
		}
		kvas = append(kvas, kva...)
	}
	oname := "mr-out-"+strconv.Itoa(task.Index)
	ofile, _ := os.Create(oname)
	sort.Sort(ByKey(kvas))
	i := 0
	for i < len(kvas) {
		j := i + 1
		for j < len(kvas) && kvas[j].Key == kvas[i].Key {
			j++
		}
		values := []string{}
		for k := i; k < j; k++ {
			values = append(values, kvas[k].Value)
		}
		output := reducef(kvas[i].Key, values)
		fmt.Printf("k:%v, v:%v\n",kvas[i].Key,output)
		fmt.Fprintf(ofile, "%v %v\n", kvas[i].Key, output)

		i = j
	}
	ofile.Close()
	CallTaskComplete(task)
}

func CallTaskComplete(task Task){
	// declare an argument structure.
	taskCompleteReply := TaskCompleteReply{}
	ok := call("Coordinator.TaskComplete", &task, &taskCompleteReply)
	if ok && taskCompleteReply.CompleteMessageCode==SUCCESS{
		fmt.Printf("CallTaskComplete complete!\n")
	} else {
		fmt.Printf("CallTaskComplete failed!\n")
	}
}
//
// example function to show how to make an RPC call to the coordinator.
//
// the RPC argument and reply types are defined in rpc.go.
//
func CallMaster() ReplyTask{
	// declare an argument structure.
	args := ExampleArgs{}

	// fill in the argument(s).
	args.X = 99

	// declare a reply structure.
	replyTask := ReplyTask{}

	// send the RPC request, wait for the reply.
	// the "Coordinator.Example" tells the
	// receiving server that we'd like to call
	// the Example() method of struct Coordinator.
	ok := call("Coordinator.RequestTask", &args, &replyTask)
	if ok {
		return replyTask
	} else {
		fmt.Printf("CallMaster failed!\n")
		replyTask.DoJob = WAIT
	}
	return replyTask
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

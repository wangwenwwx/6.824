package mr

import (
	"bufio"
	"encoding/json"
	"fmt"
	"io/ioutil"
	"math/rand"
	"os"
	"sort"
	"strconv"
	"time"
)
import "log"
import "net/rpc"
import "hash/fnv"

//
// Map functions return a slice of KeyValue.
//
type KeyValue struct {
	Key   string
	Value string
}

// for sorting by key.
type ByKey []KeyValue

// for sorting by key.
func (a ByKey) Len() int           { return len(a) }
func (a ByKey) Swap(i, j int)      { a[i], a[j] = a[j], a[i] }
func (a ByKey) Less(i, j int) bool { return a[i].Key < a[j].Key }

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
	var reply *TaskReply
	reply = getTask()
	// Your worker implementation here.
	for reply != nil {
		if reply.MapTask != nil {
			mapTask(mapf, reply.MapTask)
		} else if reply.ReduceTask != nil {
			reduceTask(reducef, reply.ReduceTask)
		}
		reply = getTask()
	}
}
func mapTask(mapf func(string, string) []KeyValue, task *MapTask) {
	//log.Printf("%v\n",task)
	file, err := os.Open(task.File)
	if err != nil {
		log.Fatalf("cannot open %v", task.File)
	}
	content, err := ioutil.ReadAll(file)
	if err != nil {
		log.Fatalf("cannot read %v", task.File)
	}
	file.Close()
	kva := mapf(task.File, string(content))
	r := rand.New(rand.NewSource(time.Now().UnixNano()))
	uid := r.Int()
	files := make([]*os.File, task.NReduce)
	for i := 0; i < task.NReduce; i++ {
		path := fmt.Sprintf("%d_%d", uid, i)
		file, _ := os.Create(path)
		defer file.Close()
		files[i] = file
	}
	for _, value := range kva {
		index := ihash(value.Key) % task.NReduce
		b, _ := json.Marshal(value)
		files[index].WriteString(string(b) + "\n")
	}
	res := TaskResult{}
	res.Task = task.Task
	res.File = strconv.Itoa(uid)
	call("Coordinator.TaskComplete", &res, nil)
}
func reduceTask(reducef func(string, []string) string, task *ReduceTask) {
	kva := []KeyValue{}
	for i := 0; i < task.FileCount; i++ {
		path := fmt.Sprintf("mid_%d_%d", i, task.TaskId)
		file, e := os.Open(path)
		if e != nil {
			log.Fatalf("openfile", e)
		}
		defer file.Close()
		scanner := bufio.NewScanner(file)
		for scanner.Scan() {
			line := scanner.Bytes()
			kv := KeyValue{}
			json.Unmarshal(line, &kv)
			kva = append(kva, kv)
		}
	}
	sort.Sort(ByKey(kva))
	r := rand.New(rand.NewSource(time.Now().UnixNano()))
	oname := fmt.Sprintf("%d", r.Int())
	ofile, _ := os.Create(oname)

	//
	// call Reduce on each distinct key in intermediate[],
	// and print the result to mr-out-0.
	//
	i := 0
	for i < len(kva) {
		j := i + 1
		for j < len(kva) && kva[j].Key == kva[i].Key {
			j++
		}
		values := []string{}
		for k := i; k < j; k++ {
			values = append(values, kva[k].Value)
		}
		output := reducef(kva[i].Key, values)

		// this is the correct format for each line of Reduce output.
		fmt.Fprintf(ofile, "%v %v\n", kva[i].Key, output)

		i = j
	}
	ofile.Close()
	res := TaskResult{}
	res.Task = task.Task
	res.File = oname
	call("Coordinator.TaskComplete", &res, nil)
}
func getTask() *TaskReply {
	args := ExampleArgs{}
	reply := TaskReply{}
	for true {
		ok := call("Coordinator.GetTask", &args, &reply)
		if ok {
			if reply.IsAllDown {
				return nil
			} else {
				if reply.MapTask == nil && reply.ReduceTask == nil {
					time.Sleep(1e10)
				} else {
					return &reply
				}
			}
		} else {
			fmt.Printf("call failed!\n")
		}
	}
	return nil
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

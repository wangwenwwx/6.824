package mr

import (
	"fmt"
	"log"
	"math/rand"
	"sync"
	"sync/atomic"
	"time"
)
import "net"
import "os"
import "net/rpc"
import "net/http"

type Coordinator struct {
	// Your definitions here.
	listener           net.Listener
	files              []string
	nReduce            int
	mapLock            sync.Mutex
	mapTask            []*MapTask
	reduceLock         sync.Mutex
	reduceTask         []*ReduceTask
	successMapCount    int32
	successReduceCount int
	isSuccess          int64
}
type Task struct {
	TaskId    int
	TaskType  string
	Status    string
	StartTime int64
	RunningId int
}
type MapTask struct {
	File    string
	NReduce int
	Task
}
type ReduceTask struct {
	ReduceIndex int
	FileCount   int
	Task
}
type TaskResult struct {
	File string
	Task
}
type TaskReply struct {
	MapTask    *MapTask
	ReduceTask *ReduceTask
	IsAllDown  bool
}

// Your code here -- RPC handlers for the worker to call.

//
// an example RPC handler.
//
// the RPC argument and reply types are defined in rpc.go.
//
func (c *Coordinator) Example(args *ExampleArgs, reply *ExampleReply) error {
	reply.Y = args.X + 1
	return nil
}
func (c *Coordinator) GetTask(args *ExampleArgs, reply *TaskReply) error {
	reply.IsAllDown = c.Done()
	r := rand.New(rand.NewSource(time.Now().UnixNano()))
	if atomic.LoadInt32(&c.successMapCount) < int32(len(c.mapTask)) {
		c.mapLock.Lock()
		defer c.mapLock.Unlock()
		for index, task := range c.mapTask {
			//log.Printf("----%v\n",task)
			if task.Status == "new" {
				task.TaskId = index
				task.Status = "running"
				task.StartTime = time.Now().Unix()
				task.RunningId = r.Int()
				reply.MapTask = task

				//log.Printf("%v\n",task)
				return nil
			} else if task.Status == "running" {
				seconds := time.Now().Unix() - task.StartTime
				if seconds > 10 {
					task.TaskId = index
					task.Status = "running"
					task.StartTime = time.Now().Unix()
					task.RunningId = r.Int()
					reply.MapTask = task
					//log.Printf("%v\n",task)
					return nil
				}
			}
		}
	} else {
		c.reduceLock.Lock()
		defer c.reduceLock.Unlock()
		for index, task := range c.reduceTask {
			if task.Status == "new" {
				task.TaskId = index
				task.Status = "running"
				task.StartTime = time.Now().Unix()
				task.RunningId = r.Int()
				reply.ReduceTask = task

				//log.Printf("getTask%v\n",task)
				return nil
			} else if task.Status == "running" {
				seconds := time.Now().Unix() - task.StartTime
				if seconds > 10 {
					task.TaskId = index
					task.Status = "running"
					task.StartTime = time.Now().Unix()
					task.RunningId = r.Int()
					reply.ReduceTask = task

					//log.Printf("getTask%v\n",task)
					return nil
				}
			}
		}
	}
	return nil
}
func (c *Coordinator) TaskComplete(result *TaskResult, reply *ExampleReply) error {
	if time.Now().Unix()-result.StartTime > 10 {
		return nil
	}

	//defer log.Printf("%d_%d",c.successMapCount,c.successReduceCount)
	//log.Printf("result---%v\n",result)
	if result.TaskType == "map" {
		c.mapLock.Lock()
		defer c.mapLock.Unlock()
		if c.mapTask[result.TaskId].RunningId != result.RunningId || c.mapTask[result.TaskId].Status == "success" {
			return nil
		}
		//log.Printf("task---%v\n",result)
		for i := 0; i < c.nReduce; i++ {
			oldPath := fmt.Sprintf("%s_%d", result.File, i)
			newPath := fmt.Sprintf("mid_%d_%d", result.TaskId, i)
			os.Rename(oldPath, newPath)
		}
		c.mapTask[result.TaskId].Status = "success"
		atomic.AddInt32(&c.successMapCount, 1)
		if c.successMapCount == int32(len(c.mapTask)) {
			c.reduceLock.Lock()
			defer c.reduceLock.Unlock()
			reduceTask := make([]*ReduceTask, c.nReduce)
			for i := 0; i < c.nReduce; i++ {
				reduceTask[i] = &ReduceTask{}
				reduceTask[i].ReduceIndex = i
				reduceTask[i].TaskType = "reduce"
				reduceTask[i].TaskId = i
				reduceTask[i].FileCount = len(c.files)
				reduceTask[i].Status = "new"
			}
			c.reduceTask = reduceTask
		}
	} else {
		c.reduceLock.Lock()
		defer c.reduceLock.Unlock()
		if c.reduceTask[result.TaskId].RunningId != result.RunningId || c.reduceTask[result.TaskId].Status == "success" {
			return nil
		}
		oldPath := fmt.Sprintf("%s", result.File)
		newPath := fmt.Sprintf("mr-out-%d", result.TaskId)
		os.Rename(oldPath, newPath)
		c.reduceTask[result.TaskId].Status = "success"
		c.successReduceCount++
		if c.successReduceCount == c.nReduce {
			//c.close()
			atomic.StoreInt64(&c.isSuccess, 1)
		}
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
	c.listener = l
}

func (c *Coordinator) close() {
	c.listener.Close()
}

//
// main/mrcoordinator.go calls Done() periodically to find out
// if the entire job has finished.
//
func (c *Coordinator) Done() bool {
	return atomic.LoadInt64(&c.isSuccess) == 1
}

//
// create a Coordinator.
// main/mrcoordinator.go calls this function.
// nReduce is the number of reduce tasks to use.
//
func MakeCoordinator(files []string, nReduce int) *Coordinator {
	c := Coordinator{}
	// Your code here.
	c.files = files
	c.isSuccess = 0
	c.nReduce = nReduce
	c.mapTask = make([]*MapTask, len(files))
	for i := 0; i < len(files); i++ {
		c.mapTask[i] = &MapTask{}
		c.mapTask[i].TaskId = i
		c.mapTask[i].TaskType = "map"
		c.mapTask[i].Status = "new"
		c.mapTask[i].File = files[i]
		c.mapTask[i].NReduce = nReduce

		//log.Printf("%v\n", c.mapTask[i])
	}
	c.server()
	return &c
}

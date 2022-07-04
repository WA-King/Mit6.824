package mr

import (
	"fmt"
	"log"
	"net"
	"net/http"
	"net/rpc"
	"os"
	"sync"
	"time"
)

const (
	ready = iota
	finished
)

const (
	mapTask = iota
	reduceTask
	waitTask
	emptyTask
)

type Task struct {
	id         int
	TaskType   int
	inputfiles []string
	startTime  time.Time
}
type InfoTable struct {
	cnt        int
	finish     []bool
	outputfile []string
}

func (c *InfoTable) addTask() int {
	c.finish = append(c.finish, false)
	c.outputfile = append(c.outputfile, "")
	c.cnt += 1
	return c.cnt - 1
}

type Coordinator struct {
	files     []string
	workercnt int
	nMap      int
	nReduce   int
	isFinish  bool
	TaskType  int
	taskQueue chan Task
	taskInfo  InfoTable
	lock      sync.Mutex
}

// Your code here -- RPC handlers for the worker to call.
func (c *Coordinator) GetTask(args *AskArgs, reply *AskReply) error {
	for !c.Done() {
		select {
		case x := <-c.taskQueue:
			if c.taskInfo.finish[x.id] == false {
				if time.Now().Before(x.startTime) {
					reply.TaskType = waitTask
					c.taskQueue <- x
					return nil
				} else {
					reply.File = x.inputfiles
					reply.TaskType = x.TaskType
					reply.Workid = x.id
					if x.TaskType == reduceTask {
						reply.Workid -= c.nMap
					}
					x.startTime = time.Now().Add(10 * time.Second)
					c.taskQueue <- x
					return nil
				}
			}
		default:
			reply.TaskType = waitTask
			return nil
		}
	}
	reply.TaskType = emptyTask
	return nil
}

func (c *Coordinator) ReportTask(args *ReportArgs, reply *ReportReply) error {
	c.lock.Lock()
	defer c.lock.Unlock()
	if args.TaskType == reduceTask {
		args.Id += c.nMap
	}
	reply.Success = false
	if !c.taskInfo.finish[args.Id] {
		c.taskInfo.finish[args.Id] = true
		if c.TaskType == reduceTask {
			os.Rename(args.File, fmt.Sprint("mr-out-", args.Id-c.nMap))
			args.File = fmt.Sprint("mr-out-", args.Id-c.nMap)
		}
		c.taskInfo.outputfile[args.Id] = args.File
		reply.Success = true
		tmp := true
		for _, x := range c.taskInfo.finish {
			tmp = tmp && x
		}
		c.isFinish = tmp
	}
	return nil
}

//
// an example RPC handler.
//
// the RPC argument and reply types are defined in rpc.go.
//
func (c *Coordinator) Example(args *ExampleArgs, reply *ExampleReply) error {
	c.lock.Lock()
	defer c.lock.Unlock()
	c.workercnt += 1
	reply.Id = c.workercnt
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
	c.lock.Lock()
	defer c.lock.Unlock()
	ret := false
	if c.isFinish {
		if c.TaskType == reduceTask {
			ret = true
			os.Remove("tmpfile*")
		} else {
			c.TaskType = reduceTask
			c.isFinish = false
			c.initReduce()
		}
	}
	return ret
}
func (c *Coordinator) initReduce() {
	for i := 0; i < c.nReduce; i++ {
		c.taskQueue <- Task{c.taskInfo.addTask(), reduceTask, c.taskInfo.outputfile[:c.nMap], time.Now().Add(-10 * time.Second)}
	}
}

//
// create a Coordinator.
// main/mrcoordinator.go calls this function.
// nReduce is the number of reduce tasks to use.
//
func MakeCoordinator(files []string, nReduce int) *Coordinator {
	c := Coordinator{
		files:     files,
		workercnt: 0,
		nMap:      len(files),
		nReduce:   nReduce,
		isFinish:  false,
		TaskType:  mapTask,
		taskQueue: make(chan Task, 100)}
	for _, fileName := range files {
		c.taskQueue <- Task{c.taskInfo.addTask(), mapTask, []string{fileName}, time.Now().Add(-10 * time.Second)}
	}
	c.server()
	return &c
}

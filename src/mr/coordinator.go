package mr

import (
	"fmt"
	"log"
	"net"
	"net/http"
	"net/rpc"
	"os"
	"strconv"
	"sync"
	"time"
)

const (
	NotGiven = iota
	Given
	Finished
)

var MapChan chan MapTask
var ReduceChan chan ReduceTask

type Coordinator struct {
	// Your definitions here.
	FileNames        []string
	MapDone          bool
	ReduceDone       bool
	MapTaskStatus    map[string]int
	ReduceTaskStatus map[int]int
	NReduce          int
	Lock             *sync.RWMutex
}

// Your code here -- RPC handlers for the worker to call.
//
// the RPC argument and reply types are defined in rpc.go.
//
func (c *Coordinator) FetchTask(args *FetchTaskArgs, reply *FetchTaskReply) error {
	// fmt.Println("(coor) Fetching...", args.Msg)
	switch args.Msg {
	case MsgTask:
		select {
		case t := <-MapChan:
			// give map task
			c.Lock.Lock()
			// fmt.Println("gave out:", t.Filename)
			c.MapTaskStatus[t.Filename] = Given
			c.Lock.Unlock()

			task := Task{
				MapTask:  t,
				Tasktype: "MapTask",
				NReduce:  c.NReduce,
			}
			reply.TaskObj = task

			// go routine to keep track of time for this worker
			go c.timer(task)

		case t := <-ReduceChan:
			// give reduce task
			c.Lock.Lock()
			// fmt.Println(">> gave out reduce id:", t.Id)
			c.ReduceTaskStatus[t.Id] = Given
			c.Lock.Unlock()

			task := Task{
				ReduceTask: t,
				Tasktype:   "ReduceTask",
				MapNum:     len(c.FileNames),
			}
			reply.TaskObj = task

			// go routine to keep track of time for this worker
			go c.timer(task)
		}

		return nil

	case MsgFinishedMap:

		// mark map task as finished
		c.Lock.Lock()
		defer c.Lock.Unlock()
		c.MapTaskStatus[args.MsgString] = Finished
		// fmt.Println("** Received finished filename:", args.MsgString)

		return nil

	case MsgFinishedReduce:

		// mark reduce task as finished
		taskID, _ := strconv.Atoi(args.MsgString)
		c.Lock.Lock()
		defer c.Lock.Unlock()
		c.ReduceTaskStatus[taskID] = Finished
		// fmt.Println(" ** Received finished reduce ID:", taskID)

		return nil
	}
	return nil
}

func (c *Coordinator) timer(t Task) {
	ticker := time.NewTicker(10 * time.Second)
	defer ticker.Stop()
	for {
		select {
		case <-ticker.C:
			// if ticker finishes 10 secs of ticking
			// mark task as NotGiven again
			if t.Tasktype == "MapTask" {
				c.Lock.Lock()
				c.MapTaskStatus[t.MapTask.Filename] = NotGiven
				c.Lock.Unlock()
				AddMapTask(t.MapTask.Id, t.MapTask.Filename, c)
				// fmt.Println("!! reset map:", t.MapTask.Filename)
				// fmt.Println(c.MapTaskStatus)
			} else if t.Tasktype == "ReduceTask" {
				c.Lock.Lock()
				c.ReduceTaskStatus[t.ReduceTask.Id] = NotGiven
				c.Lock.Unlock()
				AddReduceTask(t.ReduceTask.Id, c)
				// fmt.Println("!! reset reduce:", t.ReduceTask.Id)
				// fmt.Println(c.ReduceTaskStatus)
			}
			return

		default:
			// either ticker hasn't reached 10 secs
			// or task could be done at this moment
			if t.Tasktype == "MapTask" {
				c.Lock.RLock()
				if c.MapTaskStatus[t.MapTask.Filename] == Finished {
					// if task finished, end this function
					c.Lock.RUnlock()
					return
				} else {
					// not finished yet, keep ticking and checking
					c.Lock.RUnlock()
				}
			} else if t.Tasktype == "ReduceTask" {
				c.Lock.RLock()
				if c.ReduceTaskStatus[t.ReduceTask.Id] == Finished {
					c.Lock.RUnlock()
					return
				} else {
					c.Lock.RUnlock()
				}
			}
		}
	}
}

//
// start a thread that listens for RPCs from worker.go
//
func (c *Coordinator) server() {
	MapChan = make(chan MapTask, 5)
	ReduceChan = make(chan ReduceTask, 5)

	rpc.Register(c)
	rpc.HandleHTTP()

	go TaskHelper(c)

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
	c.Lock.RLock()
	ret = c.ReduceDone
	c.Lock.RUnlock()
	return ret
}

type Task struct {
	Tasktype string
	NReduce  int
	MapNum   int
	MapTask
	ReduceTask
}

type MapTask struct {
	Id       int
	Filename string
}

type ReduceTask struct {
	Id int
}

//
// create a Coordinator.
// main/mrcoordinator.go calls this function.
// nReduce is the number of reduce tasks to use.
//
func MakeCoordinator(files []string, nReduce int) *Coordinator {
	c := Coordinator{
		FileNames:        files,
		NReduce:          nReduce,
		MapDone:          false,
		ReduceDone:       false,
		MapTaskStatus:    make(map[string]int),
		ReduceTaskStatus: make(map[int]int),
		Lock:             new(sync.RWMutex),
	}

	fmt.Println("made coordinator...")

	for _, file := range files {
		c.MapTaskStatus[file] = NotGiven
	}

	for i := 0; i < nReduce; i++ {
		c.ReduceTaskStatus[i] = NotGiven
	}

	c.server()
	return &c
}

func TaskHelper(c *Coordinator) {

	// add map tasks
	index := 0
	for _, filename := range c.FileNames {
		AddMapTask(index, filename, c)
		index += 1
	}

	ok := false
	for !ok {
		ok = CheckMapTasks(c)
	}

	fmt.Println("MAP TASKS ALL DONE")
	c.Lock.Lock()
	c.MapDone = true
	c.Lock.Unlock()

	// add reduce tasks
	for i := 0; i < c.NReduce; i++ {
		AddReduceTask(i, c)
	}

	ok = false
	for !ok {
		ok = CheckReduceTasks(c)
	}

	fmt.Println("REDUCE TASKS ALL DONE")
	c.Lock.Lock()
	c.ReduceDone = true
	c.Lock.Unlock()
}

func AddMapTask(index int, filename string, c *Coordinator) {
	mtask := MapTask{
		Id:       index,
		Filename: filename,
	}
	MapChan <- mtask
}

func AddReduceTask(AddReduceIndex int, c *Coordinator) {
	rtask := ReduceTask{
		Id: AddReduceIndex,
	}
	ReduceChan <- rtask
}

func CheckMapTasks(c *Coordinator) bool {
	c.Lock.RLock()
	defer c.Lock.RUnlock()
	for _, t := range c.MapTaskStatus {
		if t != Finished {
			return false
		}
	}
	return true
}

func CheckReduceTasks(c *Coordinator) bool {
	c.Lock.RLock()
	defer c.Lock.RUnlock()
	for _, t := range c.ReduceTaskStatus {
		if t != Finished {
			return false
		}
	}
	return true
}

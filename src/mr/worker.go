package mr

import (
	"encoding/json"
	"fmt"
	"hash/fnv"
	"io/ioutil"
	"log"
	"net/rpc"
	"os"
	"sort"
	"strconv"
)

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

	// MapReduce
	for {
		task := FetchTask(MsgTask, "")
		if task.Tasktype == "" {
			break
		}
		switch task.Tasktype {
		case "MapTask":

			WorkerMap(task, mapf)
			// fmt.Println("finished task", task.Filename)

		case "ReduceTask":

			WorkerReduce(task, reducef)
			// fmt.Println("finished task")
		}
	}
}

func WorkerMap(task Task, mapf func(string, string) []KeyValue) {
	// fmt.Println("<< got mtask", task.MapTask.Filename)
	filename := task.MapTask.Filename

	// read the pg-__.txt file
	file, _ := os.Open(filename)
	content, _ := ioutil.ReadAll(file)
	file.Close()

	// perform map and divide into NReduce files
	kva := mapf(filename, string(content))
	divided := DivideReduce(kva, task.NReduce)

	id := strconv.Itoa(task.MapTask.Id)

	for i := 0; i < task.NReduce; i++ {
		// create temp file
		tempFile, err := ioutil.TempFile("", "map-temp")
		if err != nil {
			fmt.Print("Error creating map temp file:", err)
			return
		}
		defer tempFile.Close()

		Rid := strconv.Itoa(i)

		// actual save file name
		save_file_name := "mr-" + id + "-" + Rid
		// save_file, err := os.Create(save_file_name)
		// if err != nil {
		// 	fmt.Println("Error creating file: ", err)
		// 	return
		// }

		// save pairs into the file
		enc := json.NewEncoder(tempFile)
		for _, kv := range divided[i] {
			err = enc.Encode(&kv)
			if err != nil {
				fmt.Println("Error encoding JSON: ", err)
				return
			}
		}

		// rename into actual name
		err = os.Rename(tempFile.Name(), save_file_name)
		if err != nil {
			fmt.Println("Error renaming temp file:", err)
			return
		}
	}

	_ = FetchTask(MsgFinishedMap, task.MapTask.Filename)
	// fmt.Println("SENT FINSIHED MSG FOR:", task.MapTask.Filename)
}

func WorkerReduce(task Task, reducef func(string, []string) string) {
	// fmt.Println("got reduce task id:", task.ReduceTask.Id)
	kva := []KeyValue{}

	Rid := strconv.Itoa(task.ReduceTask.Id)
	for i := 0; i < task.MapNum; i++ {
		Mid := strconv.Itoa(i)
		open_file_path := "mr-" + Mid + "-" + Rid
		// fmt.Println("Opening: ", open_file_path)
		open_file, err := os.Open(open_file_path)
		if err != nil {
			fmt.Println("Error opening file:", err)
			return
		}
		dec := json.NewDecoder(open_file)
		for {
			var kv KeyValue
			if err := dec.Decode(&kv); err != nil {
				break
			}
			kva = append(kva, kv)
		}
		open_file.Close()
	}

	// fmt.Println("len of kva: ", strconv.Itoa(len(kva)))

	sort.Sort(ByKey(kva))

	oname := "mr-out-" + Rid

	// temp file for reduce output
	tempFile, err := ioutil.TempFile("", "reduce-temp")
	if err != nil {
		fmt.Println("Error making reduce temp file:", err)
		return
	}
	defer tempFile.Close()
	// ofile, _ := os.Create(oname)

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
		fmt.Fprintf(tempFile, "%v %v\n", kva[i].Key, output)

		i = j
	}

	err = os.Rename(tempFile.Name(), oname)
	if err != nil {
		fmt.Println("Error renaming reduce temp file:", err)
		return
	}

	_ = FetchTask(MsgFinishedReduce, Rid)
}

func DivideReduce(kva []KeyValue, nReduce int) [][]KeyValue {
	divided := make([][]KeyValue, nReduce)
	for _, kv := range kva {
		hash := ihash(kv.Key) % nReduce
		divided[hash] = append(divided[hash], kv)
	}
	return divided
}

func FetchTask(msg int, msgstring string) Task {
	// fmt.Println("(worker) fetching...", msg)
	// declare an argument structure.
	args := FetchTaskArgs{
		Msg:       msg,
		MsgString: msgstring,
	}

	// declare a reply structure.
	reply := FetchTaskReply{}

	// call FetchTask from coordinator
	ok := call("Coordinator.FetchTask", &args, &reply)
	if !ok {
		fmt.Println("call failed!!!! or no more tasks")
		return Task{}
	}

	return reply.TaskObj
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

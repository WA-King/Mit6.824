package mr

import (
	"fmt"
	"hash/fnv"
	"io/ioutil"
	"log"
	"net/rpc"
	"os"
	"path"
	"sort"
	"strconv"
	"strings"
	"time"
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

type Wk struct {
	mapf     func(string, string) []KeyValue
	reducef  func(string, []string) string
	workerid int
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

//
// main/mrworker.go calls this function.
//
func Worker(mapf func(string, string) []KeyValue,
	reducef func(string, []string) string) {
	//get workerid
	me := Wk{}
	args := ExampleArgs{}
	reply := ExampleReply{}
	call("Coordinator.Example", &args, &reply)
	me.workerid = reply.Id
	me.mapf = mapf
	me.reducef = reducef
	for {
		filename, Type, workid := getTask()
		if Type == emptyTask {
			break
		}
		switch Type {
		case mapTask:
			{
				output := me.doMap(filename[0], workid)
				args := ReportArgs{Id: workid, TaskType: mapTask, File: output}
				reply := ReportReply{}
				call("Coordinator.ReportTask", &args, &reply)
			}
		case reduceTask:
			{
				output := me.doReduce(filename, workid)
				args := ReportArgs{Id: workid, TaskType: reduceTask, File: output}
				reply := ReportReply{}
				call("Coordinator.ReportTask", &args, &reply)
				if !reply.Success {
					os.Remove(output)
				}
			}
		case waitTask:
			time.Sleep(time.Second)
		default:
		}
	}
	// uncomment to send the Example RPC to the coordinator.
	// CallExample()

}

func (c *Wk) doMap(filename string, workid int) string {
	file, err := os.Open(filename)
	if err != nil {
		log.Fatalf("cannot open %v", filename)
	}
	content, err := ioutil.ReadAll(file)
	if err != nil {
		log.Fatalf("cannot read %v", filename)
	}
	file.Close()
	kva := c.mapf(filename, string(content))
	ans := make([][]KeyValue, 10)
	for _, x := range kva {
		reduceId := ihash(x.Key) % 10
		ans[reduceId] = append(ans[reduceId], x)
	}
	root := path.Join("mr", strconv.Itoa(c.workerid), strconv.Itoa(workid))
	os.MkdirAll(root, os.ModePerm)
	for index, x := range ans {
		output := path.Join(root, strconv.Itoa(index))
		outfile, err := os.Create(output)
		if err != nil {
			panic(err)
		}
		for _, y := range x {
			fmt.Fprintf(outfile, "%v\n%v\n", y.Key, y.Value)
		}
		outfile.Close()
	}
	return root
}

func (c *Wk) doReduce(filename []string, workid int) string {
	all := []KeyValue{}
	root, _ := os.Getwd()
	for _, file := range filename {
		inputfile := path.Join(root, file, strconv.Itoa(workid))
		tmp := c.readReduceFile(inputfile)
		all = append(all, tmp...)
	}
	sort.Sort(ByKey(all))
	oname := fmt.Sprint("tmpfile-", c.workerid, "_", workid)
	ofile, _ := os.Create(oname)
	i := 0
	for i < len(all) {
		j := i + 1
		for j < len(all) && all[j].Key == all[i].Key {
			j++
		}
		values := []string{}
		for k := i; k < j; k++ {
			values = append(values, all[k].Value)
		}
		output := c.reducef(all[i].Key, values)

		// this is the correct format for each line of Reduce output.
		fmt.Fprintf(ofile, "%v %v\n", all[i].Key, output)

		i = j
	}
	ofile.Close()
	return oname
}
func (c *Wk) readReduceFile(filename string) []KeyValue {
	content, err := ioutil.ReadFile(filename)
	if err != nil {
		log.Fatalf("cannot read %v", filename)
	}
	words := strings.Split(string(content), "\n")
	var ans []KeyValue
	n := len(words)
	for i := 0; i < n-1; i += 2 {
		ans = append(ans, KeyValue{words[i], words[i+1]})
	}
	return ans
}
func getTask() ([]string, int, int) {
	args := AskArgs{}
	reply := AskReply{}
	call("Coordinator.GetTask", &args, &reply)
	return reply.File, reply.TaskType, reply.Workid
}

//
// example function to show how to make an RPC call to the coordinator.
//
// the RPC argument and reply types are defined in rpc.go.
//
func CallExample() {

	// declare an argument structure.
	args := ExampleArgs{}

	// declare a reply structure.
	reply := ExampleReply{}

	// send the RPC request, wait for the reply.
	call("Coordinator.Example", &args, &reply)
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

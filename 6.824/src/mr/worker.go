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
	wRequest := WorkRequest{REQUESTOP: NONEOP}
	wReply := WorkReply{}
	for {
		call("Master.Request", &wRequest, &wReply)
		wRequest.REQUESTOP = wReply.REPLYOP
		switch wReply.REPLYOP {
		case MAPOP:
			{
				filename := wReply.FILENAME
				file, err := os.Open(filename)
				if err != nil {
					log.Fatalf("cannot open %v", filename)
				}
				content, err := ioutil.ReadAll(file)
				if err != nil {
					log.Fatalf("cannot read %v", filename)
				}
				file.Close()
				kva := mapf(filename, string(content))
				var onames []string
				var ofiles []*os.File
				var encs []*json.Encoder
				nReduce := wReply.NREDUCE

				for i := 0; i < nReduce; i++ {
					name := "mr-" + strconv.Itoa(wReply.MAPID) + strconv.Itoa(i)
					file, err = os.Create(name + ".temp")
					if err != nil {
						//TODO:handle map too long
						log.Fatal("have some file %v", err)
					}
					onames = append(onames, name)
					ofiles = append(ofiles, file)
					encs = append(encs, json.NewEncoder(file))
				}
				for _, kv := range kva {
					reduceid := ihash(kv.Key) % nReduce
					//fmt.Fprintf(ofiles[reduceid], "%v %v\n", kv.Key, kv.Value)
					err := encs[reduceid].Encode(&kv)
					if err != nil {
						log.Fatal("error in encode %v", err)
					}
				}
				wRequest.FR = make([]FileReduceID, 0)
				for i := 0; i < nReduce; i++ {
					ofiles[i].Close()
					os.Rename(onames[i]+".temp", onames[i])
					wRequest.FR = append(wRequest.FR, FileReduceID{onames[i], i})
				}
				wRequest.FILENAME = filename
			}
		case REDUCEOP:
			{
				ifileNames := wReply.REDUCEFILES
				reduceID := wReply.REDUCEID
				oname := "mr-out-" + strconv.Itoa(reduceID)
				ofile, err := os.Create(oname)
				if err != nil {
					//TODO:handle map too long
					log.Fatal("have some file %v", err)
				}
				kva := []KeyValue{}
				for _, ifileName := range ifileNames {
					file, err := os.Open(ifileName)
					if err != nil {
						log.Fatalf("cannot open %v", ifileName)
					}
					dec := json.NewDecoder(file)
					for {
						var kv KeyValue
						if err := dec.Decode(&kv); err != nil {

							break
						}
						kva = append(kva, kv)
					}
					file.Close()
				}

				sort.Sort(ByKey(kva))

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

			}
		case NONEOP:
			{
				time.Sleep(time.Second)

			}
		default:
			{
				log.Fatal("unkonwn op:", wReply.REPLYOP)
			}

		}
	}

	// Your worker implementation here.

	// uncomment to send the Example RPC to the master.
	//CallExample()

}

//
// example function to show how to make an RPC call to the master.
//
// the RPC argument and reply types are defined in rpc.go.
//
func CallExample() {

	// declare an argument structure.
	args := ExampleArgs{}

	// fill in the argument(s).
	args.X = 99

	// declare a reply structure.
	reply := ExampleReply{}

	// send the RPC request, wait for the reply.
	call("Master.Example", &args, &reply)

	// reply.Y should be 100.
	fmt.Printf("reply.Y %v\n", reply.Y)
}

//
// send an RPC request to the master, wait for the response.
// usually returns true.
// returns false if something goes wrong.
//
func call(rpcname string, args interface{}, reply interface{}) bool {
	//c, err := rpc.DialHTTP("tcp", "127.0.0.1"+":1234")
	sockname := masterSock()
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

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

	"../mytools/linkedmap"
)

//task states
const (
	WAITING int = 1
	WORKING int = 2
	END     int = 3
)

type TaskState struct {
	name      string
	beginTime int64
	state     int
}

type Master struct {

	// Your definitions here.
	nReduce               int
	unprocessFileNames    []string
	fileNames             []string
	reduceID              int
	mapID                 int
	reduceSuccessCnt      int
	intermediateFileNames [][]string
	reduceMutex           sync.Mutex
	mapMutex              sync.Mutex
	mapTasks              *linkedmap.LinkMap
	reduceTasks           *linkedmap.LinkMap
}

func GetTime() int64 {
	return time.Now().Unix()
}

func DecTime(t int64) int64 {
	return GetTime() - t
}

// Your code here -- RPC handlers for the worker to call.

//
// an example RPC handler.
//
// the RPC argument and reply types are defined in rpc.go.
//
func (m *Master) Example(args *ExampleArgs, reply *ExampleReply) error {
	reply.Y = args.X + 1
	return nil
}

func (m *Master) Request(wrequest *WorkRequest, wreply *WorkReply) error {
	switch wrequest.REQUESTOP {
	case MAPOP:
		{
			name := wrequest.FILENAME
			m.mapMutex.Lock()
			mState := m.mapTasks.GetItem(name).Value.(*TaskState)
			switch mState.state {
			case WAITING:
				{
					log.Fatalf("I haven't set this state")
				}
			case WORKING:
				{
					mState.state = END
					m.mapTasks.RemoveByKey(name)
				}
			case END:
				{
					fmt.Println("this map task overtime before , handling %v", name)
				}
			default:
				{
					log.Fatal("wrong file state")
				}
			}
			m.mapMutex.Unlock()
			for _, FR := range wrequest.FR {
				m.intermediateFileNames[FR.REDUCEID] = append(m.intermediateFileNames[FR.REDUCEID], FR.FILENAME)
			}
		}
	case REDUCEOP:
		{
			m.reduceMutex.Lock()
			name := wrequest.FILENAME
			rState := m.reduceTasks.GetItem(name).Value.(*TaskState)
			switch rState.state {
			case WAITING:
				{
					log.Fatalf("I haven't set this state")
				}
			case WORKING:
				{
					m.reduceSuccessCnt++
					rState.state = END
					m.reduceTasks.RemoveByKey(name)
				}
			case END:
				{
					fmt.Println("this reduce task overtime before , handling %v", name)
				}
			default:
				{
					log.Fatal("wrong file state")
				}
			}

			m.reduceMutex.Unlock()
		}
	case NONEOP:
		{

		}
	case ENDOP:
		{
			fmt.Println("I think never will be here")
		}
	default:
		{
			log.Fatal("unkonwn op:", wrequest.REQUESTOP)
		}
	}

	if len(m.unprocessFileNames) != 0 {
		name := m.unprocessFileNames[0]
		m.mapMutex.Lock()
		m.unprocessFileNames = m.unprocessFileNames[1:]

		m.mapTasks.Insert(name, &TaskState{name, GetTime(), WORKING})

		wreply.FILENAME = name
		wreply.REPLYOP = MAPOP
		wreply.NREDUCE = m.nReduce
		wreply.MAPID = m.mapID
		m.mapID++
		m.mapMutex.Unlock()
	} else if m.mapTasks.Len() != 0 {
		// waiting to reduce
		m.mapMutex.Lock()
		top := m.mapTasks.Front().Value.(*TaskState)

		if DecTime(top.beginTime) >= 10 {
			name := top.name
			m.mapTasks.RemoveByKey(name)
			m.mapTasks.Insert(name, &TaskState{name, GetTime(), WORKING})
			wreply.FILENAME = name
			wreply.REPLYOP = MAPOP
			wreply.NREDUCE = m.nReduce
			wreply.MAPID = m.mapID
			m.mapID++
		} else {
			wreply.REPLYOP = NONEOP
		}
		m.mapMutex.Unlock()
	} else {
		//reduce
		if m.reduceID == 0 {
			fmt.Println("map task all done!")
		}
		if m.reduceID != m.nReduce {
			name := strconv.Itoa(m.reduceID)
			m.reduceTasks.Insert(name, &TaskState{name, GetTime(), WORKING})
			wreply.REDUCEID = m.reduceID
			wreply.REPLYOP = REDUCEOP
			wreply.REDUCEFILES = m.intermediateFileNames[m.reduceID]
			m.reduceID++
		} else if m.reduceTasks.Len() != 0 {
			top := m.reduceTasks.Front().Value.(*TaskState)
			if DecTime(top.beginTime) >= 10 {
				name := top.name
				m.reduceTasks.RemoveByKey(name)
				m.reduceTasks.Insert(name, &TaskState{name, GetTime(), WORKING})
				var err error
				wreply.REDUCEID, err = strconv.Atoi(name)
				if err != nil {
					log.Fatalf("can't convert reduceid %v", name)
				}
				wreply.REPLYOP = REDUCEOP
				wreply.REDUCEFILES = m.intermediateFileNames[wreply.REDUCEID]
			} else {
				wreply.REPLYOP = NONEOP
			}

		} else {
			wreply.REPLYOP = ENDOP
		}
	}

	return nil
}

//
// start a thread that listens for RPCs from worker.go
//
func (m *Master) server() {
	rpc.Register(m)
	rpc.HandleHTTP()
	// l, e := net.Listen("tcp", "127.0.0.1"+":1234")
	sockname := masterSock()
	os.Remove(sockname)
	l, e := net.Listen("unix", sockname)
	if e != nil {
		log.Fatal("listen error:", e)
	}

	go http.Serve(l, nil)
}

//
// main/mrmaster.go calls Done() periodically to find out
// if the entire job has finished.
//
func (m *Master) Done() bool {
	ret := m.reduceSuccessCnt == m.nReduce
	//fmt.Println(m.reduceSuccessCnt)
	// Your code here.

	return ret
}

//
// create a Master.
// main/mrmaster.go calls this function.
// nReduce is the number of reduce tasks to use.
//
func MakeMaster(files []string, nReduce int) *Master {
	m := Master{}

	// Your code here.
	m.nReduce = nReduce
	m.intermediateFileNames = make([][]string, nReduce)
	for i := 0; i < nReduce; i++ {
		m.intermediateFileNames[i] = make([]string, 0)
	}
	m.unprocessFileNames = files
	m.fileNames = files
	m.reduceID = 0
	m.reduceSuccessCnt = 0
	m.mapTasks = linkedmap.New()
	m.reduceTasks = linkedmap.New()
	m.server()
	return &m
}

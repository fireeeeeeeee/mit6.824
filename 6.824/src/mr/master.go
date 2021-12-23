package mr

import (
	"log"
	"net"
	"net/http"
	"net/rpc"
	"os"
	"sync"
	"time"

	"container/list"
)

//file state
const (
	UNPROCESSING int = 1
	PROCESSING   int = 2
	CONSUMED     int = 3
)

type ProcessingFile struct {
	name      string
	beginTime int64
}

type FileState struct {
	state            int
	prcessingElement *list.Element
}

type Master struct {

	// Your definitions here.
	nReduce               int
	unprocessFileNames    []string
	fileNames             []string
	processingFiles       list.List
	fileStates            map[string]*FileState
	reduceID              int
	mapID                 int
	reduceSuccessCnt      int
	intermediateFileNames [][]string
	mutex                 sync.Mutex
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
			switch m.fileStates[name].state {
			case PROCESSING:
				{
					m.fileStates[name].state = CONSUMED
					element := m.fileStates[name].prcessingElement
					m.processingFiles.Remove(element)
				}
			default:
				{
					log.Fatal("wrong file state")
				}
			}
			for _, FR := range wrequest.FR {
				m.intermediateFileNames[FR.REDUCEID] = append(m.intermediateFileNames[FR.REDUCEID], FR.FILENAME)
			}
		}
	case REDUCEOP:
		{
			m.mutex.Lock()
			m.reduceSuccessCnt++

			m.mutex.Unlock()
		}
	case NONEOP:
		{

		}
	default:
		{
			log.Fatal("unkonwn op:", wrequest.REQUESTOP)
		}
	}

	if len(m.unprocessFileNames) != 0 {
		name := m.unprocessFileNames[0]
		m.unprocessFileNames = m.unprocessFileNames[1:]
		element := m.processingFiles.PushBack(ProcessingFile{name, GetTime()})
		m.fileStates[name] = &FileState{PROCESSING, element}

		wreply.FILENAME = name
		wreply.REPLYOP = MAPOP
		wreply.NREDUCE = m.nReduce
		wreply.MAPID = m.mapID
		m.mapID++
	} else if m.processingFiles.Len() != 0 {
		// waiting to reduce
		wreply.REPLYOP = NONEOP
	} else {
		//reduce
		if m.reduceID != m.nReduce {
			wreply.REDUCEID = m.reduceID
			wreply.REPLYOP = REDUCEOP
			wreply.REDUCEFILES = m.intermediateFileNames[m.reduceID]
			m.reduceID++
		} else {
			//TODO: check long tail reduce task
			//let worker wait now
			wreply.REPLYOP = NONEOP
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
	m.processingFiles = *list.New()
	m.reduceID = 0
	m.reduceSuccessCnt = 0
	m.fileStates = make(map[string]*FileState)
	for _, name := range files {
		m.fileStates[name] = &FileState{UNPROCESSING, nil}
	}
	m.server()
	return &m
}

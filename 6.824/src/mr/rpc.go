package mr

//
// RPC definitions.
//
// remember to capitalize all names.
//

import (
	"os"
	"strconv"
)

//
// example to show how to declare the arguments
// and reply for an RPC.
//

type ExampleArgs struct {
	X int
}

type ExampleReply struct {
	Y int
}

type OP int

const (
	MAPOP    OP = 1
	REDUCEOP OP = 2
	NONEOP   OP = 3
)

type FileReduceID struct {
	FILENAME string
	REDUCEID int
}

type WorkRequest struct {
	FILENAME  string
	REQUESTOP OP
	FR        []FileReduceID
}

type WorkReply struct {
	FILENAME    string
	NREDUCE     int
	REDUCEID    int
	REDUCEFILES []string
	MAPID       int
	REPLYOP     OP
}

// Add your RPC definitions here.

// Cook up a unique-ish UNIX-domain socket name
// in /var/tmp, for the master.
// Can't use the current directory since
// Athena AFS doesn't support UNIX-domain sockets.
func masterSock() string {
	s := "/var/tmp/824-mr-"
	s += strconv.Itoa(os.Getuid())
	return s
}

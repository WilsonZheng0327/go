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

const (
	MsgTask = iota
	MsgFinishedMap
	MsgFinishedReduce
)

// MapReduce
type FetchTaskArgs struct {
	Msg       int
	MsgString string
}

type FetchTaskReply struct {
	TaskObj Task
}

// Add your RPC definitions here.

// Cook up a unique-ish UNIX-domain socket name
// in /var/tmp, for the coordinator.
// Can't use the current directory since
// Athena AFS doesn't support UNIX-domain sockets.
func coordinatorSock() string {
	s := "/var/tmp/5840-mr-"
	s += strconv.Itoa(os.Getuid())
	return s
}

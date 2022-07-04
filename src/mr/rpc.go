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
}

type ExampleReply struct {
	Id int
}

// Add your RPC definitions here.

type AskArgs struct {
}

type AskReply struct {
	File     []string
	Workid   int
	TaskType int
}
type ReportArgs struct {
	Id       int
	TaskType int
	File     string
}
type ReportReply struct {
	Success bool
}

// Cook up a unique-ish UNIX-domain socket name
// in /var/tmp, for the coordinator.
// Can't use the current directory since
// Athena AFS doesn't support UNIX-domain sockets.
func coordinatorSock() string {
	s := "/var/tmp/824-mr-"
	s += strconv.Itoa(os.Getuid())
	return s
}

package mr

import "sync"
// RPC definitions.

// KeyValue is a type used to hold the key/value pairs
type KeyValue struct {
	Key   string
	Value string
}

type Worker struct {
	WorkerID string
}

type GetJobArgs struct{
	WorkerID string
}

type GetJobReply struct {
	FileName string
	JobID    string 
	Type     string
	State    string
}

type JobDoneArgs struct {
	JobID string 
}

type JobDoneReply struct{}

type Job struct {
	JobID     string
	Type      string
	FileName  string
	StartTime int64
}

type MetaData struct {
	Nreduce int
	Nmap    int
}

type Coordinator struct {
	mu sync.Mutex

	Jobs    []*Job
	Workers []*Worker
	Record  map[string]string
	Status  map[string]string
	phase   string

	Nreduce int
	Nmap    int
	jobDone bool
}

type RegisterWorkerArgs struct {
	WorkerID string
}

type GetMetaDataArgs struct {

}

type GetMetaDataReply struct {
	Nreduce int
	Nmap int
}

type RegisterWorkerReply struct{}

type JobCompleteArgs struct {
	JobID string
}

type JobCompleteReply struct{}


// Add your RPC definitions here.


// Cook up a unique-ish UNIX-domain socket name
// in /var/tmp, for the coordinator.
// Can't use the current directory since
// Athena AFS doesn't support UNIX-domain sockets.

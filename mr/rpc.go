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

// Coordinator struct to hold the state of the coordinator
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

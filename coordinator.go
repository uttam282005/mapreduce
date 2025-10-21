// Package mr This file contains the implementation of the Coordinator for a MapReduce system.
package mr

import (
	"log"
	"net"
	"net/http"
	"net/rpc"
	"os"
	"sync"
	"time"
)

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

	NReduce int
	Nmap    int
	jobDone bool
}

// map job
// file
// id
// create jobs from input filess
func (c *Coordinator) createJobs(files []string) []*Job {
	var jobs []*Job
	for i, file := range files {
		job := Job{
			string(i),
			c.phase,
			file,
			-1,
		}

		jobs = append(jobs, &job)
	}
	return jobs
}

type RegisterWorkerArgs struct {
	WorkerID string
}

type RegisterWorkerReply struct{}

type JobCompleteArgs struct {
	JobID string
}

type JobCompleteReply struct{}

func (c *Coordinator) assign(jobID, workerID string) {
	c.mu.Lock()
	defer c.mu.Unlock()

	c.Record[jobID] = workerID
	c.Status[jobID] = "inprogress"
}

func (c *Coordinator) getIdleTask() *Job {
	for _, job := range c.Jobs {
		if c.Status[job.JobID] == "idle" {
			return job 
		}
	}

	return nil
}

func (c *Coordinator) reassignFailedTasks() {}
func (c *Coordinator) allTasksDone() bool   { return true }

func (c *Coordinator) monitor() {
	ticker := time.NewTicker(time.Second)
	defer ticker.Stop()

	for range ticker.C {
		c.mu.Lock()
		if c.phase == "map" && c.allTasksDone() {
			c.phase = "reduce"
		}
		if c.phase == "reduce" && c.allTasksDone() {
			c.jobDone = true
		}

		c.reassignFailedTasks()
		c.mu.Unlock()

		if c.jobDone {
			break
		}
	}
}

// GetJob RPC handlers for the worker to call.
func (c *Coordinator) GetJob(args *GetJobArgs, reply *GetJobReply) error {
	c.mu.Lock()
	defer c.mu.Unlock()

	if c.jobDone {
		return nil
	}

	job := c.getIdleTask()

	if job == nil {
		reply.State = "wait"
		return nil
	}

	workerID := args.WorkerID
	c.assign(job.JobID, workerID)

	reply.FileName = job.FileName
	reply.JobID = job.JobID
	reply.Type = c.phase

	return nil
}

func (c *Coordinator) ReportJobDone(args *JobCompleteArgs, reply *JobCompleteReply) {
	c.mu.Lock()
	defer c.mu.Unlock()

	delete(c.Record, args.JobID)
	c.Status[args.JobID] = "done"
}

// RegisterWorker RPC handler for worker registration.
func (c *Coordinator) RegisterWorker(args *RegisterWorkerArgs, reply *RegisterWorkerReply) error {
	c.mu.Lock()
	defer c.mu.Unlock()

	worker := Worker{
		args.WorkerID,
	}

	c.Workers = append(c.Workers, &worker)
	return nil
}

// start a thread that listens for RPCs from worker.go
func (c *Coordinator) server() {
	rpc.Register(c)
	rpc.HandleHTTP()
	// l, e := net.Listen("tcp", ":1234")
	sockname := coordinatorSock()
	os.Remove(sockname)
	l, e := net.Listen("unix", sockname)
	if e != nil {
		log.Fatal("listen error:", e)
	}
	go http.Serve(l, nil)
}

// Done is called periodically by the client to find out
// if the entire job has finished.
func (c *Coordinator) Done() bool {
	done := false

	return done
}

// MakeCoordinator creates a Coordinator.
func MakeCoordinator(files []string, nReduce int) *Coordinator {
	c := Coordinator{}
	c.Jobs = c.createJobs(files)
	go c.monitor()
	c.server()
	return &c
}

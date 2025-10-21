// Package mr This file contains the implementation of the Coordinator for a MapReduce system.
package mr

import (
	"fmt"
	"net"
	"net/rpc"
	"time"
)

// create jobs from input filess
func (c *Coordinator) createJobs(files []string) []*Job {
	var jobs []*Job
	for i, file := range files {
		job := Job{
			fmt.Sprintf("job-%d", i),
			c.phase,
			file,
			-1,
		}

		jobs = append(jobs, &job)
	}
	return jobs
}

// assign a job to worker
func (c *Coordinator) assign(job *Job, workerID string) {
	c.mu.Lock()
	defer c.mu.Unlock()

	c.Record[job.JobID] = workerID
	c.Status[job.JobID] = "inprogress"
	job.StartTime = time.Now().Unix()
}

// get an idle task
func (c *Coordinator) getIdleTask() *Job {
	for _, job := range c.Jobs {
		if c.Status[job.JobID] == "idle" {
			return job 
		}
	}

	return nil
}

// reassign tasks that have failed, here failed means not completed in 10 seconds
func (c *Coordinator) reassignFailedTasks() {
	for _, job := range c.Jobs {
		if c.Status[job.JobID] == "inprogress" {
			if time.Now().Unix() - job.StartTime > 10 {
				c.Status[job.JobID] = "idle"
			}
		}
	}
}

// check tasks completion
func (c *Coordinator) allTasksDone() bool {
	for _, status := range c.Status {
		if status != "done" {
			return false
		}
	}

	return true 
}

// monitor the progress of the jobs
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

// GetJob RPC handler for the worker get job.
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
	c.assign(job, workerID)

	reply.FileName = job.FileName
	reply.JobID = job.JobID
	reply.Type = c.phase

	return nil
}

// GetMetaData RPC handler for worker to get metadata.
func (c *Coordinator) GetMetaData(args *GetMetaDataArgs, reply *GetMetaDataReply) error {
	c.mu.Lock()
	defer c.mu.Unlock()

	reply.Nmap = c.Nmap
	reply.Nreduce = c.Nreduce

	return nil
}

// ReportJobDone RPC handler for worker to report job completion.
func (c *Coordinator) ReportJobDone(args *JobDoneArgs, reply *JobDoneReply) {
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
  rpc.Register(c) // Register the Coordinator service

	listener, err := net.Listen("tcp", ":1234") // Listen on port 1234
	if err != nil {
		fmt.Println("Error listening:", err)
		return
	}
	defer listener.Close()

	fmt.Println("RPC server listening on port 1234")

	for {
		conn, err := listener.Accept() // Accept incoming connections
		if err != nil {
			fmt.Println("Error accepting connection:", err)
			continue
		}
		go rpc.ServeConn(conn) // Serve each connection in a goroutine
	}
}

// Done is called periodically by the client to find out
// if the entire job has finished.
func (c *Coordinator) Done() bool {
	c.mu.Lock()
	defer c.mu.Unlock()

	return c.jobDone
}

// MakeCoordinator creates a Coordinator.
func MakeCoordinator(files []string, nReduce int) *Coordinator {
	c := Coordinator{
		Nmap: 	len(files),
	  Status: make(map[string]string),
		Record: make(map[string]string),
		phase:  "map",
		Nreduce: nReduce,

		jobDone: false,
	}

	c.Jobs = c.createJobs(files)
	go c.monitor()
	c.server()
	return &c
}

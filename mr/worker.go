package mr

import (
	"encoding/json"
	"fmt"
	"hash/fnv"
	"io"
	"log"
	"math/rand/v2"
	"net/rpc"
	"os"
	"sort"
	"time"
)

// use ihash(key) % Nreduce to choose the reduce
func ihash(key string) int {
	h := fnv.New32a()
	h.Write([]byte(key))
	return int(h.Sum32() & 0x7fffffff)
}

func MakeWorker() (*Worker, error) {
	worker := Worker{}
	id := fmt.Sprintf("%d-%d-%d", time.Now().UnixNano(), os.Getpid(), rand.IntN(1e6))
	worker.WorkerID = id
	err := worker.register()
	if err != nil {
		return nil, fmt.Errorf("failed to create worker: %v", worker.WorkerID)
	}
	return &worker, nil
}

// Register the worker with the coordinator
func (w *Worker) register() error {
	args := RegisterWorkerArgs{w.WorkerID}
	reply := RegisterWorkerReply{}
	ok := call("Coordinator.RegisterWorker", &args, &reply)
	if !ok {
		log.Printf("failed to register worker %v", w.WorkerID)
		return fmt.Errorf("register failed")
	}
	log.Printf("worker registered: %s", w.WorkerID)
	return nil
}

// Hanldle the map job
func handleMapJob(
	fileName string,
	mapf func(string, string) []KeyValue,
	mapTaskID string,
	nReduce int,
) bool {
	inputFile, err := os.Open(fileName)
	if err != nil {
		log.Fatalf("cannot read %v", fileName)
	}
	defer inputFile.Close()

	content, err := io.ReadAll(inputFile)
	if err != nil {
		log.Fatalf("cannot read %v", fileName)
	}

	mapgoResult := mapf(fileName, string(content))

	// create encoders and files for each reduce partition
	enc := make([]*json.Encoder, nReduce)
	files := make([]*os.File, nReduce)
	for r := 0; r < nReduce; r++ {
		intermediateFile := fmt.Sprintf("mr-%v-%d", mapTaskID, r)
		f, err := os.Create(intermediateFile)
		if err != nil {
			log.Printf("cannot create intermediate file %v", intermediateFile)
			continue
		}
		files[r] = f
		enc[r] = json.NewEncoder(f)
	}

	for _, kv := range mapgoResult {
		reduceTaskID := ihash(kv.Key) % nReduce
		if enc[reduceTaskID] == nil {
			continue
		}
		err := enc[reduceTaskID].Encode(&kv)
		if err != nil {
			log.Printf("cannot encode map result")
			continue
		}
	}

	// close all files explicitly
	for _, f := range files {
		if f != nil {
			f.Close()
		}
	}

	log.Printf("map task %s: wrote %d partition files", mapTaskID, nReduce)
	return true
}

// Notify the coordinator that the job is done
func notifyCoordinatorDone(JobID string) {
	for {
		args := JobDoneArgs{
			JobID,
		}
		reply := JobDoneReply{}
		ok := call("Coordinator.ReportJobDone", &args, &reply)
		if !ok {
			time.Sleep(2 * time.Second)
			continue
		}
		break
	}
}

// Handle the reduce job
func handleReduceJob(
	reduceTaskID string,
	reducef func(string, []string) string,
	nMap int,
) bool {
	kva := []KeyValue{}

	// Read all intermediate files that match the reduce id
	for m := 0; m < nMap; m++ {
		fileName := fmt.Sprintf("mr-%d-%v", m, reduceTaskID)
		file, err := os.Open(fileName)
		if err != nil {
			continue
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

	// Sort by key
	sort.Slice(kva, func(i, j int) bool {
		return kva[i].Key < kva[j].Key
	})

	// Ensure output directory exists
	if err := os.MkdirAll("output", 0o755); err != nil {
		log.Fatalf("cannot create output directory: %v", err)
	}

	outputFileName := fmt.Sprintf("output/mr-out-%v", reduceTaskID)
	tempFileName := fmt.Sprintf("%s.tmp", outputFileName)
	outputFile, err := os.Create(tempFileName)
	if err != nil {
		log.Fatalf("cannot create output file %v", outputFileName)
	}
	defer outputFile.Close()

	// Aggregate values for each key and call reduce function
	i := 0
	for i < len(kva) {
		j := i + 1
		for j < len(kva) && kva[j].Key == kva[i].Key {
			j++
		}

		values := make([]string, 0, j-i)
		for k := i; k < j; k++ {
			values = append(values, kva[k].Value)
		}

		output := reducef(kva[i].Key, values)
		fmt.Fprintf(outputFile, "%v %v\n", kva[i].Key, output)

		i = j
	}

	// Atomically replace final output
	if err := os.Rename(tempFileName, outputFileName); err != nil {
		log.Fatalf("cannot rename temp output file: %v", err)
	}

	return true
}

func (w *Worker) getMetaData() (*MetaData, error) {
	var metaData GetMetaDataReply
	for i := 0; i < 10; i++ {
		ok := call("Coordinator.GetMetaData", &GetMetaDataArgs{}, &metaData)
		if ok {
			log.Printf("worker: got metadata Nmap=%d Nreduce=%d", metaData.Nmap, metaData.Nreduce)
			return &MetaData{metaData.Nmap, metaData.Nreduce}, nil
		}
		time.Sleep(500 * time.Millisecond)
	}
	return nil, fmt.Errorf("failed to get metadata after retries")
}

// StartWorker main loop
func (w *Worker) StartWorker(
	mapf func(string, string) []KeyValue,
	reducef func(string, []string) string,
) {
	metaData, err := w.getMetaData()
	if err != nil {
		log.Fatal("Worker: failed to get metadata from coordinator")
		return
	}

	Nreduce := metaData.Nreduce
	Nmap := metaData.Nmap

	for {
		args := GetJobArgs{
			WorkerID: w.WorkerID,
		}
		reply := GetJobReply{}

		if !call("Coordinator.GetJob", &args, &reply) {
			log.Println("Worker: coordinator unavailable, retrying...")
			time.Sleep(2 * time.Second)
			continue
		}

		log.Printf("worker %s: got job reply: %+v", w.WorkerID, reply)

		// Check if there’s actually a job
		if reply.State == "exit" {
			log.Println("Worker: no more jobs, exiting")
			break
		}

		if reply.State == "wait" {
			time.Sleep(500 * time.Millisecond)
			continue
		}

		switch reply.Type {
		case "map":
			handleMapJob(reply.FileName, mapf, reply.JobID, Nreduce)
		case "reduce":
			handleReduceJob(reply.JobID, reducef, Nmap)
		}

		notifyCoordinatorDone(reply.JobID)

		time.Sleep(500 * time.Millisecond) // avoid hot-looping
	}
}

// Send an RPC request to the coordinator, wait for the response.
func call(rpcname string, args any, reply any) bool {
	c, err := rpc.Dial("tcp", "localhost:1234")
	if err != nil {
		log.Fatal("failed to connect  to the coordinator.")
	}

	defer c.Close()

	err = c.Call(rpcname, args, reply)

	return err == nil
}

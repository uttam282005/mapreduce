package mr

import (
	"math/rand/v2"
	"encoding/json"
	"fmt"
	"hash/fnv"
	"io"
	"log"
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

func MakeWorker() (*Worker ,error) {
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
		log.Fatalf("failed to register worker %v", w.WorkerID)
		return fmt.Errorf("")
	}
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

	enc := make([]*json.Encoder, nReduce)

	for r := range nReduce {
		intermediateFile := fmt.Sprintf("mr-%v-%d", mapTaskID, r)
		f, err := os.Create(intermediateFile)
		if err != nil {
			log.Fatalf("cannot create intermediate file %v", intermediateFile)
		}
		defer f.Close()
		enc[r] = json.NewEncoder(f)
	}

	for _, kv := range mapgoResult {
		reduceTaskID := ihash(kv.Key) % nReduce
		err := enc[reduceTaskID].Encode(&kv)
		if err != nil {
			log.Fatal("cannot encode map result")
		}
	}

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

	// Read all intermediate files
	for m := range nMap {
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

	// Group by keys
	sort.Slice(kva, func(i, j int) bool {
		return kva[i].Key < kva[j].Key
	})

	outputFileName := fmt.Sprintf("mr-out-%v", reduceTaskID)
	tempFileName := fmt.Sprintf("%s.tmp", outputFileName)
	outputFile, err := os.Create(tempFileName)
	if err != nil {
		log.Fatalf("cannot create output file %v", outputFileName)
	}
	defer outputFile.Close()

	// Aggregreate values for each key and call the reduce func
	i := 0
	for i < len(kva) {
		j := i + 1
		for j < len(kva) && kva[j].Key == kva[i].Key {
			j++
		}
		values := []string{}
		for k := i; k < j; k++ {
			values = append(values, kva[k].Value)
		}

		output := reducef(kva[i].Key, values)
		fmt.Fprintf(outputFile, "%v %v\n", kva[i].Key, output)

		i = j
	}

	// To make sure that there are no half written output files
	os.Rename(tempFileName, outputFileName)
	return true
}

func (w *Worker) getMetaData() (MetaData, error) {
	metaData := GetMetaDataReply{}
	ok := call("Coordinator.GetMetaData", &GetMetaDataArgs{}, &metaData)
	if !ok {
		log.Fatal("Worker: failed to get metadata from coordinator")
		return MetaData(metaData), fmt.Errorf("failed to get metadata")
	}
	return MetaData(metaData), nil
}

// StartWorker main loop
func(w *Worker) StartWorker(
	mapf func(string, string) []KeyValue,
	reducef func(string, []string) string,
) {
	MakeWorker()

	metaData, err := w.getMetaData()
	if err != nil {
		log.Fatal("Worker: failed to get metadata from coordinator")
		return
	}

	Nreduce := metaData.Nreduce
	Nmap := metaData.Nmap

	for {
		args := GetJobArgs{}
		reply := GetJobReply{}

		if !call("Coordinator.GetJob", &args, &reply) {
			log.Println("Worker: coordinator unavailable, retrying...")
			time.Sleep(2 * time.Second)
			continue
		}

		// Check if there’s actually a job
		if reply.FileName == "" {
			log.Println("Worker: no more jobs, exiting")
			break
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

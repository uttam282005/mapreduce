package main

import (
	"flag"
	"fmt"
	"log"
	"os"
	"time"

	"mapreduce/mr"
)

// Example map function for word count.
func mapF(filename string, contents string) []mr.KeyValue {
	var kva []mr.KeyValue
	word := ""
	for _, r := range contents {
		if r == ' ' || r == '\n' || r == '\t' || r == '\r' {
			if word != "" {
				kva = append(kva, mr.KeyValue{Key: word, Value: "1"})
				word = ""
			}
			continue
		}
		word += string(r)
	}
	if word != "" {
		kva = append(kva, mr.KeyValue{Key: word, Value: "1"})
	}
	return kva
}

// Example reduce function for word count.
func reduceF(key string, values []string) string {
	return fmt.Sprintf("%d", len(values))
}

func main() {
	if len(os.Args) < 2 {
		fmt.Println("usage: go run main.go coordinator|worker [options]")
		os.Exit(1)
	}

	mode := os.Args[1]

	switch mode {
	case "coordinator":
		coordFlags := flag.NewFlagSet("coordinator", flag.ExitOnError)
		nReduce := coordFlags.Int("nreduce", 3, "number of reduce tasks")
		coordFlags.Parse(os.Args[2:])

		files := coordFlags.Args()
		if len(files) == 0 {
			log.Fatal("coordinator: need input files")
		}

		c := mr.MakeCoordinator(files, *nReduce)

		// Run coordinator in a separate goroutine
		doneChan := make(chan bool)
		go func() {
			for {
				time.Sleep(500 * time.Millisecond)
				if c.Done() {
					log.Println("Coordinator: all tasks finished")
					doneChan <- true
					return
				}
			}
		}()

		// Main thread waits here
		<-doneChan
		// Wait for all the worker processes to die
		time.Sleep( 2* time.Second)
		
		log.Println("Coordinator exited cleanly")

	case "worker":
		go func() {
			w, err := mr.MakeWorker()
			if err != nil {
				log.Fatalf("failed to create worker: %v", err)
			}
			w.StartWorker(mapF, reduceF)
		}()

		// Keep worker process alive
		for {
			time.Sleep(1 * time.Second)
		}

	default:
		fmt.Println("unknown mode:", mode)
		os.Exit(1)
	}
}


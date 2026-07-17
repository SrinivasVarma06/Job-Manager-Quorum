package main

import (
	"log"
	"quorum/internal/engine"
	"time"
)

func main() {

	e, err := engine.New()
	if err != nil {
		log.Fatal(err)
	}

	if err := e.Restore(); err != nil {
		log.Fatal(err)
	}

	e.Start()
	if err := e.SubmitJob("email", 10); err != nil {
		log.Fatal(err)
	}
	if err := e.SubmitJob("play games", 5); err != nil {
		log.Fatal(err)
	}
	if err := e.SubmitJob("study", 1); err != nil {
		log.Fatal(err)
	}
	if err := e.SubmitJob("sleep", 3); err != nil {
		log.Fatal(err)
	}

	time.Sleep(10 * time.Second)
	if err := e.Stop(); err != nil {
		log.Fatal(err)
	}
	time.Sleep(time.Second)
}

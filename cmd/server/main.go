package main 

import (
	"fmt"
	"quorum/internal/job"
	"quorum/internal/queue"
)
func main(){
	q:=queue.NewQueue();
	q.Enqueue(job.Job{
		ID:1,
		Type: "email",
		Priority: 10,
	})
	q.Enqueue(job.Job{
		ID:2,
		Type: "resize-image",
		Priority: 5,
	})
	j,ok:=q.Dequeue();
	if ok{
		fmt.Println(j);
	} else {
		fmt.Println("Empty");
	} 
}
package queue

import "quorum/internal/job"
type Queue struct{
	jobs []job.Job
}

func NewQueue() *Queue {
	return &Queue{
		jobs:make([]job.Job,0),
	}
}
func (q *Queue) IsEmpty()(bool){
	return len(q.jobs)==0
}
func (q *Queue) Enqueue(j job.Job){
	q.jobs=append(q.jobs,j)
}

func (q* Queue) Dequeue()(job.Job,bool){
	if(q.IsEmpty()){
		return job.Job{},false;
	}
	removed:=q.jobs[0]
	q.jobs=q.jobs[1:]
	return removed,true
}

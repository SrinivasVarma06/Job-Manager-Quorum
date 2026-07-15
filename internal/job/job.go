package job

type Job struct{
	ID int
	Type string
	Priority int
	Status Status
}

type Status string

const (
	Pending Status="PENDING"
	Running Status="RUNNING"
	Completed Status="COMPLETED"
	Failed Status="FAILED"
)
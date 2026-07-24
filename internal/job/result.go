package job

type Result struct {
	JobID   int
	Success bool
	Error   error
}

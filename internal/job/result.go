package job

// Result is the outcome of a single job execution attempt.
// Attempt must match Job.Attempt at the time the result arrives; mismatched
// attempts are discarded by resultLoop to prevent duplicate completions after
// a worker is declared dead but later comes back and reports an old result.
type Result struct {
	JobID   int
	Success bool
	Error   error
	Attempt int
}

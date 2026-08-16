package executor

type Limiter interface {
	Acquire()
}

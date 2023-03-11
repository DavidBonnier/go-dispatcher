package dispatcher

import "time"

// Job defines a task, which is given to a dispatcher to be executed
// by a worker with a separate goroutine
type Job interface {
	Do()
	Priority() int
}

type _DelayedJob struct {
	job         Job
	delayPeriod time.Duration
}

type _FinishSignal struct{}

type _EmptyJob struct {
	finishSignReceiver chan _FinishSignal
	priority int
}

func (quitJob *_EmptyJob) Do() {
	// Tell the dispatcher that all jobs have been dispatched
	quitJob.finishSignReceiver <- _FinishSignal{}
}

func (quitJob *_EmptyJob) Priority() int {
	return quitJob.priority
}

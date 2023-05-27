// Package dispatcher provides capabilities of limiting the total number of goroutines
// and easily managing the dependency of concurrent executions.
package dispatcher

import (
	"sort"
	"sync"
	"time"

	"github.com/DavidBonnier/queue"
)

// Dispatcher internally maintains a worker pool and runs a job dispatching loop
// assigning jobs to workers available.
type Dispatcher interface {
	// Dispatch gives a job to a worker at a time, and blocks until at least one worker
	// becomes available. Each job dispatched is handled by a separate goroutine.
	Dispatch(job Job) error
	// DispatchWithDelay behaves similarly to Dispatch, except it is delayed for a given
	// period of time (in nanoseconds) before the job is allocated to a worker.
	DispatchWithDelay(job Job, delayPeriod time.Duration) error
	// Finalize blocks until all jobs dispatched are finished and all workers are returned
	// to worker pool. Note it must be called to terminate the dispatching loop when the
	// dispatcher is no longer needed, and a finalized dispatcher must not be reused.
	Finalize() error
	// GetNumWorkersAvail returns the number of workers availalbe for tasks at the time
	// it is called.
	GetNumWorkersAvail(priority int) int
	// GetTotalNumWorkers returns total number of workers created by the dispatcher.
	GetTotalNumWorkers(priority int) int
	// GetNumJobAvail returns the current number of workers
	GetNumJobAvail(priority int) int
}

type DispatcherConfig struct {
	Priority int
	NbWorker int
}

type _Dispatcher struct {
	sync.RWMutex
	allPriority []int
	wg          *sync.WaitGroup
	workerPool  map[int]chan *_Worker
	jobListener map[int]*queue.Queue
	notify      chan struct{}
}

func (dispatcher *_Dispatcher) Dispatch(job Job) error {
	dispatcher.Lock()
	defer dispatcher.Unlock()

	queue, exists := dispatcher.jobListener[job.Priority()]
	if !exists {
		return newError("Invalid job priority")
	}
	queue.Push(_DelayedJob{job: job})

	dispatcher.notify <- struct{}{}
	return nil
}

func (dispatcher *_Dispatcher) DispatchWithDelay(job Job, delayPeriod time.Duration) error {
	if delayPeriod <= 0 {
		return newError("Invalid delay period")
	}

	dispatcher.Lock()
	defer dispatcher.Unlock()

	queue, exists := dispatcher.jobListener[job.Priority()]
	if !exists {
		return newError("Invalid job priority")
	}

	queue.Push(_DelayedJob{job: job, delayPeriod: delayPeriod})
	dispatcher.notify <- struct{}{}
	return nil
}

func (dispatcher *_Dispatcher) Finalize() error {
	// Wait for all jobs to be dispatched (to ensure wg.Add() happens before wg.Wait())
	finishSignReceiver := make(chan _FinishSignal)
	job := &_EmptyJob{
		finishSignReceiver: finishSignReceiver,
		priority:           dispatcher.allPriority[len(dispatcher.allPriority)-1],
	}

	errorDisp := dispatcher.Dispatch(job)
	if errorDisp != nil {
		return errorDisp
	}

	<-finishSignReceiver
	// Wait for all workers to finish their jobs
	dispatcher.wg.Wait()
	// Stop job dispatching loop
	close(dispatcher.notify)

	return nil
}

func (dispatcher *_Dispatcher) GetNumJobAvail(priority int) int {
	dispatcher.RLock()
	defer dispatcher.RUnlock()

	queue, exists := dispatcher.jobListener[priority]
	if !exists {
		return 0
	}
	return queue.Length()
}

func (dispatcher *_Dispatcher) GetNumWorkersAvail(priority int) int {
	return len(dispatcher.workerPool[priority])
}

func (dispatcher *_Dispatcher) GetTotalNumWorkers(priority int) int {
	return cap(dispatcher.workerPool[priority])
}

// NewDispatcher returns a new job dispatcher with a worker pool
// initialized with given number of workers.
func NewDispatcher(config []*DispatcherConfig) (Dispatcher, error) {
	wg := &sync.WaitGroup{}
	dispatcher := &_Dispatcher{wg: wg}
	dispatcher.notify = make(chan struct{}, 1<<30)
	dispatcher.jobListener = make(map[int]*queue.Queue)
	dispatcher.workerPool = make(map[int]chan *_Worker)

	for _, value := range config {
		dispatcher.allPriority = append(dispatcher.allPriority, value.Priority)
		dispatcher.jobListener[value.Priority] = queue.New()
		if value.NbWorker <= 0 {
			return nil, newError("Invalid number of workers given to create a new dispatcher")
		}
		dispatcher.workerPool[value.Priority] = make(chan *_Worker, value.NbWorker)
	}
	sort.Sort(sort.Reverse(sort.IntSlice(dispatcher.allPriority)))

	for key, value := range dispatcher.workerPool {
		// Register the worker in the dispatcher
		for i := 0; i < cap(value); i++ {
			dispatcher.workerPool[key] <- &_Worker{}
		}
	}

	go dispatcher.dispatcheJob()

	return dispatcher, nil
}

func (dispatcher *_Dispatcher) dispatcheJob() {
	for _ = range dispatcher.notify {
		var interfacePop interface{}
		dispatcher.Lock()
		for _, key := range dispatcher.allPriority {
			interfacePop = dispatcher.jobListener[key].Pop()
			if interfacePop != nil {
				break
			}
		}
		dispatcher.Unlock()

		if interfacePop == nil {
			// Here if we have error with Queue
			panic("Queue is empty")
		}

		var delayedJob _DelayedJob = interfacePop.(_DelayedJob)
		time.Sleep(delayedJob.delayPeriod)
		worker := <-dispatcher.workerPool[delayedJob.job.Priority()]
		dispatcher.wg.Add(1)
		go dispatcher.addJob(delayedJob.job, worker)
	}
}

func (dispatcher *_Dispatcher) addJob(job Job, worker *_Worker) {
	job.Do()
	// Return it back to the worker pool
	dispatcher.workerPool[job.Priority()] <- worker
	dispatcher.wg.Done()
}

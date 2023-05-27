package dispatcher

import (
	"runtime"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
)

const (
	Min = -300
	Low = -200
)

type testJob struct {
	resultSender chan bool
	priority     int
}

func (job *testJob) Do() {
	job.resultSender <- true
}

func (job *testJob) Priority() int {
	return job.priority
}

type testBigJob struct {
	mutex       *sync.Mutex
	accumulator *int
	priority    int
}

func (job *testBigJob) Do() {
	job.mutex.Lock()
	defer job.mutex.Unlock()

	time.Sleep(50000)
	*job.accumulator++
}

func (job *testBigJob) Priority() int {
	return job.priority
}

func TestGoroutineLeaks(T *testing.T) {
	assertion := assert.New(T)

	config := []*DispatcherConfig{
		{Priority: Min, NbWorker: 100},
		{Priority: Low, NbWorker: 100},
	}

	for i := 0; i < 100; i++ {
		disp, _ := NewDispatcher(config)
		disp.Finalize()
	}

	runtime.GC()
	assertion.Equal(2, runtime.NumGoroutine())
}

func TestInitWorkerPool(T *testing.T) {
	assertion := assert.New(T)

	config := []*DispatcherConfig{
		{Priority: Min, NbWorker: 1000},
		{Priority: Low, NbWorker: 100},
	}
	disp, _ := NewDispatcher(config)

	// Expect correct number of workers should be returned
	numWorkersAvail := disp.GetNumWorkersAvail(Min)
	assertion.Equal(numWorkersAvail, 1000)

	totalNumWorkers := disp.GetTotalNumWorkers(Min)
	assertion.Equal(totalNumWorkers, 1000)

	numJobAvail := disp.GetNumJobAvail(Min)
	assertion.Equal(numJobAvail, 0)

	// Expect correct number of workers should be returned
	numWorkersAvail = disp.GetNumWorkersAvail(Low)
	assertion.Equal(numWorkersAvail, 100)

	totalNumWorkers = disp.GetTotalNumWorkers(Low)
	assertion.Equal(totalNumWorkers, 100)

	numJobAvail = disp.GetNumJobAvail(Low)
	assertion.Equal(numJobAvail, 0)

	// Expect an error to be returned by getting number of available workers
	// given invalid param
	config = []*DispatcherConfig{
		{Priority: Min, NbWorker: 0},
	}
	disp, err := NewDispatcher(config)
	assertion.Nil(disp)
	assertion.EqualError(
		err,
		"Invalid number of workers given to create a new dispatcher",
		"Unexpected error returned by creating new dispatcher given invalid number of workers",
	)

	config = []*DispatcherConfig{
		{Priority: Min, NbWorker: -1},
	}
	disp, err = NewDispatcher(config)
	assertion.Nil(disp)
	assertion.EqualError(
		err,
		"Invalid number of workers given to create a new dispatcher",
		"Unexpected error returned by creating new dispatcher given invalid number of workers",
	)
}

func TestFinializingJobs(T *testing.T) {
	assertion := assert.New(T)
	numWorkers := 100

	config := []*DispatcherConfig{
		{Priority: Min, NbWorker: numWorkers},
		{Priority: Low, NbWorker: numWorkers},
	}

	disp, _ := NewDispatcher(config)
	var mutex sync.Mutex
	accumulator := 0
	for i := 0; i < numWorkers; i++ {
		// Each big job takes 50000 ns to finish
		disp.Dispatch(&testBigJob{accumulator: &accumulator, mutex: &mutex, priority: Low})
	}

	disp.Finalize()
	assertion.Equal(accumulator, numWorkers, "Dispatcher did not wait for all jobs to complete")
}

func TestFinializingDelayedJobs(T *testing.T) {
	assertion := assert.New(T)
	numWorkers := 100

	config := []*DispatcherConfig{
		{Priority: Min, NbWorker: numWorkers},
		{Priority: Low, NbWorker: numWorkers},
	}

	disp, _ := NewDispatcher(config)

	var mutex sync.Mutex
	accumulator := 0
	for i := 0; i < numWorkers; i++ {
		// Each big job takes 50000 ns to finish
		disp.DispatchWithDelay(&testBigJob{accumulator: &accumulator, mutex: &mutex, priority: Low}, 50000)
	}

	disp.Finalize()
	assertion.Equal(accumulator, numWorkers, "Dispatcher did not wait for all jobs to complete")
}

func TestFinializingManyDelayedJobs(T *testing.T) {
	assertion := assert.New(T)
	numWorkers := 100
	numJobs := numWorkers * 10

	config := []*DispatcherConfig{
		{Priority: Min, NbWorker: numWorkers},
		{Priority: Low, NbWorker: numWorkers},
	}

	disp, _ := NewDispatcher(config)
	var mutex sync.Mutex
	accumulator := 0
	for i := 0; i < numJobs; i++ {
		// Each big job takes 50000 ns to finish
		disp.DispatchWithDelay(&testBigJob{accumulator: &accumulator, mutex: &mutex, priority: Low}, 50000)
	}

	disp.Finalize()
	assertion.Equal(accumulator, numJobs, "Dispatcher did not wait for all jobs to complete")
}

func TestDispatchingJobs(T *testing.T) {
	assertion := assert.New(T)
	numWorkers := 100

	config := []*DispatcherConfig{
		{Priority: Min, NbWorker: numWorkers},
		{Priority: Low, NbWorker: numWorkers},
	}

	receiver := make(chan bool, numWorkers)
	disp, _ := NewDispatcher(config)

	errorDisp := disp.Dispatch(&testJob{resultSender: receiver, priority: 50})
	assertion.Equal(errorDisp.Error(), "Invalid job priority", "Incorrect job priority for dispach job")

	// Dispatch jobs
	sum := 0
	for i := 0; i < numWorkers; i++ {
		disp.Dispatch(&testJob{resultSender: receiver, priority: Low})
	}
	disp.Finalize()
	close(receiver)
	// Verify the number of jobs being done
	for range receiver {
		sum++
	}
	assertion.Equal(numWorkers, sum, "Incorrect number of job being dispatched")
}

func TestDispatchingJobsWithDelay(T *testing.T) {
	assertion := assert.New(T)
	numWorkers := 100
	receiver := make(chan bool, numWorkers)

	config := []*DispatcherConfig{
		{Priority: Min, NbWorker: numWorkers},
		{Priority: Low, NbWorker: numWorkers},
	}

	disp, _ := NewDispatcher(config)

	start := time.Now()
	// Dispatch jobs with delay
	for i := 0; i < numWorkers; i++ {
		disp.DispatchWithDelay(&testJob{resultSender: receiver, priority: Low}, 10000)
	}
	disp.Finalize()
	elapse := time.Since(start)
	assertion.True(elapse >= 1000000, "Job dispatching was not delayed with the correct time period")
}

func TestDispatchingJobsWithDelayError(T *testing.T) {
	assertion := assert.New(T)
	numWorkers := 100
	receiver := make(chan bool, numWorkers)

	config := []*DispatcherConfig{
		{Priority: Min, NbWorker: numWorkers},
		{Priority: Low, NbWorker: numWorkers},
	}

	disp, _ := NewDispatcher(config)

	err := disp.DispatchWithDelay(&testJob{resultSender: receiver, priority: Low}, 0)
	assertion.EqualError(
		err,
		"Invalid delay period",
		"Unexpected error returned by dispatch given invalid delay period",
	)
}

func TestDispatchingManyJobs(T *testing.T) {
	assertion := assert.New(T)
	numWorkers := 100

	config := []*DispatcherConfig{
		{Priority: Min, NbWorker: numWorkers},
		{Priority: Low, NbWorker: numWorkers},
	}

	numJobs := numWorkers * 100
	receiver := make(chan bool, numWorkers)
	disp, _ := NewDispatcher(config)

	go func(numJobs int) {
		for i := 0; i < numJobs; i++ {
			disp.Dispatch(&testJob{resultSender: receiver, priority: Low})
		}
		disp.Finalize()
		close(receiver)
	}(numJobs)

	sum := 0
	// Verify the number of jobs being done
	for range receiver {
		sum++
	}
	assertion.Equal(numJobs, sum, "Incorrect number of job executed")
}

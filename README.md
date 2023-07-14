# go-dispatcher

![Build](https://github.com/DavidBonnier/go-dispatcher/workflows/Build/badge.svg?branch=master)
[![Coverage Status](https://coveralls.io/repos/github/DavidBonnier/go-dispatcher/badge.svg?branch=master)](https://coveralls.io/github/DavidBonnier/go-dispatcher?branch=master)
[![GoDoc](https://godoc.org/github.com/DavidBonnier/go-dispatcher/dispatcher?status.svg)](https://godoc.org/github.com/DavidBonnier/go-dispatcher/dispatcher)

A worker-pool job dispatcher inspired by the [Go-Dispacher](https://github.com/YSZhuoyang/go-dispatcher) and by [Post: Handling 1 Million Requests per Minute with Go](http://marcio.io/2015/07/handling-1-million-requests-per-minute-with-golang/).

* [Barrier Synchronization](https://docs.microsoft.com/en-us/windows/win32/sync/synchronization-barriers): Easily run batches of jobs in sequence, for algorithms that involve running a set of independent tasks concurrently for a while, then all wait on a barrier, and repeat again.
* [Bulkhead](https://docs.microsoft.com/en-us/azure/architecture/patterns/bulkhead): keep workers in different pools isolated thus one failing does not affect others.
* Limit total number of goroutines to prevent it from draining out resources.
* Prioritizing has job compared to others.

## How it works

                    ----------------
       job queue -> |  dispatcher  |
                    |              |
                    ----------------
                          | /|\
                          |  |
                         \|/ |
                      -----------
                      |  worker |
                      |   pool  |
                      -----------

A dispatcher internally maintains a worker pool and runs a job dispatching loop assigning jobs to workers available. It then waits for all Jobs dispatched to finish and cleanup everything.

Note: a dispatcher is not meant to be reused, `Finalize()` must be called at the end to terminate its job dispatching loop. This is to avoid goroutine leaks.

## How to use

1. Download and import package.

        go get -u github.com/DavidBonnier/go-dispatcher/dispatcher

2. Create a job dispatcher with a worker pool initialized with given size, and start listening to new jobs.

        config := []*DispatcherConfig{
                {Priority: 0, NbWorker: 1000},
                {Priority: 1, NbWorker: 100},
        }
        disp, _ := dispatcher.NewDispatcher(config)

3. Dispatch jobs (dispatch() will block until at least one worker becomes available and takes the job).

        type myJob struct {
            // ...
        }

        func (job *myJob) Do() {
            // Do something ...
        }

        func (job *myJob) Priority() int {
            return 1
        }

        disp.Dispatch(&myJob{...})

4. Wait until all jobs are done and terminate the task loop.

        disp.Finalize()

package worker

import (
	"runtime"

	"github.com/Jeffail/tunny"
)

// Pool for tunny.Pool
type Pool = tunny.Pool

// Worker ...
type Worker struct {
	pools []*tunny.Pool
}

// NewWorker ...
func NewWorker() *Worker {
	return &Worker{}
}

// RegisterProcessorFunc ...
func (w *Worker) RegisterProcessorFunc(f func(interface{}) interface{}) *Pool {
	numCPUs := runtime.NumCPU()
	pool := tunny.NewFunc(numCPUs, f)
	w.pools = append(w.pools, pool)
	return pool
}

// Stop ...
func (w *Worker) Stop() {
	for _, p := range w.pools {
		p.Close()
	}
}

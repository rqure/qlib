package transformer

import (
	"container/heap"
	"time"

	"github.com/d5/tengo/v2"
)

type Job struct {
	task     *tengo.UserFunction
	deadline time.Time
	index    int
}

type JobQueue []*Job

// Len returns the length of the queue.
func (pq JobQueue) Len() int { return len(pq) }

// Less compares two items based on their timestamps (earlier timestamp = higher priority).
func (pq JobQueue) Less(i, j int) bool {
	return pq[i].deadline.Before(pq[j].deadline)
}

// Swap swaps the elements at the specified indices.
func (pq JobQueue) Swap(i, j int) {
	pq[i], pq[j] = pq[j], pq[i]
	pq[i].index = i
	pq[j].index = j
}

// Push adds an element to the queue.
func (pq *JobQueue) Push(x any) {
	n := len(*pq)
	item := x.(*Job)
	item.index = n
	*pq = append(*pq, item)
}

// Pop removes the highest-priority element from the queue.
func (pq *JobQueue) Pop() any {
	old := *pq
	n := len(old)
	job := old[n-1]
	old[n-1] = nil // Avoid memory leak.
	job.index = -1
	*pq = old[0 : n-1]
	return job
}

// update modifies the timestamp and value of an Item in the queue.
func (pq *JobQueue) Update(job *Job, task *tengo.UserFunction, deadline time.Time) {
	job.task = task
	job.deadline = deadline
	heap.Fix(pq, job.index)
}

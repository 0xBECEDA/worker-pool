package main

import (
	"context"
	"log"
	"sync"
	"sync/atomic"
	"time"

	"github.com/antigloss/go/container/concurrent/queue"
)

type Job interface {
	ID() string
	Do() error
}

type JobResult struct {
	JobID string
	Err   error
}

type WorkerPool interface {
	Start(ctx context.Context)
	AddWorkers(count int)
	RemoveWorkers(count int)
	AddJob(job Job)
	Subscribe() chan JobResult
}

type Pool struct {
	tasks        *queue.LockfreeQueue[Job]
	workersAlive *atomic.Int64
	workers      *sync.Map
}

type Worker struct {
	id        int64
	tasks     *queue.LockfreeQueue[Job]
	quit      chan bool
	jobResult chan JobResult
}

type Task struct {
	id  string
	job func() error
}

func NewTask(id string, job func() error) *Task {
	return &Task{
		id:  id,
		job: job,
	}
}

func (t *Task) ID() string {
	return t.id
}

func (t *Task) Do() error {
	return t.job()
}

func NewPool() *Pool {
	return &Pool{
		tasks:        queue.NewLockfreeQueue[Job](),
		workers:      &sync.Map{},
		workersAlive: &atomic.Int64{},
	}
}

func (w *Worker) RunWorker() {
	log.Printf("worker %d started \n", w.id)
	for {
		select {
		case quit, ok := <-w.quit:
			if ok && quit {
				log.Printf("worker %d is killed \n", w.id)
				return
			}
		default:
			task, ok := w.tasks.Pop()
			if !ok {
				continue
			}
			log.Printf("worker %d got task %s\n", w.id, task.ID())
			w.jobResult <- JobResult{
				JobID: task.ID(),
				Err:   task.Do(),
			}
		}
	}
}

const defaultWorkersCount = 2

func (p *Pool) Start(ctx context.Context) {
	p.AddWorkers(defaultWorkersCount)

	<-ctx.Done()
	p.RemoveWorkers(int(p.workersAlive.Load()))
	time.Sleep(5 * time.Second)
	return
}

func (p *Pool) AddWorkers(count int) {
	for i := 0; i < count; i++ {
		p.workersAlive.Add(1)
		w := Worker{
			id:        p.workersAlive.Load(),
			tasks:     p.tasks,
			quit:      make(chan bool),
			jobResult: p.Subscribe(),
		}
		p.workers.Store(w.id, w)

		go w.RunWorker()
	}
}

func (p *Pool) RemoveWorkers(count int) {
	p.workers.Range(func(id, w any) bool {
		worker, ok := w.(Worker)
		if !ok {
			return false
		}

		alive := p.workersAlive.Load()
		if count <= 0 {
			return true
		}
		worker.quit <- true

		p.workers.Delete(id)
		p.workersAlive.Store(alive - 1)

		count--
		return true
	})
}

func (p *Pool) AddJob(job Job) {
	p.tasks.Push(job)
}

func (p *Pool) Subscribe() chan JobResult {
	return make(chan JobResult, 1000)
}

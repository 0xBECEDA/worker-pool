package main

import (
	"context"
	"errors"
	"github.com/stretchr/testify/assert"
	"log"
	"testing"
	"time"
)

func TestStartWorker(t *testing.T) {
	t.Run("start worker pool", func(t *testing.T) {
		ctx, cancel := context.WithCancel(context.Background())
		pool := NewPool()
		go pool.Start(ctx)

		time.Sleep(1 * time.Second)
		assert.Equal(t, defaultWorkersCount, int(pool.workersAlive.Load()))
		assert.Equal(t, defaultWorkersCount, len(pool.workers))

		cancel()
		time.Sleep(2 * time.Second)
	})
}

func TestAddWorkers(t *testing.T) {
	t.Run("add workers successfully", func(t *testing.T) {
		ctx, cancel := context.WithCancel(context.Background())
		pool := NewPool()
		go pool.Start(ctx)

		pool.AddWorkers(3)
		time.Sleep(1 * time.Second)

		assert.Equal(t, defaultWorkersCount+3, int(pool.workersAlive.Load()))
		assert.Equal(t, defaultWorkersCount+3, len(pool.workers))

		cancel()
		time.Sleep(3 * time.Second)
	})
}

func TestRemoveWorkers(t *testing.T) {
	t.Run("remove workers successfully", func(t *testing.T) {
		ctx, cancel := context.WithCancel(context.Background())
		pool := NewPool()
		go pool.Start(ctx)

		time.Sleep(1 * time.Second)
		pool.RemoveWorkers(3)

		defer func() {
			assert.Equal(t, 0, int(pool.workersAlive.Load()))
			assert.Equal(t, 0, len(pool.workers))
			cancel()
		}()
	})
}

func TestDoSomeJob(t *testing.T) {
	t.Run("run job and get result successfully", func(t *testing.T) {
		const expectedResultsCount = 2
		jobSuccess := NewTask("1", func() error {
			return nil
		})

		jobFail := NewTask("2", func() error {
			return errors.New("task error")
		})

		ctx, cancel := context.WithCancel(context.Background())
		pool := NewPool()
		go pool.Start(ctx)
		time.Sleep(2 * time.Second)

		pool.AddJob(jobSuccess)
		pool.AddJob(jobFail)

		resCount := 0
	loop:
		for _, w := range pool.workers {
			select {
			case res := <-w.jobResult:
				log.Printf("job %s, error '%v' \n", res.JobID, res.Err)
				resCount++
				if resCount == expectedResultsCount {
					cancel()
					return
				}
			default:
				continue
			}
		}
		goto loop
	})
}

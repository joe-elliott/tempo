package livetraces

import (
	"sync"
	"time"

	"github.com/segmentio/fasthash/fnv1a"
)

type Sizer interface {
	Size() int
}

type Trace[T Sizer] struct {
	ID        []byte
	timestamp time.Time
	Batches   []T

	sz uint64
}

type Tracker[T Sizer] struct {
	traces map[uint64]*Trace[T]

	sz uint64

	mtx sync.RWMutex
}

func New[T Sizer]() *Tracker[T] {
	return &Tracker[T]{
		traces: make(map[uint64]*Trace[T]),
	}
}

func (l *Tracker[T]) token(traceID []byte) uint64 {
	return fnv1a.HashBytes64(traceID)
}

func (l *Tracker[T]) Len() uint64 {
	l.mtx.RLock()
	defer l.mtx.RUnlock()

	return uint64(len(l.traces))
}

func (l *Tracker[T]) Size() uint64 {
	l.mtx.RLock()
	defer l.mtx.RUnlock()

	return l.sz
}

func (l *Tracker[T]) Push(traceID []byte, batch T, max uint64) bool {
	l.mtx.RLock()
	unlock := func() {
		l.mtx.RUnlock()
	}

	token := l.token(traceID)

	tr := l.traces[token]
	if tr == nil {
		// Before adding this check against max
		// Zero means no limit
		if max > 0 && uint64(len(l.traces)) >= max {
			unlock()
			return false
		}

		// exchange read lock for write lock and overwrite unlock so we can unlock correctly
		l.mtx.RUnlock()
		l.mtx.Lock()
		unlock = func() {
			l.mtx.Unlock()
		}

		// check again. during the exchange, another goroutine may have added the trace. it is also possible
		// that we exceeded the max while exchanging locks, but it should only be minimally
		tr = l.traces[token]
		if tr == nil {
			tr = &Trace[T]{
				ID: traceID,
			}
			l.traces[token] = tr
		}
	}

	sz := uint64(batch.Size())
	tr.sz += sz
	l.sz += sz

	tr.Batches = append(tr.Batches, batch)
	tr.timestamp = time.Now()

	unlock()

	return true
}

func (l *Tracker[T]) CutIdle(idleSince time.Time, immediate bool) []*Trace[T] {
	l.mtx.Lock()
	defer l.mtx.Unlock()

	res := []*Trace[T]{}

	for k, tr := range l.traces {
		if tr.timestamp.Before(idleSince) || immediate {
			res = append(res, tr)
			l.sz -= tr.sz
			delete(l.traces, k)
		}
	}

	return res
}

func (l *Tracker[T]) Lookup(id []byte) *Trace[T] {
	l.mtx.RLock()
	defer l.mtx.RUnlock()

	return l.traces[l.token(id)]
}

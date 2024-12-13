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

	mtx sync.Mutex
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
	l.mtx.Lock()
	defer l.mtx.Unlock()

	return uint64(len(l.traces))
}

func (l *Tracker[T]) Size() uint64 {
	l.mtx.Lock()
	defer l.mtx.Unlock()

	return l.sz
}

func (l *Tracker[T]) Push(traceID []byte, batch T, max uint64) bool {
	l.mtx.Lock()
	defer l.mtx.Unlock()

	token := l.token(traceID)

	tr := l.traces[token]
	if tr == nil {
		// Before adding this check against max
		// Zero means no limit
		if max > 0 && uint64(len(l.traces)) >= max {
			return false
		}

		tr = &Trace[T]{
			ID: traceID,
		}
		l.traces[token] = tr
	}

	sz := uint64(batch.Size())
	tr.sz += sz
	l.sz += sz

	tr.Batches = append(tr.Batches, batch)
	tr.timestamp = time.Now()

	return true
}

func (l *Tracker[T]) CutIdle(idleSince time.Time, immediate bool) []*Trace[T] { // jpe - can return length and size
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
	l.mtx.Lock()
	defer l.mtx.Unlock()

	return l.traces[l.token(id)]
}

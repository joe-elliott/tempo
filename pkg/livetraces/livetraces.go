package livetraces

import (
	"hash"
	"hash/fnv"
	"sync"
	"time"
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
	hash   hash.Hash64
	traces map[uint64]*Trace[T]

	sz uint64

	mtx sync.RWMutex
}

func New[T Sizer]() *Tracker[T] {
	return &Tracker[T]{
		hash:   fnv.New64(),
		traces: make(map[uint64]*Trace[T]),
	}
}

// token must be called under lock
func (l *Tracker[T]) token(traceID []byte) uint64 {
	l.hash.Reset()
	l.hash.Write(traceID)
	return l.hash.Sum64()
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

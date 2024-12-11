package localblocks

import (
	"hash"
	"hash/fnv"
	"sync"
	"time"
)

type sizer interface {
	Size() int
}

type liveTrace[T sizer] struct {
	id        []byte
	timestamp time.Time
	Batches   []T

	sz uint64
}

type liveTraces[T sizer] struct {
	hash   hash.Hash64
	traces map[uint64]*liveTrace[T]

	sz uint64

	mtx sync.RWMutex
}

func newLiveTraces[T sizer]() *liveTraces[T] {
	return &liveTraces[T]{
		hash:   fnv.New64(),
		traces: make(map[uint64]*liveTrace[T]),
	}
}

// token must be called under lock
func (l *liveTraces[T]) token(traceID []byte) uint64 {
	l.hash.Reset()
	l.hash.Write(traceID)
	return l.hash.Sum64()
}

func (l *liveTraces[T]) Len() uint64 {
	l.mtx.RLock()
	defer l.mtx.RUnlock()

	return uint64(len(l.traces))
}

func (l *liveTraces[T]) Size() uint64 {
	l.mtx.RLock()
	defer l.mtx.RUnlock()

	return l.sz
}

func (l *liveTraces[T]) Push(traceID []byte, batch T, max uint64) bool {
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

		tr = &liveTrace[T]{
			id: traceID,
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

func (l *liveTraces[T]) CutIdle(idleSince time.Time, immediate bool) []*liveTrace[T] {
	l.mtx.Lock()
	defer l.mtx.Unlock()

	res := []*liveTrace[T]{}

	for k, tr := range l.traces {
		if tr.timestamp.Before(idleSince) || immediate {
			res = append(res, tr)
			l.sz -= tr.sz
			delete(l.traces, k)
		}
	}

	return res
}

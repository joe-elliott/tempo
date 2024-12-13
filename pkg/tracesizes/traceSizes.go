package tracesizes

import (
	"sync"
	"time"

	"github.com/segmentio/fasthash/fnv1a"
)

type Tracker struct {
	mtx   sync.RWMutex
	sizes map[uint64]*traceSize
}

type traceSize struct {
	size      int
	timestamp time.Time
}

func New() *Tracker {
	return &Tracker{
		sizes: make(map[uint64]*traceSize),
	}
}

func (s *Tracker) token(traceID []byte) uint64 {
	return fnv1a.HashBytes64(traceID)
}

// Allow returns true if the historical total plus incoming size is less than
// or equal to the max.  The historical total is kept alive and incremented even
// if not allowed, so that long-running traces are cutoff as expected.
func (s *Tracker) Allow(traceID []byte, sz, max int) bool {
	s.mtx.RLock()
	unlock := func() {
		s.mtx.RUnlock()
	}

	token := s.token(traceID)
	tr := s.sizes[token]
	if tr == nil {
		// exchange locks
		s.mtx.RUnlock()
		s.mtx.Lock()
		unlock = func() {
			s.mtx.Unlock()
		}

		// check again. during the exchange, another goroutine may have added the trace
		tr = s.sizes[token]
		if tr == nil {
			tr = &traceSize{
				size: 0, // size added below
			}

			s.sizes[token] = tr
		}
	}

	tr.timestamp = time.Now()
	tr.size += sz

	unlock()
	return tr.size <= max
}

func (s *Tracker) ClearIdle(idleSince time.Time) {
	s.mtx.Lock()
	defer s.mtx.Unlock()

	for token, tr := range s.sizes {
		if tr.timestamp.Before(idleSince) {
			delete(s.sizes, token)
		}
	}
}

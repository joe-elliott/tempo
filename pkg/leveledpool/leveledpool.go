package leveledpool

import (
	"math/bits"
	"sync"
)

type Pool[T any] struct {
	baseSize int
	levels   int
	pools    []sync.Pool
}

func New[T any](baseSize int, levels int) *Pool[T] {
	if baseSize == 0 || levels == 0 {
		panic("both baseSize and levels must be greater than 0")
	}

	return &Pool[T]{
		baseSize: baseSize,
		levels:   levels,
		pools:    make([]sync.Pool, levels),
	}
}

func (p *Pool[T]) Get(sz int) []T {
	i := p.levelForSize(sz)
	if i < 0 {
		// requested slice is too large for our pools, just allocate a new one
		return make([]T, 0, sz)
	}
	s := p.pools[i].Get()
	if s == nil {
		sz = p.baseSize << i
		return make([]T, 0, sz)
	}
	return s.([]T)
}

func (p *Pool[T]) Put(s []T) {
	sz := cap(s)
	if sz < p.baseSize { // this object won't make slices smaller then p.baseSize so just drop this
		return
	}
	sz = sz / 2 // divide by 2 to force this slice to go to the next pool down. this will guarantee that the returned slices are always large enough
	i := p.levelForSize(sz)
	if i < 0 {
		// this slice doesn't fit in our pools, just put in the largest one
		i = len(p.pools) - 1
	}
	p.pools[i].Put(s[:0])
}

func (p *Pool[T]) levelForSize(sz int) int {
	i := sz / p.baseSize
	i = 32 - bits.LeadingZeros32(uint32(i)) // log2
	if i >= len(p.pools) {
		return -1 // signal that the requested slice was too large for our pools by returning a negative index
	}
	if i < 0 {
		i = 0
	}
	return i
}

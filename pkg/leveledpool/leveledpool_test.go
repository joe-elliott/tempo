package leveledpool

import (
	"math/rand"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

func TestAlwaysBigEnoughSlice(t *testing.T) {
	r := rand.New(rand.NewSource(time.Now().Unix()))

	for i := 0; i < 10; i++ {
		base := (r.Intn(10) + 1) * 512
		levels := r.Intn(10) + 1
		p := New[byte](base, levels)

		for j := 0; j < 100; j++ {
			sz := r.Intn(10000) + 1
			slice := p.Get(sz)
			require.GreaterOrEqual(t, cap(slice), sz)
			p.Put(slice)
		}
	}
}

func TestLevelForSize(t *testing.T) {
	tcs := []struct {
		baseSize int
		pools    int
		sz       int
		expected int
	}{
		{
			baseSize: 1024,
			pools:    3,
			sz:       1023,
			expected: 0,
		},
		{
			baseSize: 1024,
			pools:    3,
			sz:       1024,
			expected: 1,
		},
		{
			baseSize: 1024,
			pools:    3,
			sz:       2048,
			expected: 2,
		},
		{
			baseSize: 1024,
			pools:    3,
			sz:       4095,
			expected: 2,
		},
		{
			baseSize: 1024,
			pools:    3,
			sz:       4096,
			expected: -1,
		},
	}

	for _, tc := range tcs {
		p := New[byte](tc.baseSize, tc.pools)
		require.Equal(t, tc.expected, p.levelForSize(tc.sz))
	}
}

package tracesizes

import "testing"

func BenchmarkToken(b *testing.B) {
	s := New()
	traceID := []byte("test")
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		s.token(traceID)
	}
}

func BenchmarkAllow(b *testing.B) {
	s := New()
	// create a pool of 10 traceIDs
	traceIDs := make([][]byte, 100)
	for i := 0; i < 100; i++ {
		traceIDs[i] = []byte{byte(i)}
	}
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		// choose a random traceID
		traceID := traceIDs[i%10]
		s.Allow(traceID, 1, 1)
	}
}

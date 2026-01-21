package mc

import (
	"strings"
	"testing"
)

func BenchmarkSet(b *testing.B) {
	b.StopTimer()
	c := NewMC(mcAddr, user, pass)
	// Lazy connection. Make sure it connects before starting benchmark.
	_, err := c.Set("foo", "bar", 0, 0, 0)
	if err != nil {
		b.Skip("memcached not available")
	}

	b.StartTimer()
	b.ReportAllocs()

	for i := 0; i < b.N; i++ {
		_, err := c.Set("foo", "bar", 0, 0, 0)
		if err != nil {
			b.Fatal(err)
		}
	}
}

func BenchmarkGet(b *testing.B) {
	b.StopTimer()
	c := NewMC(mcAddr, user, pass)
	_, err := c.Set("bench_key", "bench_value", 0, 0, 0)
	if err != nil {
		b.Skip("memcached not available")
	}

	b.StartTimer()
	b.ReportAllocs()

	for i := 0; i < b.N; i++ {
		_, _, _, err := c.Get("bench_key")
		if err != nil {
			b.Fatal(err)
		}
	}
}

func BenchmarkSetSmall(b *testing.B) {
	b.StopTimer()
	c := NewMC(mcAddr, user, pass)
	value := strings.Repeat("x", 100)
	_, err := c.Set("bench_small", value, 0, 0, 0)
	if err != nil {
		b.Skip("memcached not available")
	}

	b.StartTimer()
	b.ReportAllocs()

	for i := 0; i < b.N; i++ {
		_, err := c.Set("bench_small", value, 0, 0, 0)
		if err != nil {
			b.Fatal(err)
		}
	}
}

func BenchmarkSetMedium(b *testing.B) {
	b.StopTimer()
	c := NewMC(mcAddr, user, pass)
	value := strings.Repeat("x", 1000)
	_, err := c.Set("bench_medium", value, 0, 0, 0)
	if err != nil {
		b.Skip("memcached not available")
	}

	b.StartTimer()
	b.ReportAllocs()

	for i := 0; i < b.N; i++ {
		_, err := c.Set("bench_medium", value, 0, 0, 0)
		if err != nil {
			b.Fatal(err)
		}
	}
}

func BenchmarkSetLarge(b *testing.B) {
	b.StopTimer()
	c := NewMC(mcAddr, user, pass)
	value := strings.Repeat("x", 10000)
	_, err := c.Set("bench_large", value, 0, 0, 0)
	if err != nil {
		b.Skip("memcached not available")
	}

	b.StartTimer()
	b.ReportAllocs()

	for i := 0; i < b.N; i++ {
		_, err := c.Set("bench_large", value, 0, 0, 0)
		if err != nil {
			b.Fatal(err)
		}
	}
}

func BenchmarkGetSmall(b *testing.B) {
	b.StopTimer()
	c := NewMC(mcAddr, user, pass)
	value := strings.Repeat("x", 100)
	_, err := c.Set("bench_get_small", value, 0, 0, 0)
	if err != nil {
		b.Skip("memcached not available")
	}

	b.StartTimer()
	b.ReportAllocs()

	for i := 0; i < b.N; i++ {
		_, _, _, err := c.Get("bench_get_small")
		if err != nil {
			b.Fatal(err)
		}
	}
}

func BenchmarkGetMedium(b *testing.B) {
	b.StopTimer()
	c := NewMC(mcAddr, user, pass)
	value := strings.Repeat("x", 1000)
	_, err := c.Set("bench_get_medium", value, 0, 0, 0)
	if err != nil {
		b.Skip("memcached not available")
	}

	b.StartTimer()
	b.ReportAllocs()

	for i := 0; i < b.N; i++ {
		_, _, _, err := c.Get("bench_get_medium")
		if err != nil {
			b.Fatal(err)
		}
	}
}

func BenchmarkGetLarge(b *testing.B) {
	b.StopTimer()
	c := NewMC(mcAddr, user, pass)
	value := strings.Repeat("x", 10000)
	_, err := c.Set("bench_get_large", value, 0, 0, 0)
	if err != nil {
		b.Skip("memcached not available")
	}

	b.StartTimer()
	b.ReportAllocs()

	for i := 0; i < b.N; i++ {
		_, _, _, err := c.Get("bench_get_large")
		if err != nil {
			b.Fatal(err)
		}
	}
}

func BenchmarkIncr(b *testing.B) {
	b.StopTimer()
	c := NewMC(mcAddr, user, pass)
	_, _, err := c.Incr("bench_incr", 1, 0, 0, 0)
	if err != nil {
		b.Skip("memcached not available")
	}

	b.StartTimer()
	b.ReportAllocs()

	for i := 0; i < b.N; i++ {
		_, _, err := c.Incr("bench_incr", 1, 0, 0, 0)
		if err != nil {
			b.Fatal(err)
		}
	}
}

// BenchmarkHasher benchmarks the hasher performance
func BenchmarkHasher(b *testing.B) {
	h := NewModuloHasher()
	// Simulate having 10 servers
	servers := make([]*server, 10)
	for i := range servers {
		servers[i] = &server{address: "localhost:11211", isAlive: true}
	}
	h.update(servers)

	b.ReportAllocs()
	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		_, _ = h.getServerIndex("some_cache_key")
	}
}

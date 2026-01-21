# mc.go: A High-Performance Go client for Memcached

[![godoc](https://godoc.org/github.com/Morditux/mc?status.svg)](http://godoc.org/github.com/Morditux/mc)

This is a (pure) Go client for [Memcached](http://memcached.org). It supports
the binary Memcached protocol, SASL authentication and Compression. It's thread-safe.
It allows connections to entire Memcached clusters and supports connection
pools, timeouts, and failover.

This fork is highly optimized for memory efficiency, using buffer pooling and tiered memory management to minimize GC pressure in high-throughput environments.

## Install

Module-aware mode:

```bash
go get github.com/Morditux/mc
```

Legacy GOPATH mode:

```bash
go get github.com/Morditux/mc
```

## Use

```go
import "github.com/Morditux/mc"

func main() {
  // Error handling omitted for demo

  // Only PLAIN SASL auth supported right now
  c := mc.NewMC("localhost:11211", "username", "password")
  defer c.Quit()

  exp := 3600 // 2 hours
  cas, err = c.Set("foo", "bar", flags, exp, cas)
  if err != nil {
   ...
  }

  val, flags, cas, err = c.Get("foo")
  if err != nil {
   ...
  }

  err = c.Del("foo")
  if err != nil {
   ...
  }
}
```

## Features

- **Binary Protocol**: Complete support for the Memcached binary protocol.
- **SASL Authentication**: Support for PLAIN SASL authentication.
- **Memory Efficient**: Uses `sync.Pool` with tiered buffers (256B, 4KB, 64KB) to reduce allocations.
- **Optimized Hot Paths**: Internal structures and hashers optimized to minimize heap allocations.
- **Failover & Pooling**: Built-in connection pooling and automatic failover for cluster support.
- **Compression**: Flexible support for zlib or gzip compression.

## Performance & Benchmarks

The library is designed for performance. The recent optimizations have reduced allocations to a consistent **9 allocations per operation** for most Get/Set requests, regardless of value size.

Running benchmarks on local machine:

```
BenchmarkSet-12          20292    58981 ns/op     496 B/op    9 allocs/op
BenchmarkGet-12          19879    58003 ns/op     432 B/op    9 allocs/op
BenchmarkSetSmall-12     19284    62728 ns/op     440 B/op    9 allocs/op
BenchmarkSetMedium-12    19284    61708 ns/op     440 B/op    9 allocs/op
BenchmarkSetLarge-12     13347    88996 ns/op     440 B/op    9 allocs/op
BenchmarkGetSmall-12     19680    61881 ns/op     577 B/op    9 allocs/op
BenchmarkGetMedium-12    18968    64686 ns/op    1446 B/op    9 allocs/op
BenchmarkGetLarge-12     16568    76551 ns/op   10936 B/op    9 allocs/op
BenchmarkHasher-12    22224796       51 ns/op      16 B/op    1 allocs/op
```

## Missing Feature

There is nearly coverage of the Memcached protocol.
The biggest missing protocol feature is support for `multi_get` and other
batched operations.

There is also no support for asynchronous IO.

## Performance

Right now we use a single per-connection mutex and don't support pipe-lining any
operations. There is however support for connection pools which should make up
for it.

## Get involved

We are happy to receive bug reports, fixes, documentation enhancements,
and other improvements.

Please report bugs via the
[github issue tracker](http://github.com/Morditux/mc/issues).

Master [git repository](http://github.com/Morditux/mc):

- `git clone git://github.com/Morditux/mc.git`

## Licensing

This library is MIT-licensed.

## Authors

New authors:

- [Morditux](https://github.com/Morditux) (current maintainer)

---

This is a fork of [mc](https://github.com/memcachier/mc) by MemCachier.
It was originally written by [Blake Mizerany](https://github.com/bmizerany/mc).

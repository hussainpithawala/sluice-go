# sluice

[![CI](https://github.com/hussainpithawala/sluice-go/actions/workflows/ci.yml/badge.svg)](https://github.com/hussainpithawala/sluice-go/actions/workflows/ci.yml)
[![Go Reference](https://pkg.go.dev/badge/github.com/hussainpithawala/sluice-go.svg)](https://pkg.go.dev/github.com/hussainpithawala/sluice-go)
[![Go Version](https://img.shields.io/badge/go-1.25-blue)](https://go.dev)
[![License: MIT](https://img.shields.io/badge/License-MIT-yellow.svg)](LICENSE)

**Wide-breadth Redis-shielded write batcher for document stores.**

## Install

```bash
go get github.com/hussainpithawala/sluice-go@latest
```

## Quickstart

```go
import (
    sluice "github.com/hussainpithawala/sluice-go"
    "github.com/hussainpithawala/sluice-go/sink/docdb"
)

sk, _ := docdb.New(ctx, docdb.DefaultConfig(uri, "mydb", "inventory"))

s, _ := sluice.New("inventory").
    WithRedis(sluice.RedisConfig{Addrs: []string{"redis:6379"}}).
    WithSink(sk).
    WithWriteContract(func(key string, p []byte) (*sluice.WriteModel, error) {
        return &sluice.WriteModel{
            Filter: bson.D{{"_id", key}},
            Update: bson.D{{"$set", bson.Raw(p)}},
            Upsert: true,
        }, nil
    }).Build(ctx)

defer s.DrainAndClose(ctx)
s.Write(ctx, correlationKey, payload) // returns immediately
```

## Testing

```bash
make test-unit          # unit tests (Redis auto-started)
make test-integration   # full stack: Redis + MongoDB + Kafka + LocalStack
make test-all           # both, then tear down
```

## License

MIT — see [LICENSE](LICENSE).

package sluice

import (
	"time"

	"github.com/hussainpithawala/sluice-go/internal/engine"
	"github.com/hussainpithawala/sluice-go/internal/shield"
)

// Config holds every tunable for the library.
// Construct via the Builder — do not instantiate directly.
type Config struct {
	Namespace          string
	BandCount          int
	FlushWindow        time.Duration
	MaxBatchSize       int
	KeyTTL             time.Duration
	DegradedModeDirect bool
	Redis              RedisConfig
	Metrics            MetricsRecorder
}

// RedisConfig holds Redis connectivity parameters.
// Single Addrs entry = standalone mode. Multiple entries = Cluster mode.
type RedisConfig struct {
	Addrs        []string
	Password     string
	DB           int
	DialTimeout  time.Duration
	ReadTimeout  time.Duration
	WriteTimeout time.Duration
	PoolSize     int
}

// toInternal converts to internal shield.RedisConfig.
func (c RedisConfig) toInternal() shield.RedisConfig {
	return shield.RedisConfig{
		Addrs: c.Addrs, Password: c.Password, DB: c.DB,
		DialTimeout: c.DialTimeout, ReadTimeout: c.ReadTimeout,
		WriteTimeout: c.WriteTimeout, PoolSize: c.PoolSize,
	}
}

// toInternal converts to internal engine.Config.
func (c Config) toInternal() engine.Config {
	return engine.Config{
		Namespace:          c.Namespace,
		BandCount:          c.BandCount,
		FlushWindow:        c.FlushWindow,
		MaxBatchSize:       c.MaxBatchSize,
		KeyTTL:             c.KeyTTL,
		DegradedModeDirect: c.DegradedModeDirect,
		Redis:              c.Redis.toInternal(),
		Metrics:            c.Metrics,
	}
}

func defaultConfig(namespace string) Config {
	return Config{
		Namespace:          namespace,
		BandCount:          16,
		FlushWindow:        250 * time.Millisecond,
		MaxBatchSize:       1000,
		KeyTTL:             30 * time.Second,
		DegradedModeDirect: true,
	}
}

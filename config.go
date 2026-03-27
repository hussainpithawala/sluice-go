package sluice

import "time"

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

package sluice

import (
	"time"

	"github.com/hussainpithawala/sluice-go/internal/engine"
	"github.com/hussainpithawala/sluice-go/internal/shield"
)

// toInternal converts to internal shield.RedisConfig.
func (c RedisConfig) toInternal() shield.RedisConfig {
	return shield.RedisConfig{
		Network:      c.Network,
		Addrs:        c.Addrs,
		ClusterMode:  c.ClusterMode,
		Username:     c.Username,
		Password:     c.Password,
		DB:           c.DB,
		DialTimeout:  c.DialTimeout,
		ReadTimeout:  c.ReadTimeout,
		WriteTimeout: c.WriteTimeout,
		PoolSize:     c.PoolSize,
		TLSConfig:    c.TLSConfig,
	}
}

// toInternal converts to internal engine.Config.
// Note: ReadContract and IndexContract are NOT passed to the engine.
// The engine only handles the write path (flushing dirty keys to the sink).
// The read path (HotLoad/Read) is orchestrated directly by Sluice using the Source.
func (c Config) toInternal() engine.Config {
	return engine.Config{
		Namespace:      c.Namespace,
		BandCount:      c.BandCount,
		FlushWindow:    c.FlushWindow,
		MaxBatchSize:   c.MaxBatchSize,
		KeyTTL:         c.KeyTTL,
		ActivityWindow: c.ActivityWindow,
		HotAwareFlush:  c.HotAwareFlush,
	}
}

func defaultConfig(namespace string) Config {
	return Config{
		Namespace:          namespace,
		BandCount:          16,
		FlushWindow:        250 * time.Millisecond,
		MaxBatchSize:       1000,
		KeyTTL:             30 * time.Second,
		ActivityWindow:     4 * time.Hour,
		IdempotencyTTL:     4 * time.Hour,
		DegradedModeDirect: true,
		HotAwareFlush:      true,
		ContentDedup:       false,
	}
}

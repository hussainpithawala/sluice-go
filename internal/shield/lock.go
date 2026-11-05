package shield

import (
	"context"
	"crypto/rand"
	"fmt"
	"time"

	"github.com/redis/go-redis/v9"
)

var podID = randomID()

func randomID() string {
	b := make([]byte, 16)
	_, _ = rand.Read(b)
	return fmt.Sprintf("%x", b)
}

func (s *Shield) flushLockKey(band int) string {
	return fmt.Sprintf("sl:%s:flush_lock:{%d}", s.namespace, band)
}

// TryFlushLock attempts to acquire an advisory lock for flushing a band.
// Returns true if the lock was acquired. The lock auto-expires after ttl
// so a crashed pod's lock does not block others indefinitely.
func (s *Shield) TryFlushLock(ctx context.Context, band int, ttl time.Duration) (bool, error) {
	return s.client.SetNX(ctx, s.flushLockKey(band), podID, ttl).Result()
}

// releaseFlushLockLua atomically checks ownership before deleting, preventing
// a pod from releasing another pod's lock after its own expired.
const releaseFlushLockLua = `
if redis.call('GET', KEYS[1]) == ARGV[1] then
    return redis.call('DEL', KEYS[1])
end
return 0`

var releaseScript = redis.NewScript(releaseFlushLockLua)

// ReleaseFlushLock releases the advisory lock for a band, but only if this
// pod still owns it.
func (s *Shield) ReleaseFlushLock(ctx context.Context, band int) error {
	return releaseScript.Run(ctx, s.client, []string{s.flushLockKey(band)}, podID).Err()
}

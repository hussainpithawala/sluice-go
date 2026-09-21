package broadcast

import (
	"context"
	"errors"
	"log/slog"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/hussainpithawala/sluice-go/internal/localjournal"
	"github.com/redis/go-redis/v9"
)

// Subscriber reads the L1 broadcast stream and applies messages to the local
// in-process cache. One instance runs per namespace per pod.
//
// Design invariants:
//   - Starts reading from ID "0" to catch up on retained history (bounded by MAXLEN).
//   - Applies messages blindly (including its own pod's broadcasts); the L1 cache's
//     strict version gating (ts > existing_ts) handles same-pod redundancy for free.
//   - In ModeInvalidation, it calls local.Invalidate() instead of Put(), forcing
//     the next Read() to fall through to the Redis journal (lazy heal).
type Subscriber struct {
	client  redis.UniversalClient
	stream  string
	local   *localjournal.Cache
	metrics MetricsRecorder
	cfg     Config

	// cursor is the last successfully processed Stream ID.
	// Protected by cursorMu. Starts empty (translates to "0" on first read).
	cursor   string
	cursorMu sync.RWMutex

	// Lifecycle management
	ctx    context.Context
	cancel context.CancelFunc
	wg     sync.WaitGroup
}

// NewSubscriber creates a new broadcast subscriber.
// It does not start the background goroutines until Start() is called.
func NewSubscriber(
	client redis.UniversalClient,
	stream string,
	local *localjournal.Cache,
	metrics MetricsRecorder,
	cfg Config,
) *Subscriber {
	if cfg.BlockMS <= 0 {
		cfg.BlockMS = 50 * time.Millisecond // ← was 1s, now 50ms
	}
	return &Subscriber{
		client:  client,
		stream:  stream,
		local:   local,
		metrics: metrics,
		cfg:     cfg,
	}
}

// Start launches the background XREAD loop and the lag monitor.
// Non-blocking. Safe to call only once per instance.
func (s *Subscriber) Start() {
	s.ctx, s.cancel = context.WithCancel(context.Background())

	// DIAGNOSTIC: Prove Start() was called and what stream it's watching
	slog.Info("SUBSCRIBER-STARTING", "stream", s.stream, "namespace", s.cfg.Namespace)

	s.wg.Add(2)
	go s.run(s.ctx)
	go s.runLagMonitor(s.ctx)
}

// Stop signals the subscriber to drain and exit. Blocks until the background
// goroutines have fully terminated. Safe to call multiple times.
func (s *Subscriber) Stop() {
	if s.cancel != nil {
		s.cancel()
	}
	s.wg.Wait()
}

// run is the main blocked XREAD loop.
func (s *Subscriber) runO(ctx context.Context) {
	defer s.wg.Done()
	// DIAGNOSTIC: Prove the goroutine actually started running
	slog.Info("SUBSCRIBER-RUN-ENTERED", "stream", s.stream)

	// Add a panic catcher just in case it's crashing silently
	defer func() {
		if r := recover(); r != nil {
			slog.Error("SUBSCRIBER-PANIC", "stream", s.stream, "panic", r)
		}
	}()

	for {
		// Check for shutdown before blocking
		select {
		case <-ctx.Done():
			return
		default:
		}

		cursor := s.getCursor()

		// DIAGNOSTIC: Log every time we attempt to read (will log every BlockMS)
		slog.Info("SUBSCRIBER-LOOP-TICK", "stream", s.stream, "cursor", cursor)

		// Use a SHORT block timeout (50ms) instead of a long one.
		// XREAD BLOCK on Redis Cluster clients can fail to wake up promptly
		// when the XADD arrives via a pipelined connection. A short block
		// ensures we poll frequently and catch new messages within ~50ms.
		res, err := s.client.XRead(ctx, &redis.XReadArgs{
			Streams: []string{s.stream, cursor},
			Count:   100,
			Block:   50 * time.Millisecond, // ← SHORT block, not 1s
		}).Result()

		if err != nil {
			// Context cancelled means Stop() was called. Exit cleanly.
			if errors.Is(err, context.Canceled) || errors.Is(err, context.DeadlineExceeded) {
				return
			}
			// redis.Nil means the Block timeout elapsed with no new messages. This is normal.
			if errors.Is(err, redis.Nil) {
				continue
			}

			// Network blip or stream doesn't exist yet. Log and backoff.
			slog.Warn("broadcast XREAD error", "stream", s.stream, "err", err)
			select {
			case <-ctx.Done():
				return
			case <-time.After(1 * time.Second):
			}
			continue
		}

		// Process messages and advance cursor
		var lastID string
		for _, xStream := range res {
			for _, msg := range xStream.Messages {
				s.apply(msg)
				lastID = msg.ID
			}
		}

		if lastID != "" {
			s.setCursor(lastID)
		}
	}
}

func (s *Subscriber) run(ctx context.Context) {
	defer s.wg.Done()

	const (
		blockTimeout = 5 * time.Second // bounded, so ctx/shutdown is observed regularly
		minBackoff   = 100 * time.Millisecond
		maxBackoff   = 5 * time.Second
	)

	cursor := s.getCursor() // single owner goroutine -> keep it local
	backoff := minBackoff

	for ctx.Err() == nil {
		res, err := s.client.XRead(ctx, &redis.XReadArgs{
			Streams: []string{s.stream, cursor},
			Count:   100,
			Block:   blockTimeout, // NEVER 0 (= forever); use -1 for non-blocking
		}).Result()

		if err != nil {
			switch {
			case errors.Is(err, redis.Nil):
				// Block timeout elapsed with no data: normal, loop again.
				backoff = minBackoff
				continue
			case ctx.Err() != nil:
				return
			default:
				slog.Warn("broadcast XREAD error", "stream", s.stream, "err", err)
				select {
				case <-ctx.Done():
					return
				case <-time.After(backoff):
				}
				if backoff *= 2; backoff > maxBackoff {
					backoff = maxBackoff
				}
				continue
			}
		}
		backoff = minBackoff

		for _, xs := range res {
			for _, msg := range xs.Messages {
				s.apply(msg)
				cursor = msg.ID // advance per message, not per batch
				s.setCursor(cursor)
			}
		}
	}
}

// apply processes a single stream message.
func (s *Subscriber) apply(msg redis.XMessage) {
	crn, _ := msg.Values["crn"].(string)
	tsStr, _ := msg.Values["ts"].(string)
	kindStr, _ := msg.Values["kind"].(string)

	// DIAGNOSTIC: Confirm the subscriber actually received the message
	slog.Info("SUBSCRIBER-APPLY", "crn", crn, "ts", tsStr, "kind", kindStr)

	if crn == "" || tsStr == "" {
		return
	}

	ts, err := strconv.ParseInt(tsStr, 10, 64)
	if err != nil {
		return
	}

	switch Kind(kindStr) {
	case KindUpsert, KindSeed:
		if s.cfg.Mode == ModePayload {
			payloadStr, ok := msg.Values["payload"].(string)
			if !ok || payloadStr == "" {
				slog.Warn("SUBSCRIBER-INVALIDATE", "crn", crn, "reason", "missing payload")
				s.local.Invalidate(crn)
				return
			}
			// DIAGNOSTIC: Confirm Put was called and whether it succeeded
			applied := s.local.Put(crn, []byte(payloadStr), ts)
			slog.Debug("SUBSCRIBER-PUT", "crn", crn, "ts", ts, "applied", applied)
		} else {
			s.local.Invalidate(crn)
		}
	case KindTouch:
		// Deferred
	}
}

// runLagMonitor periodically calculates and emits the broadcast lag metric.
func (s *Subscriber) runLagMonitor(ctx context.Context) {
	defer s.wg.Done()

	ticker := time.NewTicker(1 * time.Second)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			s.emitLag(ctx)
		}
	}
}

// emitLag fetches the stream's last-generated ID and compares it to the current cursor.
func (s *Subscriber) emitLag(ctx context.Context) {
	if s.metrics == nil {
		return
	}

	cursor := s.getCursor()
	if cursor == "" || cursor == "0" {
		// Haven't processed anything yet, or just starting. Skip lag emission
		// to avoid reporting the entire stream retention as "lag".
		return
	}

	info, err := s.client.XInfoStream(ctx, s.stream).Result()
	if err != nil {
		// Stream might not exist yet, or transient network error. Ignore.
		return
	}

	lastTS := parseStreamIDTimestamp(info.LastGeneratedID)
	cursorTS := parseStreamIDTimestamp(cursor)

	if lastTS > 0 && cursorTS > 0 && lastTS >= cursorTS {
		lag := time.Duration(lastTS-cursorTS) * time.Millisecond
		s.metrics.RecordBroadcastLag(s.cfg.Namespace, lag)
	}
}

// ── Helpers ─────────────────────────────────────────────────────────────────

func (s *Subscriber) getCursor() string {
	s.cursorMu.RLock()
	defer s.cursorMu.RUnlock()
	if s.cursor == "" {
		return "0" // Start from the beginning of retained history
	}
	return s.cursor
}

func (s *Subscriber) setCursor(id string) {
	s.cursorMu.Lock()
	s.cursor = id
	s.cursorMu.Unlock()
}

// parseStreamIDTimestamp extracts the millisecond timestamp from a Redis Stream ID.
// Stream IDs are formatted as "<milliseconds>-<sequence>" (e.g., "1695123456789-0").
func parseStreamIDTimestamp(id string) int64 {
	parts := strings.Split(id, "-")
	if len(parts) == 0 {
		return 0
	}
	ts, _ := strconv.ParseInt(parts[0], 10, 64)
	return ts
}

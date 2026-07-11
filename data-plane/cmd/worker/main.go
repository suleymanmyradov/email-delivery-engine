// Command worker is the email-delivery-engine data plane. It consumes message
// IDs from a Redis Stream consumer group, runs each message through the
// delivery pipeline, retries/dead-letters as needed, re-injects deferred
// messages whose backoff has elapsed, periodically limits abusive senders, and
// exposes Prometheus metrics.
package main

import (
	"context"
	"errors"
	"log/slog"
	"math/rand"
	"net/http"
	"os"
	"os/signal"
	"sync"
	"syscall"
	"time"

	"github.com/suleyman/email-delivery-engine/internal/config"
	"github.com/suleyman/email-delivery-engine/internal/metrics"
	"github.com/suleyman/email-delivery-engine/internal/queue"
	"github.com/suleyman/email-delivery-engine/internal/store"
	"github.com/suleyman/email-delivery-engine/internal/throttle"
	"github.com/suleyman/email-delivery-engine/internal/worker"
)

const (
	readBatch        = 10
	readBlock        = 5 * time.Second
	reclaimMinIdle   = time.Minute
	deferredInterval = 15 * time.Second
	abuseInterval    = 60 * time.Second
	metricsInterval  = 10 * time.Second
	deferredBatch    = 100
)

func main() {
	log := slog.New(slog.NewJSONHandler(os.Stdout, &slog.HandlerOptions{Level: slog.LevelInfo}))
	slog.SetDefault(log)

	if err := run(log); err != nil {
		log.Error("fatal", "error", err)
		os.Exit(1)
	}
}

func run(log *slog.Logger) error {
	cfg, err := config.Load()
	if err != nil {
		return err
	}

	metrics.Init()

	// Root context cancelled on SIGINT/SIGTERM for graceful shutdown.
	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer stop()

	st, err := store.New(cfg.DatabaseURL)
	if err != nil {
		return err
	}
	defer st.Close()

	q, err := queue.New(cfg.RedisURL, cfg.QueueStream, cfg.QueueGroup, cfg.ConsumerName)
	if err != nil {
		return err
	}
	defer q.Close()
	if err := q.Ping(ctx); err != nil {
		return err
	}
	if err := q.EnsureGroup(ctx); err != nil {
		return err
	}

	limiter, err := throttle.NewFromURL(cfg.RedisURL)
	if err != nil {
		return err
	}
	defer limiter.Close()

	// Seed the RNG once; used for jitter and simulated provider behaviour.
	rng := rand.New(rand.NewSource(time.Now().UnixNano()))
	proc := worker.NewProcessor(st, limiter, log, cfg.MaxAttempts, rng)

	log.Info("worker starting",
		"consumer", cfg.ConsumerName,
		"stream", cfg.QueueStream,
		"group", cfg.QueueGroup,
		"max_attempts", cfg.MaxAttempts,
		"metrics_addr", cfg.MetricsAddr)

	var wg sync.WaitGroup
	wg.Add(4)
	go func() { defer wg.Done(); runConsumer(ctx, log, q, proc) }()
	go func() { defer wg.Done(); runDeferredPoller(ctx, log, st, q) }()
	go func() { defer wg.Done(); runAbuseSweep(ctx, log, proc) }()
	go func() { defer wg.Done(); runMetricsUpdater(ctx, log, q) }()

	metricsSrv := startMetricsServer(log, cfg.MetricsAddr)

	<-ctx.Done()
	log.Info("shutdown signal received; draining")

	// Give in-flight work a moment, then stop the metrics server.
	shutCtx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	if err := metricsSrv.Shutdown(shutCtx); err != nil {
		log.Warn("metrics server shutdown", "error", err)
	}

	wg.Wait()
	log.Info("worker stopped")
	return nil
}

// runConsumer reads from the stream and processes each entry. Entries that
// error are left unacked so a later reclaim retries them; successful ones are
// acked. Stale entries from crashed consumers are reclaimed on each idle tick.
func runConsumer(ctx context.Context, log *slog.Logger, q *queue.Queue, proc *worker.Processor) {
	// Reclaim anything left pending by a previous run before reading new work.
	reclaimAndProcess(ctx, log, q, proc)

	for ctx.Err() == nil {
		entries, err := q.Read(ctx, readBatch, readBlock)
		if err != nil {
			if ctx.Err() != nil {
				return
			}
			log.Error("stream read failed", "error", err)
			sleep(ctx, time.Second)
			continue
		}
		if len(entries) == 0 {
			// Idle window elapsed — good time to reclaim stranded entries.
			reclaimAndProcess(ctx, log, q, proc)
			continue
		}
		for _, e := range entries {
			processEntry(ctx, log, q, proc, e)
		}
	}
}

func reclaimAndProcess(ctx context.Context, log *slog.Logger, q *queue.Queue, proc *worker.Processor) {
	entries, err := q.Reclaim(ctx, reclaimMinIdle, readBatch)
	if err != nil {
		if ctx.Err() == nil {
			log.Error("reclaim failed", "error", err)
		}
		return
	}
	for _, e := range entries {
		processEntry(ctx, log, q, proc, e)
	}
}

func processEntry(ctx context.Context, log *slog.Logger, q *queue.Queue, proc *worker.Processor, e queue.Entry) {
	if e.MessageID == "" {
		log.Warn("stream entry missing message_id; acking", "entry_id", e.ID)
		_ = q.Ack(ctx, e.ID)
		return
	}
	if err := proc.Process(ctx, e.MessageID); err != nil {
		metrics.Inc("worker_process_errors_total")
		log.Error("process failed; leaving entry pending for reclaim",
			"message_id", e.MessageID, "entry_id", e.ID, "error", err)
		return // do not ack — reclaim will retry
	}
	if err := q.Ack(ctx, e.ID); err != nil {
		log.Error("ack failed", "message_id", e.MessageID, "entry_id", e.ID, "error", err)
	}
}

// runDeferredPoller re-injects deferred messages whose next_attempt_at has
// passed by flipping them to queued and XADDing them back onto the stream.
func runDeferredPoller(ctx context.Context, log *slog.Logger, st *store.Store, q *queue.Queue) {
	ticker := time.NewTicker(deferredInterval)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			ids, err := st.GetDeferredMessagesReady(ctx, deferredBatch)
			if err != nil {
				log.Error("deferred poll failed", "error", err)
				continue
			}
			for _, id := range ids {
				if err := st.MarkQueued(ctx, id); err != nil {
					log.Error("requeue mark failed", "message_id", id, "error", err)
					continue
				}
				if err := q.Enqueue(ctx, id); err != nil {
					log.Error("requeue enqueue failed", "message_id", id, "error", err)
				}
			}
			if len(ids) > 0 {
				log.Info("re-enqueued deferred messages", "count", len(ids))
			}
		}
	}
}

func runAbuseSweep(ctx context.Context, log *slog.Logger, proc *worker.Processor) {
	ticker := time.NewTicker(abuseInterval)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			proc.RunAbuseSweep(ctx)
		}
	}
}

func runMetricsUpdater(ctx context.Context, log *slog.Logger, q *queue.Queue) {
	ticker := time.NewTicker(metricsInterval)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			depth, err := q.Depth(ctx)
			if err != nil {
				if ctx.Err() == nil {
					log.Error("queue depth failed", "error", err)
				}
				continue
			}
			metrics.SetGauge("queue_depth", float64(depth))
		}
	}
}

func startMetricsServer(log *slog.Logger, addr string) *http.Server {
	mux := http.NewServeMux()
	mux.Handle("/metrics", metrics.Handler())
	mux.HandleFunc("/health", func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write([]byte(`{"status":"ok"}`))
	})
	srv := &http.Server{Addr: addr, Handler: mux}
	go func() {
		if err := srv.ListenAndServe(); err != nil && !errors.Is(err, http.ErrServerClosed) {
			log.Error("metrics server failed", "error", err)
		}
	}()
	return srv
}

func sleep(ctx context.Context, d time.Duration) {
	t := time.NewTimer(d)
	defer t.Stop()
	select {
	case <-ctx.Done():
	case <-t.C:
	}
}

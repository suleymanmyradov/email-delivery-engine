// Package queue wraps the Redis Streams consumer-group protocol used to hand
// messages from the control plane to the worker.
//
// The control plane XADDs {message_id} onto the stream. The worker reads via a
// consumer group so each message is delivered to exactly one consumer and stays
// in the Pending Entries List (PEL) until acked — giving at-least-once delivery
// with recovery of entries left behind by a crashed consumer.
package queue

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"time"

	"github.com/redis/go-redis/v9"
)

const messageIDField = "message_id"

// Queue is a Redis Streams consumer-group client.
type Queue struct {
	rdb      *redis.Client
	stream   string
	group    string
	consumer string
}

// Entry is one stream entry to process.
type Entry struct {
	ID        string // Redis stream entry ID, needed to ack
	MessageID string
}

// New connects to Redis and returns a Queue.
func New(redisURL, stream, group, consumer string) (*Queue, error) {
	opt, err := redis.ParseURL(redisURL)
	if err != nil {
		return nil, fmt.Errorf("parse redis url: %w", err)
	}
	rdb := redis.NewClient(opt)
	return &Queue{rdb: rdb, stream: stream, group: group, consumer: consumer}, nil
}

// Ping verifies connectivity.
func (q *Queue) Ping(ctx context.Context) error {
	return q.rdb.Ping(ctx).Err()
}

func (q *Queue) Close() error {
	return q.rdb.Close()
}

// EnsureGroup creates the consumer group (and the stream) if it does not exist.
func (q *Queue) EnsureGroup(ctx context.Context) error {
	err := q.rdb.XGroupCreateMkStream(ctx, q.stream, q.group, "$").Err()
	if err != nil && !isBusyGroup(err) {
		return fmt.Errorf("create consumer group: %w", err)
	}
	return nil
}

// isBusyGroup reports whether the error is Redis's "group already exists"
// response, which is expected and safe to ignore on every boot.
func isBusyGroup(err error) bool {
	return err != nil && strings.Contains(err.Error(), "BUSYGROUP")
}

// Enqueue appends a message ID to the stream. Used by the deferred poller to
// re-inject messages whose backoff has elapsed.
func (q *Queue) Enqueue(ctx context.Context, messageID string) error {
	return q.rdb.XAdd(ctx, &redis.XAddArgs{
		Stream: q.stream,
		Values: map[string]any{messageIDField: messageID},
	}).Err()
}

// Read blocks up to `block` for new entries for this consumer, returning at
// most `count` entries. Returns an empty slice (not an error) on timeout.
func (q *Queue) Read(ctx context.Context, count int64, block time.Duration) ([]Entry, error) {
	res, err := q.rdb.XReadGroup(ctx, &redis.XReadGroupArgs{
		Group:    q.group,
		Consumer: q.consumer,
		Streams:  []string{q.stream, ">"},
		Count:    count,
		Block:    block,
	}).Result()
	if errors.Is(err, redis.Nil) {
		return nil, nil // no new entries within the block window
	}
	if err != nil {
		return nil, fmt.Errorf("xreadgroup: %w", err)
	}
	return toEntries(res), nil
}

// Reclaim takes over entries idle longer than minIdle that were left pending by
// other (likely crashed) consumers, so no message is stranded in the PEL.
func (q *Queue) Reclaim(ctx context.Context, minIdle time.Duration, count int64) ([]Entry, error) {
	msgs, _, err := q.rdb.XAutoClaim(ctx, &redis.XAutoClaimArgs{
		Stream:   q.stream,
		Group:    q.group,
		Consumer: q.consumer,
		MinIdle:  minIdle,
		Start:    "0",
		Count:    count,
	}).Result()
	if err != nil {
		return nil, fmt.Errorf("xautoclaim: %w", err)
	}
	return xMessagesToEntries(msgs), nil
}

// Ack acknowledges an entry so it leaves the Pending Entries List.
func (q *Queue) Ack(ctx context.Context, entryID string) error {
	return q.rdb.XAck(ctx, q.stream, q.group, entryID).Err()
}

// Depth returns the current number of entries in the stream (queue_depth gauge).
func (q *Queue) Depth(ctx context.Context) (int64, error) {
	return q.rdb.XLen(ctx, q.stream).Result()
}

func toEntries(streams []redis.XStream) []Entry {
	var out []Entry
	for _, s := range streams {
		out = append(out, xMessagesToEntries(s.Messages)...)
	}
	return out
}

func xMessagesToEntries(msgs []redis.XMessage) []Entry {
	out := make([]Entry, 0, len(msgs))
	for _, m := range msgs {
		id, _ := m.Values[messageIDField].(string)
		out = append(out, Entry{ID: m.ID, MessageID: id})
	}
	return out
}

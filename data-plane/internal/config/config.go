package config

import (
	"fmt"
	"os"
	"strconv"
)

// Config holds all runtime configuration for the worker, read from the
// environment. Defaults mirror control-plane/.env.example and docker-compose.
type Config struct {
	DatabaseURL  string
	RedisURL     string
	QueueStream  string
	QueueGroup   string
	ConsumerName string
	MetricsAddr  string

	// MaxAttempts is the number of delivery attempts before a message is
	// dead-lettered. Matches the plan's schedule (attempt 5 -> dead-letter).
	MaxAttempts int
}

// Load reads configuration from the environment, applying defaults.
func Load() (*Config, error) {
	c := &Config{
		DatabaseURL:  getenv("DATABASE_URL", "postgresql://ede:ede@localhost:5432/email_delivery_engine"),
		RedisURL:     getenv("REDIS_URL", "redis://localhost:6379"),
		QueueStream:  getenv("QUEUE_STREAM", "messages"),
		QueueGroup:   getenv("QUEUE_GROUP", "worker"),
		ConsumerName: getenv("CONSUMER_NAME", defaultConsumerName()),
		MetricsAddr:  getenv("METRICS_ADDR", ":9100"),
		MaxAttempts:  getenvInt("MAX_ATTEMPTS", 4),
	}

	if c.DatabaseURL == "" {
		return nil, fmt.Errorf("DATABASE_URL is required")
	}
	if c.RedisURL == "" {
		return nil, fmt.Errorf("REDIS_URL is required")
	}
	if c.MaxAttempts < 1 {
		return nil, fmt.Errorf("MAX_ATTEMPTS must be >= 1, got %d", c.MaxAttempts)
	}
	return c, nil
}

func defaultConsumerName() string {
	if h, err := os.Hostname(); err == nil && h != "" {
		return "worker-" + h
	}
	return "worker-1"
}

func getenv(key, fallback string) string {
	if v := os.Getenv(key); v != "" {
		return v
	}
	return fallback
}

func getenvInt(key string, fallback int) int {
	if v := os.Getenv(key); v != "" {
		if n, err := strconv.Atoi(v); err == nil {
			return n
		}
	}
	return fallback
}

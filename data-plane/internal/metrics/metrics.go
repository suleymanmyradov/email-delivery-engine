// Package metrics provides a tiny, dependency-free in-process metric registry
// with a Prometheus text-format exposition endpoint. For a learning project
// this mirrors the control plane's approach; production would use
// prometheus/client_golang with histograms and a proper registry.
package metrics

import (
	"fmt"
	"net/http"
	"sort"
	"sync"
)

type metricType string

const (
	counter metricType = "counter"
	gauge   metricType = "gauge"
)

type entry struct {
	typ   metricType
	help  string
	value float64
}

var (
	mu       sync.Mutex
	registry = map[string]*entry{}
)

// definitions pre-registers every worker metric so /metrics always exposes a
// stable surface (a counter reads 0 rather than being absent before first use).
var definitions = []struct {
	name string
	typ  metricType
	help string
}{
	{"messages_delivered_total", counter, "Total messages successfully delivered"},
	{"messages_bounced_total", counter, "Total messages that hard-bounced"},
	{"messages_deferred_total", counter, "Total messages deferred for retry"},
	{"messages_dead_lettered_total", counter, "Total messages moved to the DLQ"},
	{"messages_throttled_total", counter, "Total sends throttled by rate limits"},
	{"messages_suppressed_total", counter, "Total sends skipped due to suppression"},
	{"delivery_attempts_total", counter, "Total delivery attempts made by the worker"},
	{"retry_scheduled_total", counter, "Total retries scheduled"},
	{"provider_rate_limited_total", counter, "Total provider rate-limit responses received"},
	{"abuse_blocks_total", counter, "Total sends blocked by abuse controls"},
	{"abuse_limited_customers_total", counter, "Total times a customer was moved to limited state"},
	{"worker_process_errors_total", counter, "Total unexpected errors while processing a message"},
	{"queue_depth", gauge, "Current number of entries in the delivery stream"},
}

// Init registers all known metrics with zero values.
func Init() {
	mu.Lock()
	defer mu.Unlock()
	for _, d := range definitions {
		registry[d.name] = &entry{typ: d.typ, help: d.help}
	}
}

// Inc increments a counter by 1.
func Inc(name string) { Add(name, 1) }

// Add increments a counter by n.
func Add(name string, n float64) {
	mu.Lock()
	defer mu.Unlock()
	e := registry[name]
	if e == nil {
		e = &entry{typ: counter}
		registry[name] = e
	}
	e.value += n
}

// SetGauge sets a gauge value.
func SetGauge(name string, v float64) {
	mu.Lock()
	defer mu.Unlock()
	e := registry[name]
	if e == nil {
		e = &entry{typ: gauge}
		registry[name] = e
	}
	e.value = v
}

// Handler returns an http.Handler that renders metrics in Prometheus text format.
func Handler() http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		mu.Lock()
		names := make([]string, 0, len(registry))
		for name := range registry {
			names = append(names, name)
		}
		sort.Strings(names)
		snapshot := make([]struct {
			name string
			e    entry
		}, 0, len(names))
		for _, name := range names {
			snapshot = append(snapshot, struct {
				name string
				e    entry
			}{name, *registry[name]})
		}
		mu.Unlock()

		w.Header().Set("Content-Type", "text/plain; version=0.0.4")
		for _, s := range snapshot {
			if s.e.help != "" {
				fmt.Fprintf(w, "# HELP %s %s\n", s.name, s.e.help)
			}
			fmt.Fprintf(w, "# TYPE %s %s\n", s.name, s.e.typ)
			fmt.Fprintf(w, "%s %g\n", s.name, s.e.value)
		}
	})
}

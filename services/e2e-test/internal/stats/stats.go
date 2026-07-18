package stats

import (
	"fmt"
	"log"
	"sort"
	"time"
)

// Event is one operation performed by an actor. OK is the HTTP outcome;
// VerifyOK is whether the read-back assertions passed (always true for ops
// that carry no assertions).
type Event struct {
	Actor    string
	Op       string
	OK       bool
	VerifyOK bool
	Latency  time.Duration
	Detail   string
}

type opStats struct {
	total       int
	httpFails   int
	verifyFails int
	totalLat    time.Duration
	maxLat      time.Duration
}

// Collector aggregates events from all actors in a single goroutine, so no
// locking is needed — actors only ever touch the channel.
type Collector struct {
	events chan Event
	done   chan struct{}

	perOp map[string]*opStats
	start time.Time
}

func NewCollector(buffer int) *Collector {
	return &Collector{
		events: make(chan Event, buffer),
		done:   make(chan struct{}),
		perOp:  make(map[string]*opStats),
		start:  time.Now(),
	}
}

// Record is called by actors. It never blocks shutdown correctness: the
// channel is buffered and only closed after every actor goroutine has exited.
func (c *Collector) Record(e Event) {
	c.events <- e
}

// Run consumes events and prints a report every reportInterval until the
// events channel is closed, then prints the final report and signals Done.
func (c *Collector) Run(reportInterval time.Duration) {
	ticker := time.NewTicker(reportInterval)
	defer ticker.Stop()

	for {
		select {
		case e, ok := <-c.events:
			if !ok {
				c.report("FINAL")
				close(c.done)
				return
			}
			c.apply(e)
		case <-ticker.C:
			c.report("periodic")
		}
	}
}

// Close stops the collector after all actors are done recording.
func (c *Collector) Close() {
	close(c.events)
	<-c.done
}

func (c *Collector) apply(e Event) {
	s, exists := c.perOp[e.Op]
	if !exists {
		s = &opStats{}
		c.perOp[e.Op] = s
	}

	s.total++
	if !e.OK {
		s.httpFails++
	} else if !e.VerifyOK {
		s.verifyFails++
	}
	s.totalLat += e.Latency
	if e.Latency > s.maxLat {
		s.maxLat = e.Latency
	}

	if !e.OK || !e.VerifyOK {
		log.Printf("[%s] %s FAILED: %s", e.Actor, e.Op, e.Detail)
	}
}

func (c *Collector) report(kind string) {
	ops := make([]string, 0, len(c.perOp))
	for op := range c.perOp {
		ops = append(ops, op)
	}
	sort.Strings(ops)

	fmt.Printf("\n===== %s report (uptime %s) =====\n", kind, time.Since(c.start).Round(time.Second))
	fmt.Printf("%-28s %8s %10s %12s %10s %10s\n", "op", "total", "http_fail", "verify_fail", "avg_lat", "max_lat")
	for _, op := range ops {
		s := c.perOp[op]
		avg := time.Duration(0)
		if s.total > 0 {
			avg = s.totalLat / time.Duration(s.total)
		}
		fmt.Printf("%-28s %8d %10d %12d %10s %10s\n",
			op, s.total, s.httpFails, s.verifyFails,
			avg.Round(time.Millisecond), s.maxLat.Round(time.Millisecond))
	}
	fmt.Println()
}

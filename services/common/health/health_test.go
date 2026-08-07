package health

import (
	"testing"
	"time"
)

func TestMarkNotReady_IsStickyAgainstLaterUpdateReady(t *testing.T) {
	c := NewChecker(nil, time.Hour)
	c.state.Ready = true // simulate a prior successful check

	c.MarkNotReady()
	if c.GetState().Ready {
		t.Fatal("expected Ready=false immediately after MarkNotReady")
	}

	// Previously, updateReady on the next tick could flip Ready back to
	// true if the ping happened to succeed during shutdown — this
	// simulates that tick without needing a real Pinger, since
	// updateReady must now short-circuit on shuttingDown before ever
	// touching c.pinger.
	c.updateReady()

	if c.GetState().Ready {
		t.Error("expected Ready to stay false after MarkNotReady, even after another updateReady tick")
	}
}

func TestMarkNotLive_IsIdempotentFirstReasonWins(t *testing.T) {
	c := NewChecker(nil, time.Hour)

	c.MarkNotLive("first reason")
	c.MarkNotLive("second reason")

	if c.notLiveReason != "first reason" {
		t.Errorf("expected the first reason to win, got %q", c.notLiveReason)
	}
	if c.GetState().Live {
		t.Error("expected Live=false after MarkNotLive")
	}
}

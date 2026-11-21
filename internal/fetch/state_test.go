package fetch

import (
	"testing"
	"time"
)

func TestAdaptiveInterval(t *testing.T) {
	tests := []struct {
		name    string
		items   int
		elapsed time.Duration
		floor   time.Duration
		want    time.Duration
	}{
		{"two per minute is the fastest", 2, time.Minute, 0, 60 * time.Second},
		{"burst of items", 100, time.Minute, 0, 60 * time.Second},
		{"about two per minute", 119, time.Hour, 0, 60 * time.Second},
		{"one per minute", 1, time.Minute, 0, 60 * time.Second},
		{"one every two minutes", 1, 2 * time.Minute, 0, 120 * time.Second},
		{"one every three minutes", 1, 3 * time.Minute, 0, 180 * time.Second},
		{"a trickle", 1, 10 * time.Minute, 0, 300 * time.Second},
		{"quiet feed", 0, time.Minute, 0, 300 * time.Second},
		{"sub-second elapsed is clamped", 1, 0, 0, 60 * time.Second},
		{"negative elapsed is clamped", 1, -time.Hour, 0, 60 * time.Second},
		{"floor wins over a fast feed", 10, time.Minute, 180 * time.Second, 180 * time.Second},
		{"floor above five minutes is kept", 1, time.Minute, 10 * time.Minute, 10 * time.Minute},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := AdaptiveInterval(tt.items, tt.elapsed, tt.floor); got != tt.want {
				t.Errorf("AdaptiveInterval(%d, %v, %v) = %v, want %v", tt.items, tt.elapsed, tt.floor, got, tt.want)
			}
		})
	}
	// Intermediate rates map to intermediate intervals, not just the extremes.
	mid := AdaptiveInterval(1, 90*time.Second, 0)
	if mid <= 60*time.Second || mid >= 300*time.Second {
		t.Errorf("0.67/min should sit between the extremes, got %v", mid)
	}
}

// TestFeedStateEWMA checks the interval across several polls with varying item
// counts: the smoothed rate must produce intermediate intervals, and the
// source interval must act as a floor.
func TestFeedStateEWMA(t *testing.T) {
	base := time.Date(2026, 11, 20, 12, 0, 0, 0, time.UTC)
	s := NewFeedState(FeedStateOptions{Interval: time.Minute, Alpha: 0.5})
	poll := func(at time.Time, items int) time.Duration {
		s.Begin(at)
		return s.OnSuccess(at, items)
	}
	// A busy feed starts at the floor and then relaxes as it goes quiet; the
	// interval must pass through intermediate values, not jump 60→300.
	first := poll(base.Add(time.Minute), 6)
	if first != time.Minute {
		t.Fatalf("first interval = %v, want the 1 m floor", first)
	}
	at := base.Add(time.Minute)
	sawIntermediate := false
	for i := 0; i < 6; i++ {
		at = at.Add(4 * time.Minute)
		got := poll(at, 0)
		if got > time.Minute && got < MaxFeedInterval {
			sawIntermediate = true
		}
		if got != s.Interval() {
			t.Fatalf("interval mismatch: %v vs %v", got, s.Interval())
		}
	}
	if !sawIntermediate {
		t.Errorf("never saw an intermediate interval; final %v", s.Interval())
	}
	// Many more quiet polls relax all the way to the cap.
	for i := 0; i < 10; i++ {
		at = at.Add(5 * time.Minute)
		poll(at, 0)
	}
	if s.Interval() != MaxFeedInterval {
		t.Errorf("quiet interval = %v, want %v", s.Interval(), MaxFeedInterval)
	}
	// A floor above the cap is respected.
	slow := NewFeedState(FeedStateOptions{Interval: 10 * time.Minute})
	if got := slow.OnSuccess(base.Add(10*time.Minute), 0); got != 10*time.Minute {
		t.Errorf("slow floor = %v, want 10m", got)
	}
}

func TestFeedStateSuccessAndNotModified(t *testing.T) {
	now := time.Date(2026, 11, 20, 12, 0, 0, 0, time.UTC)
	s := NewFeedState(FeedStateOptions{Interval: time.Minute})
	if s.State() != StateOK || !s.Next().IsZero() || s.Interval() != time.Minute {
		t.Fatalf("initial = %v %v %v", s.State(), s.Next(), s.Interval())
	}
	s.Begin(now)
	got := s.OnSuccess(now.Add(time.Minute), 10) // 10 per minute
	if got != time.Minute || s.Interval() != time.Minute || !s.Next().Equal(now.Add(2*time.Minute)) {
		t.Errorf("success = %v next %v", got, s.Next())
	}
	s.OnFailure(now, false)
	if s.State() != StateBackoff {
		t.Fatalf("state = %v", s.State())
	}
	s.Begin(now)
	s.OnSuccess(now.Add(time.Minute), 0)
	if s.State() != StateOK || s.Failures() != 0 || s.Interval() < time.Minute {
		t.Errorf("recovery = %v failures %d interval %v", s.State(), s.Failures(), s.Interval())
	}
	s.OnFailure(now, true)
	if got := s.OnNotModified(now); got < time.Minute || s.State() != StateOK || s.Failures() != 0 || s.ParseFailures() != 0 {
		t.Errorf("304 = %v %v %d/%d", got, s.State(), s.Failures(), s.ParseFailures())
	}
	s.WithNext(now.Add(time.Hour))
	if !s.Next().Equal(now.Add(time.Hour)) {
		t.Error("WithNext not applied")
	}
}

func TestFeedStateBackoffGrowsAndCaps(t *testing.T) {
	now := time.Date(2026, 11, 20, 12, 0, 0, 0, time.UTC)
	s := NewFeedState(FeedStateOptions{
		BackoffBase:     10 * time.Second,
		BackoffMax:      60 * time.Second,
		QuarantineAfter: 100,
	})
	want := []time.Duration{10, 20, 40, 60, 60, 60}
	for i, w := range want {
		got := s.OnFailure(now, false)
		if got != w*time.Second || !s.Next().Equal(now.Add(w*time.Second)) || s.State() != StateBackoff {
			t.Errorf("failure %d: delay %v next %v state %v, want %vs", i+1, got, s.Next(), s.State(), int64(w))
		}
	}
	if s.Failures() != len(want) || s.ParseFailures() != 0 {
		t.Errorf("counters = %d/%d", s.Failures(), s.ParseFailures())
	}
}

func TestFeedStateQuarantineAndCoolOff(t *testing.T) {
	now := time.Date(2026, 11, 20, 12, 0, 0, 0, time.UTC)
	s := NewFeedState(FeedStateOptions{QuarantineAfter: 3, QuarantineFor: 10 * time.Minute})
	s.OnFailure(now, true)
	s.OnFailure(now, false) // network errors do not count toward quarantine
	if s.State() != StateBackoff || s.ParseFailures() != 1 {
		t.Fatalf("state = %v parse %d", s.State(), s.ParseFailures())
	}
	s.OnFailure(now, true)
	if s.State() != StateBackoff {
		t.Fatal("two parse failures must not quarantine yet")
	}
	delay := s.OnFailure(now, true)
	if s.State() != StateQuarantined || delay != 10*time.Minute {
		t.Fatalf("state = %v delay %v", s.State(), delay)
	}
	until := now.Add(10 * time.Minute)
	if !s.QuarantinedUntil().Equal(until) || !s.Next().Equal(until) {
		t.Errorf("until %v next %v", s.QuarantinedUntil(), s.Next())
	}
	if s.Failures() != 0 || s.ParseFailures() != 0 {
		t.Errorf("counters must reset for the next set of attempts: %d/%d", s.Failures(), s.ParseFailures())
	}

	s.ResetQuarantine(until.Add(-time.Second)) // still cooling off
	if s.State() != StateQuarantined {
		t.Error("reset before cool-off must be a no-op")
	}
	s.ResetQuarantine(until)
	if s.State() != StateOK || !s.QuarantinedUntil().IsZero() {
		t.Errorf("reset at cool-off end: %v %v", s.State(), s.QuarantinedUntil())
	}
	// A fresh set of attempts starts at the base backoff.
	if got := s.OnFailure(until, true); got != DefaultFeedBackoffBase {
		t.Errorf("first failure after reset backs off %v", got)
	}
}

func TestFeedStateSuccessClearsQuarantineAndOnBlocked(t *testing.T) {
	now := time.Date(2026, 11, 20, 12, 0, 0, 0, time.UTC)
	s := NewFeedState(FeedStateOptions{QuarantineFor: time.Hour})
	if got := s.OnBlocked(now); got != time.Hour || s.State() != StateQuarantined || !s.Next().Equal(now.Add(time.Hour)) {
		t.Fatalf("blocked = %v %v %v", got, s.State(), s.Next())
	}
	if s.ParseFailures() != 0 {
		t.Error("a policy block is not a parse failure")
	}
	s.Begin(now)
	s.OnSuccess(now.Add(time.Minute), 1)
	if s.State() != StateOK || !s.QuarantinedUntil().IsZero() {
		t.Errorf("success after block: %v %v", s.State(), s.QuarantinedUntil())
	}
}

func TestStateString(t *testing.T) {
	for state, want := range map[State]string{StateOK: "ok", StateBackoff: "backoff", StateQuarantined: "quarantined", State(9): "unknown"} {
		if got := state.String(); got != want {
			t.Errorf("State(%d) = %q, want %q", int(state), got, want)
		}
	}
}

package fetch

import (
	"time"
)

// Adaptive interval bounds from the plan: feeds are polled every 1–5 minutes
// depending on how fast they publish. A source's configured interval_s is a
// floor, so a slow upstream such as EONET is never polled faster than asked.
const (
	MinFeedInterval = time.Minute
	MaxFeedInterval = 5 * time.Minute

	DefaultFeedBackoffBase = 30 * time.Second
	DefaultFeedBackoffMax  = 15 * time.Minute
	DefaultQuarantineAfter = 5
	DefaultQuarantineFor   = 30 * time.Minute
	DefaultRateAlpha       = 0.4
)

// FeedStateOptions configure one feed's scheduling state.
type FeedStateOptions struct {
	// Interval is the floor for the adaptive interval (source interval_s).
	Interval        time.Duration
	BackoffBase     time.Duration
	BackoffMax      time.Duration
	QuarantineAfter int
	QuarantineFor   time.Duration
	// Alpha is the EWMA weight for the newest observed item rate. Default 0.4.
	Alpha float64
}

// FeedState is the per-feed schedule: an EWMA of the observed item rate sets
// the interval, failures back off exponentially, repeated parse errors
// quarantine with a cool-off. It is pure state; the agent owns the goroutine
// and the clock.
type FeedState struct {
	floor           time.Duration
	maxInterval     time.Duration
	alpha           float64
	baseBackoff     time.Duration
	maxBackoff      time.Duration
	quarantineAfter int
	quarantineFor   time.Duration

	interval         time.Duration
	rate             float64 // EWMA items per minute
	haveRate         bool
	failures         int
	parseFailures    int
	state            State
	next             time.Time
	quarantinedUntil time.Time
	prevAttempt      time.Time
	lastAttempt      time.Time
	lastSuccess      time.Time
}

// NewFeedState builds a feed state with defaults for the zero fields.
func NewFeedState(opts FeedStateOptions) *FeedState {
	if opts.Interval <= 0 {
		opts.Interval = 3 * time.Minute
	}
	if opts.BackoffBase <= 0 {
		opts.BackoffBase = DefaultFeedBackoffBase
	}
	if opts.BackoffMax <= 0 {
		opts.BackoffMax = DefaultFeedBackoffMax
	}
	if opts.BackoffMax < opts.BackoffBase {
		opts.BackoffMax = opts.BackoffBase
	}
	if opts.QuarantineAfter <= 0 {
		opts.QuarantineAfter = DefaultQuarantineAfter
	}
	if opts.QuarantineFor <= 0 {
		opts.QuarantineFor = DefaultQuarantineFor
	}
	if opts.Alpha <= 0 || opts.Alpha > 1 {
		opts.Alpha = DefaultRateAlpha
	}
	maxInterval := MaxFeedInterval
	if opts.Interval > maxInterval {
		maxInterval = opts.Interval
	}
	return &FeedState{
		floor:           opts.Interval,
		maxInterval:     maxInterval,
		alpha:           opts.Alpha,
		baseBackoff:     opts.BackoffBase,
		maxBackoff:      opts.BackoffMax,
		quarantineAfter: opts.QuarantineAfter,
		quarantineFor:   opts.QuarantineFor,
		interval:        opts.Interval,
		state:           StateOK,
	}
}

// Next is when the feed should next be polled. Zero means "as soon as the
// agent starts", which the agent staggers with jitter.
func (s *FeedState) Next() time.Time { return s.next }

// WithNext overrides the next attempt time. Fixed-cadence producers use it.
func (s *FeedState) WithNext(at time.Time) { s.next = at }

// State is the current fetch state.
func (s *FeedState) State() State { return s.state }

// Interval is the current adaptive interval.
func (s *FeedState) Interval() time.Duration { return s.interval }

// Rate is the smoothed observed item rate in items per minute.
func (s *FeedState) Rate() float64 { return s.rate }

// Failures and ParseFailures expose the consecutive counters for health.
func (s *FeedState) Failures() int      { return s.failures }
func (s *FeedState) ParseFailures() int { return s.parseFailures }

// QuarantinedUntil is the cool-off end when the state is quarantined.
func (s *FeedState) QuarantinedUntil() time.Time { return s.quarantinedUntil }

// ResetQuarantine clears a quarantine after its cool-off has elapsed so the
// feed gets a fresh set of attempts. The agent calls it before a scheduled
// retry.
func (s *FeedState) ResetQuarantine(now time.Time) {
	if s.state == StateQuarantined && !now.Before(s.quarantinedUntil) {
		s.state = StateOK
		s.quarantinedUntil = time.Time{}
	}
}

// Begin records that an attempt started at now. The previous attempt's start
// is kept so the first successful poll can measure the interval it actually
// waited, not the fetch duration.
func (s *FeedState) Begin(now time.Time) {
	s.prevAttempt = s.lastAttempt
	s.lastAttempt = now
}

// OnSuccess records a successful fetch that produced newItems new items and
// returns the next polling interval.
//
// The rate is measured over wall time since the previous successful poll (not
// since the attempt started, and not a fixed one-second window), then smoothed
// with an EWMA. interval = 60s / rate, clamped to [floor, max(5m, floor)].
func (s *FeedState) OnSuccess(now time.Time, newItems int) time.Duration {
	elapsed := now.Sub(s.lastSuccess)
	if s.lastSuccess.IsZero() {
		if !s.prevAttempt.IsZero() {
			elapsed = now.Sub(s.prevAttempt)
		} else {
			elapsed = s.floor
		}
	}
	if elapsed < time.Second {
		elapsed = time.Second
	}
	observed := float64(newItems) / elapsed.Minutes()
	if !s.haveRate {
		s.rate = observed
		s.haveRate = true
	} else {
		s.rate = s.alpha*observed + (1-s.alpha)*s.rate
	}
	s.lastSuccess = now
	s.failures = 0
	s.parseFailures = 0
	s.state = StateOK
	s.quarantinedUntil = time.Time{}
	s.interval = s.intervalForRate(s.rate)
	s.next = now.Add(s.interval)
	return s.interval
}

// OnNotModified records a 304: the feed is healthy but quiet, so the observed
// rate decays toward zero and the interval relaxes.
func (s *FeedState) OnNotModified(now time.Time) time.Duration {
	return s.OnSuccess(now, 0)
}

// intervalForRate maps an item rate onto the polling range.
func (s *FeedState) intervalForRate(rate float64) time.Duration {
	d := s.maxInterval
	if rate > 0 {
		d = time.Duration(60 / rate * float64(time.Second))
	}
	if d < s.floor {
		d = s.floor
	}
	if d > s.maxInterval {
		d = s.maxInterval
	}
	if d < MinFeedInterval && s.floor < MinFeedInterval {
		d = MinFeedInterval
	}
	return d
}

// OnFailure records a failed attempt. A parse failure counts towards
// quarantine; a network or HTTP failure only backs off. It returns the delay
// until the next attempt.
func (s *FeedState) OnFailure(now time.Time, parse bool) time.Duration {
	s.failures++
	if parse {
		s.parseFailures++
	}
	if s.parseFailures >= s.quarantineAfter {
		s.state = StateQuarantined
		s.quarantinedUntil = now.Add(s.quarantineFor)
		s.next = s.quarantinedUntil
		// Give the feed a fresh set of attempts after the cool-off.
		s.failures = 0
		s.parseFailures = 0
		return s.quarantineFor
	}
	s.state = StateBackoff
	delay := s.baseBackoff
	for i := 1; i < s.failures; i++ {
		delay *= 2
		if delay >= s.maxBackoff {
			delay = s.maxBackoff
			break
		}
	}
	s.next = now.Add(delay)
	return delay
}

// OnBlocked records a policy denial (robots.txt or a permanent HTTP error).
// The feed is quarantined for the cool-off without counting as a parse error.
func (s *FeedState) OnBlocked(now time.Time) time.Duration {
	s.state = StateQuarantined
	s.quarantinedUntil = now.Add(s.quarantineFor)
	s.next = s.quarantinedUntil
	return s.quarantineFor
}

// AdaptiveInterval is the pure instantaneous mapping used by tests and by
// callers that have not collected an EWMA: items per minute over elapsed,
// interval = 60s / rate, clamped to [floor, max(5m, floor)].
func AdaptiveInterval(newItems int, elapsed, floor time.Duration) time.Duration {
	if floor <= 0 {
		floor = MinFeedInterval
	}
	if elapsed < time.Second {
		elapsed = time.Second
	}
	state := NewFeedState(FeedStateOptions{Interval: floor})
	return state.intervalForRate(float64(newItems) / elapsed.Minutes())
}

package circuit

import (
	"errors"
	"sync"
	"time"
)

var ErrCircuitOpen = errors.New("circuit breaker OPEN — serving from cache")

type State int

const (
	StateClosed   State = iota 
	StateOpen                  
	StateHalfOpen              
)

type Breaker struct {
	mu           sync.Mutex
	state        State
	failures     int
	successes    int
	threshold    int           
	probeTimeout time.Duration 
	lastFailure  time.Time
}

func New(failureThreshold int, probeTimeout time.Duration) *Breaker {
	return &Breaker{
		state:        StateClosed,
		threshold:    failureThreshold,
		probeTimeout: probeTimeout,
	}
}

// Execute runs fn and updates breaker state based on outcome.
// When the circuit is OPEN, fn is NOT called and ErrCircuitOpen is returned.
func (b *Breaker) Execute(fn func() error) error {
	b.mu.Lock()
	defer b.mu.Unlock()

	switch b.state {
	case StateOpen:
		if time.Since(b.lastFailure) >= b.probeTimeout {
			b.state = StateHalfOpen
			b.successes = 0
		} else {
			return ErrCircuitOpen
		}
	case StateClosed, StateHalfOpen:
		// proceed
	}

	err := fn()
	if err != nil {
		b.failures++
		b.lastFailure = time.Now()
		if b.state == StateHalfOpen || b.failures >= b.threshold {
			b.state = StateOpen
			b.failures = 0
		}
		return err
	}

	// Success path
	if b.state == StateHalfOpen {
		b.successes++
		if b.successes >= 2 {
			b.state = StateClosed
			b.failures = 0
		}
	} else {
		b.failures = 0
	}
	return nil
}

// IsOpen returns true when the circuit is OPEN and calls are being dropped.
func (b *Breaker) IsOpen() bool {
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.state == StateOpen
}

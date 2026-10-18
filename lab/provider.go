package lab

import (
	"context"
	"errors"
	"fmt"
	"time"
)

var ErrHostNotFound = errors.New("host not found")

// SimulatedHost describes a health endpoint's response for this lab.
// A nil HealthError means the endpoint reports healthy after Delay.
type SimulatedHost struct {
	Delay       time.Duration
	HealthError error
}

// SimulatedProvider models health endpoints in memory, without real machines.
// Its configuration is fixed at construction, allowing concurrent health checks.
type SimulatedProvider struct {
	hosts map[string]SimulatedHost
}

func NewSimulatedProvider(hosts map[string]SimulatedHost) *SimulatedProvider {
	owned := make(map[string]SimulatedHost, len(hosts))
	for name, response := range hosts {
		owned[name] = response
	}
	return &SimulatedProvider{hosts: owned}
}

// CheckHealth simulates waiting for a host response. A canceled request stops
// waiting; it does not change the configured health of the simulated machine.
func (p *SimulatedProvider) CheckHealth(ctx context.Context, host string) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	response, found := p.hosts[host]
	if !found {
		return fmt.Errorf("%s: %w", host, ErrHostNotFound)
	}

	timer := time.NewTimer(response.Delay)
	defer timer.Stop()
	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-timer.C:
		if err := ctx.Err(); err != nil {
			return err
		}
		return response.HealthError
	}
}

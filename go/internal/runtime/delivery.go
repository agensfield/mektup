package runtime

import (
	"context"
	"fmt"

	"github.com/agensfield/mektup/go/internal/service"
)

// DeliveryAdapter maps one service delivery call to one atomic Codex
// turn/start request. The target is already resolved and remains immutable;
// retry policy belongs to service and therefore never calls the resolver.
type DeliveryAdapter struct {
	Pool *ConnectionPool
}

func (a *DeliveryAdapter) Send(ctx context.Context, target service.ResolvedTarget, text, messageID string) (service.DeliveryResult, error) {
	if a == nil || a.Pool == nil {
		return service.DeliveryResult{}, fmt.Errorf("runtime: delivery pool is required")
	}
	session, err := a.Pool.session(ctx, target)
	if err != nil {
		return service.DeliveryResult{}, err
	}
	turn, err := session.StartOrSteer(ctx, target.ThreadID, text, messageID)
	if err != nil {
		return service.DeliveryResult{}, err
	}
	return service.DeliveryResult{TurnID: turn.TurnID, Accepted: true, Evidence: turn.Evidence}, nil
}

func (a *DeliveryAdapter) Resume(ctx context.Context, target service.ResolvedTarget) (string, error) {
	if a == nil || a.Pool == nil {
		return "", fmt.Errorf("runtime: delivery pool is required")
	}
	_, err := a.Pool.subscribe(ctx, target)
	if err != nil {
		return "", err
	}
	return target.ThreadID, nil
}

func (a *DeliveryAdapter) Detach(ctx context.Context, target service.ResolvedTarget) error {
	if a == nil || a.Pool == nil {
		return fmt.Errorf("runtime: delivery pool is required")
	}
	// The endpoint session is pooled and may be serving another delivery or
	// observer. Release only this thread subscription lease. pool.close owns
	// the connection lifetime and is the only operation allowed to detach it.
	return a.Pool.unsubscribe(ctx, target)
}

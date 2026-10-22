package service

import (
	"context"
	"time"
)

// AdmissionContext also budgets CLI preparation. Its deadline is carried
// separately so completing admission does not cancel a later reply wait.
func AdmissionContext(parent context.Context, timeout time.Duration, disabled bool) (context.Context, context.CancelFunc) {
	return deliveryContext(parent, timeout, disabled)
}

func admissionContext(parent context.Context, timeout time.Duration, disabled bool, deadline time.Time) (context.Context, context.CancelFunc) {
	if !deadline.IsZero() {
		return context.WithDeadline(parent, deadline)
	}
	return deliveryContext(parent, timeout, disabled)
}

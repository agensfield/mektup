package messageexecutor

import (
	"context"

	"github.com/agensfield/mektup/go/internal/cli"
)

func setupError(ctx context.Context, err error) error {
	if ctx.Err() != nil {
		return &cli.Error{Code: "delivery_temporarily_unavailable", Message: "delivery admission ended during setup; body was not submitted", Effect: "not_sent", Exit: cli.ExitRejected, Details: map[string]any{"cause": ctx.Err().Error()}}
	}
	return err
}

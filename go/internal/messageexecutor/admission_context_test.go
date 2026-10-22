package messageexecutor

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/agensfield/mektup/go/internal/cli"
	"github.com/agensfield/mektup/go/internal/service"
)

func TestAdmissionBudgetIncludesCLICustodyPreparation(t *testing.T) {
	serviceCalls := 0
	e := New(Ports{
		PrepareCustody: func(ctx context.Context, _ cli.Invocation) error { <-ctx.Done(); return ctx.Err() },
		Service: func(context.Context, cli.Invocation) (MessagingService, error) {
			serviceCalls++
			return &fakeService{}, nil
		},
	})
	_, _, err := e.sendInvocation(context.Background(), cli.Invocation{Position: []string{"target", "body"}, Options: map[string][]string{"request-reply": {""}, "delivery-timeout": {"20ms"}}})
	var ce *cli.Error
	if !errors.As(err, &ce) || ce.Effect != "not_sent" || ce.Exit != cli.ExitRejected || serviceCalls != 0 {
		t.Fatalf("err=%v serviceCalls=%d", err, serviceCalls)
	}
}

func TestAdmissionBudgetIncludesCLIOriginalFactory(t *testing.T) {
	e := New(Ports{
		Service: func(context.Context, cli.Invocation) (MessagingService, error) { return &fakeService{}, nil },
		Original: func(ctx context.Context, _ cli.Invocation) (service.OriginalResolver, error) {
			<-ctx.Done()
			return nil, ctx.Err()
		},
	})
	_, _, _, err := e.replyInvocation(context.Background(), cli.Invocation{Position: []string{"original", "body"}, Options: map[string][]string{"delivery-timeout": {"20ms"}}})
	var ce *cli.Error
	if !errors.As(err, &ce) || ce.Effect != "not_sent" || ce.Exit != cli.ExitRejected {
		t.Fatalf("err=%v", err)
	}
}

func TestAdmissionDeadlineCarriedWithoutCancelingCaller(t *testing.T) {
	var observed time.Time
	e := New(Ports{Service: func(ctx context.Context, _ cli.Invocation) (MessagingService, error) {
		observed, _ = ctx.Deadline()
		return &fakeService{}, nil
	}})
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	_, request, err := e.sendInvocation(ctx, cli.Invocation{Position: []string{"target", "body"}, Options: map[string][]string{"delivery-timeout": {"1s"}}})
	if err != nil || observed.IsZero() || request.DeliveryDeadline != observed || ctx.Err() != nil {
		t.Fatalf("request=%+v err=%v caller=%v", request, err, ctx.Err())
	}
	_, request, err = e.sendInvocation(ctx, cli.Invocation{Position: []string{"target", "body"}, Options: map[string][]string{"delivery-timeout": {"0"}}})
	if err != nil || !request.DeliveryDeadline.IsZero() || ctx.Err() != nil {
		t.Fatalf("disabled deadline request=%+v err=%v", request, err)
	}
}

func TestAdmissionBudgetIncludesCLIServiceFactory(t *testing.T) {
	for _, reply := range []bool{false, true} {
		t.Run(map[bool]string{false: "send", true: "reply"}[reply], func(t *testing.T) {
			e := New(Ports{Service: func(ctx context.Context, _ cli.Invocation) (MessagingService, error) {
				<-ctx.Done()
				return nil, ctx.Err()
			}})
			inv := cli.Invocation{Position: []string{"reference", "body"}, Options: map[string][]string{"delivery-timeout": {"20ms"}}}
			var err error
			if reply {
				_, _, _, err = e.replyInvocation(context.Background(), inv)
			} else {
				_, _, err = e.sendInvocation(context.Background(), inv)
			}
			var ce *cli.Error
			if !errors.As(err, &ce) || ce.Code != "delivery_temporarily_unavailable" || ce.Effect != "not_sent" || ce.Exit != cli.ExitRejected {
				t.Fatalf("err=%v", err)
			}
		})
	}
}

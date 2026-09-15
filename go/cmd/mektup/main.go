package main

import (
	"context"
	"os"
	"os/signal"
	"syscall"

	"github.com/agensfield/mektup/go/internal/cli"
)

func main() {
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()
	app := cli.New()
	os.Exit(app.RunContext(ctx, os.Args[1:]))
}

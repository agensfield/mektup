package main

import (
	"context"
	"os"
	"os/signal"
	"syscall"

	"github.com/agensfield/mektup/go/internal/application"
	"github.com/agensfield/mektup/go/internal/cli"
)

func main() {
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()
	app := cli.New()
	environment := application.New(application.Options{Input: os.Stdin})
	app.Executor = environment
	code := app.RunContext(ctx, os.Args[1:])
	if err := environment.Close(); err != nil {
		_, _ = os.Stderr.WriteString("mektup: application cleanup failed: " + err.Error() + "\n")
		if code == int(cli.ExitSuccess) {
			code = int(cli.ExitInternal)
		}
	}
	os.Exit(code)
}

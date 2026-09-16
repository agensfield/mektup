package main

import (
	"context"
	"fmt"
	"io"
	"os"
	"os/signal"
	"syscall"

	"github.com/agensfield/mektup/go/internal/application"
	"github.com/agensfield/mektup/go/internal/cli"
	"github.com/agensfield/mektup/go/internal/controlreceiver"
)

func main() {
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()
	os.Exit(run(ctx, os.Args[1:], os.Stdin, os.Stdout, os.Stderr, controlreceiver.RunControlReceive))
}

type controlReceiveFunc func(context.Context, io.Reader, io.Writer, controlreceiver.CommandOptions) error

func run(ctx context.Context, args []string, input io.Reader, output, errorOutput io.Writer, receive controlReceiveFunc) int {
	// This is the only private control command. It is matched before public CLI
	// construction so the one-shot receiver cannot open an operation-selected
	// journal or artifact store. SSH supplies the fixed argv and the request
	// itself carries no path or executable authority.
	if len(args) == 2 && args[0] == "control" && args[1] == "receive" {
		if err := receive(ctx, input, output, controlreceiver.CommandOptions{}); err != nil {
			_, _ = fmt.Fprintln(errorOutput, "mektup: control receive failed:", err)
			return int(cli.ExitInternal)
		}
		return int(cli.ExitSuccess)
	}

	app := cli.New()
	app.In = input
	app.Out = output
	app.Err = errorOutput
	environment := application.New(application.Options{Input: input})
	app.Executor = environment
	code := app.RunContext(ctx, args)
	if err := environment.Close(); err != nil {
		_, _ = fmt.Fprintln(errorOutput, "mektup: application cleanup failed:", err)
		if code == int(cli.ExitSuccess) {
			code = int(cli.ExitInternal)
		}
	}
	return code
}

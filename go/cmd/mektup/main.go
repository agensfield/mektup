package main

import (
	"os"

	"github.com/agensfield/mektup/go/internal/cli"
)

func main() {
	app := cli.New()
	os.Exit(app.Run(os.Args[1:]))
}

package main

import (
	"os"

	"github.com/agensfield/mektup/go/internal/cli"
)

var (
	version     = "dev"
	commit      = "unknown"
	installKind = "source"
)

func main() {
	app := cli.New()
	app.Build = cli.DefaultBuildInfo
	app.Build.Version = version
	app.Build.Commit = commit
	app.Build.InstallKind = installKind
	os.Exit(app.Run(os.Args[1:]))
}

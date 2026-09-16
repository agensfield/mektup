package main

import (
	"fmt"
	"os"

	"github.com/agensfield/mektup/go/internal/conformance"
)

func main() {
	root, err := conformance.Root()
	if err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
	summary, err := conformance.Run(root)
	if err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
	if err := summary.WriteEvidence(os.Stdout); err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
}

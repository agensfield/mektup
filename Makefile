.PHONY: test test-race vet fmt-check contracts conformance notices govulncheck build

GOVULNCHECK_VERSION ?= v1.7.0

test:
	cd go && go test ./...

test-race:
	cd go && go test -race ./...

vet:
	cd go && go vet ./...

fmt-check:
	@test -z "$$(gofmt -l go)" || { \
		echo 'gofmt would change:'; \
		gofmt -l go; \
		exit 1; \
	}

contracts:
	./scripts/validate-contract-json.sh

conformance:
	cd go && go run ./cmd/mektup-conformance

notices:
	./scripts/generate-third-party-notices.sh

govulncheck:
	cd go && go run golang.org/x/vuln/cmd/govulncheck@$(GOVULNCHECK_VERSION) ./...

build:
	cd go && go build ./...

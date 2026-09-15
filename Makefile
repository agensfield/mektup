.PHONY: test test-race vet fmt-check contracts build

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

build:
	cd go && go build ./...

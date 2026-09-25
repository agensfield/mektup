FROM golang:1.25.13-bookworm

ARG CODEX_VERSION
ARG CODEX_ARCH
ARG CODEX_ASSET_URL
ARG CODEX_ASSET_SHA256

RUN apt-get update && apt-get install -y --no-install-recommends ca-certificates curl \
    && rm -rf /var/lib/apt/lists/*

RUN set -eu; \
    curl --fail --location --silent --show-error "$CODEX_ASSET_URL" -o /tmp/codex.tar.gz; \
    printf '%s  %s\n' "$CODEX_ASSET_SHA256" /tmp/codex.tar.gz | sha256sum --check --status; \
    tar -xzf /tmp/codex.tar.gz -C /tmp "codex-${CODEX_ARCH}-unknown-linux-musl"; \
    install -m 0755 "/tmp/codex-${CODEX_ARCH}-unknown-linux-musl" /usr/local/bin/codex; \
    test "$(codex --version)" = "codex-cli ${CODEX_VERSION}"; \
    rm /tmp/codex.tar.gz "/tmp/codex-${CODEX_ARCH}-unknown-linux-musl"

WORKDIR /work/go
COPY go/go.mod go/go.sum ./
RUN go mod download
COPY go/ ./
COPY contracts/ /work/contracts/
COPY conformance/ /work/conformance/
RUN useradd --create-home --uid 10001 qualifier \
    && chown -R qualifier:qualifier /go /work

ENV GOTOOLCHAIN=local
ENV HOME=/home/qualifier
USER qualifier

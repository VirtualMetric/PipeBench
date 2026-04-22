.PHONY: build build-containers build-generator build-receiver build-collector \
        test-local list clean tidy fmt vet

# Output binary
BINARY := bin/harness

# Version info (injected via ldflags)
VERSION  ?= $(shell git describe --tags --always --dirty 2>/dev/null || echo dev)
COMMIT   ?= $(shell git rev-parse --short HEAD 2>/dev/null || echo unknown)
BUILDDATE := $(shell date -u +%Y-%m-%dT%H:%M:%SZ)
LDFLAGS  := -s -w \
  -X main.buildVersion=$(VERSION) \
  -X main.buildCommit=$(COMMIT) \
  -X main.buildDate=$(BUILDDATE)

# Container image tags
GENERATOR_IMAGE := virtualmetric/bench-generator:latest
RECEIVER_IMAGE  := virtualmetric/bench-receiver:latest
COLLECTOR_IMAGE := virtualmetric/bench-collector:latest

# Default test / subject for test-local
TEST    ?= tcp_to_tcp_performance
SUBJECT ?= vector

# ── Harness CLI ───────────────────────────────────────────────────────────────

build:
	@mkdir -p bin
	go build -trimpath -ldflags='$(LDFLAGS)' -o $(BINARY) ./cmd/harness

# ── Container images ──────────────────────────────────────────────────────────

build-generator:
	docker build \
		-f containers/generator/Dockerfile \
		-t $(GENERATOR_IMAGE) \
		--build-arg BUILDKIT_INLINE_CACHE=1 \
		.

build-receiver:
	docker build \
		-f containers/receiver/Dockerfile \
		-t $(RECEIVER_IMAGE) \
		--build-arg BUILDKIT_INLINE_CACHE=1 \
		.

build-collector:
	docker build \
		-f containers/collector/Dockerfile \
		-t $(COLLECTOR_IMAGE) \
		--build-arg BUILDKIT_INLINE_CACHE=1 \
		.

build-containers: build-generator build-receiver build-collector

# ── End-to-end test ───────────────────────────────────────────────────────────

# Builds everything then runs a single test.
# Usage: make test-local TEST=tcp_to_tcp_performance SUBJECT=vector
test-local: build build-containers
	$(BINARY) test -t $(TEST) -s $(SUBJECT)

# ── Convenience wrappers ──────────────────────────────────────────────────────

list: build
	$(BINARY) list

clean:
	rm -f $(BINARY)
	$(BINARY) clean 2>/dev/null || true
	docker rmi -f $(GENERATOR_IMAGE) $(RECEIVER_IMAGE) $(COLLECTOR_IMAGE) 2>/dev/null || true

# ── Go tooling ────────────────────────────────────────────────────────────────

tidy:
	go mod tidy
	cd containers/generator && go mod tidy
	cd containers/receiver  && go mod tidy
	cd containers/collector && go mod tidy

fmt:
	gofmt -w -s .

vet:
	go vet ./...

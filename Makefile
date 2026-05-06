.PHONY: build build-containers build-generator build-receiver build-collector build-harness \
        push-containers push-generator push-receiver push-collector push-harness \
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
GENERATOR_IMAGE := vmetric/bench-generator:latest
RECEIVER_IMAGE  := vmetric/bench-receiver:latest
COLLECTOR_IMAGE := vmetric/bench-collector:latest
HARNESS_IMAGE   := vmetric/pipebench:latest

# Set ATTEST=1 to emit SBOM + max-mode provenance (used when publishing to Docker Hub).
# Requires the docker-container buildx driver; the default docker driver on GitHub
# Actions runners does not support attestations.
ATTEST_FLAGS := $(if $(ATTEST),--sbom=true --provenance=mode=max,)

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
		--platform linux/amd64 \
		$(ATTEST_FLAGS) \
		--build-arg BUILDKIT_INLINE_CACHE=1 \
		.

build-receiver:
	docker build \
		-f containers/receiver/Dockerfile \
		-t $(RECEIVER_IMAGE) \
		--platform linux/amd64 \
		$(ATTEST_FLAGS) \
		--build-arg BUILDKIT_INLINE_CACHE=1 \
		.

build-collector:
	docker build \
		-f containers/collector/Dockerfile \
		-t $(COLLECTOR_IMAGE) \
		--platform linux/amd64 \
		$(ATTEST_FLAGS) \
		--build-arg BUILDKIT_INLINE_CACHE=1 \
		.

# The harness image packages the orchestration CLI itself, with the
# docker CLI + compose plugin baked in so downstream consumers (e.g.
# virtualmetric-bench's vmetric-only regression tests) can run cases
# with `docker run vmetric/pipebench` and never compile any Go.
build-harness:
	docker build \
		-f containers/harness/Dockerfile \
		-t $(HARNESS_IMAGE) \
		--platform linux/amd64 \
		$(ATTEST_FLAGS) \
		--build-arg BUILDKIT_INLINE_CACHE=1 \
		--build-arg VERSION=$(VERSION) \
		--build-arg COMMIT=$(COMMIT) \
		--build-arg BUILDDATE=$(BUILDDATE) \
		.

build-containers: build-generator build-receiver build-collector build-harness

# ── Publish to Docker Hub ─────────────────────────────────────────────────────

# Tag with a sha- suffix in addition to :latest so we have a reproducible
# pinning target. The bench-* images already follow this convention; the
# harness image joins them.
push-generator: build-generator
	docker tag $(GENERATOR_IMAGE) vmetric/bench-generator:sha-$(COMMIT)
	docker push $(GENERATOR_IMAGE)
	docker push vmetric/bench-generator:sha-$(COMMIT)

push-receiver: build-receiver
	docker tag $(RECEIVER_IMAGE) vmetric/bench-receiver:sha-$(COMMIT)
	docker push $(RECEIVER_IMAGE)
	docker push vmetric/bench-receiver:sha-$(COMMIT)

push-collector: build-collector
	docker tag $(COLLECTOR_IMAGE) vmetric/bench-collector:sha-$(COMMIT)
	docker push $(COLLECTOR_IMAGE)
	docker push vmetric/bench-collector:sha-$(COMMIT)

push-harness: build-harness
	docker tag $(HARNESS_IMAGE) vmetric/pipebench:sha-$(COMMIT)
	docker push $(HARNESS_IMAGE)
	docker push vmetric/pipebench:sha-$(COMMIT)

push-containers: push-generator push-receiver push-collector push-harness

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
	docker rmi -f $(GENERATOR_IMAGE) $(RECEIVER_IMAGE) $(COLLECTOR_IMAGE) $(HARNESS_IMAGE) 2>/dev/null || true

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

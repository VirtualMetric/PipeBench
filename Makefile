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
#
# All image targets use `docker buildx build` (not plain `docker build`) so
# attestations (--sbom, --provenance) work in CI. Plain `docker build` uses
# the default `docker` driver, which doesn't support attestations; buildx
# uses the `docker-container` driver via the buildx builder created by
# `docker buildx create` (or `docker/setup-buildx-action` in CI).
#
# build-* targets use `--load` to import the result into the local docker
# daemon for `make test-local` to consume. This is incompatible with
# multi-arch + attestation manifests, so build-* targets do NOT enable
# ATTEST_FLAGS even when ATTEST=1 is set — local builds are always plain
# single-arch images. Attestations only land on the registry-pushed
# manifests via the push-* targets.
#
# push-* targets use `--push` to write directly to Docker Hub, bypassing
# the local daemon. This is the only path that can deliver attested images
# (the local daemon can't `--load` an attested manifest list, and
# `docker push` from a plain image strips attestations). Each push tags
# both `:latest` and `:sha-<commit>` in a single buildx invocation.

DOCKER_BUILD := docker buildx build

build-generator:
	$(DOCKER_BUILD) \
		-f containers/generator/Dockerfile \
		-t $(GENERATOR_IMAGE) \
		--platform linux/amd64 \
		--build-arg BUILDKIT_INLINE_CACHE=1 \
		--load \
		.

build-receiver:
	$(DOCKER_BUILD) \
		-f containers/receiver/Dockerfile \
		-t $(RECEIVER_IMAGE) \
		--platform linux/amd64 \
		--build-arg BUILDKIT_INLINE_CACHE=1 \
		--load \
		.

build-collector:
	$(DOCKER_BUILD) \
		-f containers/collector/Dockerfile \
		-t $(COLLECTOR_IMAGE) \
		--platform linux/amd64 \
		--build-arg BUILDKIT_INLINE_CACHE=1 \
		--load \
		.

# The harness image packages the orchestration CLI itself, with the
# docker CLI + compose plugin baked in so downstream consumers (e.g.
# virtualmetric-bench's vmetric-only regression tests) can run cases
# with `docker run vmetric/pipebench` and never compile any Go.
build-harness:
	$(DOCKER_BUILD) \
		-f containers/harness/Dockerfile \
		-t $(HARNESS_IMAGE) \
		--platform linux/amd64 \
		--build-arg BUILDKIT_INLINE_CACHE=1 \
		--build-arg VERSION=$(VERSION) \
		--build-arg COMMIT=$(COMMIT) \
		--build-arg BUILDDATE=$(BUILDDATE) \
		--load \
		.

build-containers: build-generator build-receiver build-collector build-harness

# ── Publish to Docker Hub ─────────────────────────────────────────────────────
#
# Each push-* target builds + pushes in a single buildx invocation. Both
# `:latest` and `:sha-<commit>` tags ride the same build (extra `-t` flags),
# so the manifests are byte-identical and we don't pay for the build twice.

push-generator:
	$(DOCKER_BUILD) \
		-f containers/generator/Dockerfile \
		-t $(GENERATOR_IMAGE) \
		-t vmetric/bench-generator:sha-$(COMMIT) \
		--platform linux/amd64 \
		$(ATTEST_FLAGS) \
		--build-arg BUILDKIT_INLINE_CACHE=1 \
		--push \
		.

push-receiver:
	$(DOCKER_BUILD) \
		-f containers/receiver/Dockerfile \
		-t $(RECEIVER_IMAGE) \
		-t vmetric/bench-receiver:sha-$(COMMIT) \
		--platform linux/amd64 \
		$(ATTEST_FLAGS) \
		--build-arg BUILDKIT_INLINE_CACHE=1 \
		--push \
		.

push-collector:
	$(DOCKER_BUILD) \
		-f containers/collector/Dockerfile \
		-t $(COLLECTOR_IMAGE) \
		-t vmetric/bench-collector:sha-$(COMMIT) \
		--platform linux/amd64 \
		$(ATTEST_FLAGS) \
		--build-arg BUILDKIT_INLINE_CACHE=1 \
		--push \
		.

push-harness:
	$(DOCKER_BUILD) \
		-f containers/harness/Dockerfile \
		-t $(HARNESS_IMAGE) \
		-t vmetric/pipebench:sha-$(COMMIT) \
		--platform linux/amd64 \
		$(ATTEST_FLAGS) \
		--build-arg BUILDKIT_INLINE_CACHE=1 \
		--build-arg VERSION=$(VERSION) \
		--build-arg COMMIT=$(COMMIT) \
		--build-arg BUILDDATE=$(BUILDDATE) \
		--push \
		.

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

FROM golang:1.26.5-alpine@sha256:0178a641fbb4858c5f1b48e34bdaabe0350a330a1b1149aabd498d0699ff5fb2 AS builder
WORKDIR /build
COPY containers/generator/go.mod containers/generator/go.sum* ./
RUN go mod download 2>/dev/null || true
# Copy every .go source in the generator package — main.go alone misses
# protocol-specific files like netflow.go and otel.go, so the build would
# fall back to the base modes (tcp/file/http) and any new mode would
# error at runtime with "unknown mode".
COPY containers/generator/*.go ./
RUN CGO_ENABLED=0 GOOS=linux go build -trimpath -ldflags="-s -w" -o /generator .

FROM alpine:3.22.5@sha256:14358309a308569c32bdc37e2e0e9694be33a9d99e68afb0f5ff33cc1f695dce
RUN apk upgrade --no-cache && apk add --no-cache ca-certificates && adduser -D -u 10001 app
COPY --from=builder /generator /generator
USER app
ENTRYPOINT ["/generator"]

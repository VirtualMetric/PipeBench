FROM golang:1.26.3-alpine@sha256:91eda9776261207ea25fd06b5b7fed8d397dd2c0a283e77f2ab6e91bfa71079d AS builder
WORKDIR /build
COPY containers/generator/go.mod containers/generator/go.sum* ./
RUN go mod download 2>/dev/null || true
# Copy every .go source in the generator package — main.go alone misses
# protocol-specific files like netflow.go and otel.go, so the build would
# fall back to the base modes (tcp/file/http) and any new mode would
# error at runtime with "unknown mode".
COPY containers/generator/*.go ./
RUN CGO_ENABLED=0 GOOS=linux go build -trimpath -ldflags="-s -w" -o /generator .

FROM alpine:3.22.4@sha256:310c62b5e7ca5b08167e4384c68db0fd2905dd9c7493756d356e893909057601
RUN apk add --no-cache ca-certificates && adduser -D -u 10001 app
COPY --from=builder /generator /generator
USER app
ENTRYPOINT ["/generator"]

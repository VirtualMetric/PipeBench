FROM golang:1.26.2-alpine@sha256:f85330846cde1e57ca9ec309382da3b8e6ae3ab943d2739500e08c86393a21b1 AS builder
WORKDIR /build
COPY containers/receiver/go.mod containers/receiver/go.sum* ./
RUN go mod download 2>/dev/null || true
# Copy every .go source — main.go alone misses protocol-specific files
# like otlp.go and otlp_grpc.go, so the build would silently exclude
# the OTLP code path.
COPY containers/receiver/*.go ./
RUN CGO_ENABLED=0 GOOS=linux go build -trimpath -ldflags="-s -w" -o /receiver .

FROM alpine:3.22.4@sha256:310c62b5e7ca5b08167e4384c68db0fd2905dd9c7493756d356e893909057601
RUN apk add --no-cache ca-certificates && adduser -D -u 10001 app
COPY --from=builder /receiver /receiver
USER app
ENTRYPOINT ["/receiver"]

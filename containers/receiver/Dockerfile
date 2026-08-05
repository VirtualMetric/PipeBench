FROM golang:1.26.5-alpine@sha256:0178a641fbb4858c5f1b48e34bdaabe0350a330a1b1149aabd498d0699ff5fb2 AS builder
WORKDIR /build
COPY containers/receiver/go.mod containers/receiver/go.sum* ./
RUN go mod download 2>/dev/null || true
# Copy every .go source — main.go alone misses protocol-specific files
# like otlp.go and otlp_grpc.go, so the build would silently exclude
# the OTLP code path.
COPY containers/receiver/*.go ./
RUN CGO_ENABLED=0 GOOS=linux go build -trimpath -ldflags="-s -w" -o /receiver .

FROM alpine:3.22.5@sha256:14358309a308569c32bdc37e2e0e9694be33a9d99e68afb0f5ff33cc1f695dce
RUN apk upgrade --no-cache && apk add --no-cache ca-certificates && adduser -D -u 10001 app
COPY --from=builder /receiver /receiver
USER app
ENTRYPOINT ["/receiver"]

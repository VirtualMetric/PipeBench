FROM golang:1.26.4-alpine@sha256:f1ddd9fe14fffc091dd98cb4bfa999f32c5fc77d2f2305ea9f0e2595c5437c14 AS builder
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

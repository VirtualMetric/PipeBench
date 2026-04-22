FROM golang:alpine AS builder
WORKDIR /build
COPY containers/collector/go.mod containers/collector/go.sum* ./
RUN go mod download 2>/dev/null || true
COPY containers/collector/*.go ./
RUN CGO_ENABLED=0 GOOS=linux go build -trimpath -ldflags="-s -w" -o /collector .

FROM alpine:3.21
RUN apk add --no-cache ca-certificates
COPY --from=builder /collector /collector
ENTRYPOINT ["/collector"]

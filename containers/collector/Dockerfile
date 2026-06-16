FROM golang:1.26.4-alpine@sha256:f1ddd9fe14fffc091dd98cb4bfa999f32c5fc77d2f2305ea9f0e2595c5437c14 AS builder
WORKDIR /build
COPY containers/collector/go.mod containers/collector/go.sum* ./
RUN go mod download 2>/dev/null || true
COPY containers/collector/*.go ./
RUN CGO_ENABLED=0 GOOS=linux go build -trimpath -ldflags="-s -w" -o /collector .

FROM alpine:3.22.4@sha256:310c62b5e7ca5b08167e4384c68db0fd2905dd9c7493756d356e893909057601
RUN apk upgrade --no-cache && apk add --no-cache ca-certificates && adduser -D -u 10001 app
COPY --from=builder /collector /collector
USER app
ENTRYPOINT ["/collector"]

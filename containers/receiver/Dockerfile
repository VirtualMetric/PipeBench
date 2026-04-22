FROM golang:1.25.6-alpine@sha256:98e6cffc31ccc44c7c15d83df1d69891efee8115a5bb7ede2bf30a38af3e3c92 AS builder
WORKDIR /build
COPY containers/receiver/go.mod containers/receiver/go.sum* ./
RUN go mod download 2>/dev/null || true
COPY containers/receiver/main.go .
RUN CGO_ENABLED=0 GOOS=linux go build -trimpath -ldflags="-s -w" -o /receiver .

FROM alpine:3.21.4@sha256:b6a6be0ff92ab6db8acd94f5d1b7a6c2f0f5d10ce3c24af348d333ac6da80685
RUN apk add --no-cache ca-certificates && adduser -D -u 10001 app
COPY --from=builder /receiver /receiver
USER app
ENTRYPOINT ["/receiver"]

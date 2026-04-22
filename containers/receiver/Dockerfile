FROM golang:alpine AS builder
WORKDIR /build
COPY containers/receiver/go.mod containers/receiver/go.sum* ./
RUN go mod download 2>/dev/null || true
COPY containers/receiver/main.go .
RUN CGO_ENABLED=0 GOOS=linux go build -trimpath -ldflags="-s -w" -o /receiver .

FROM alpine:3.21
RUN apk add --no-cache ca-certificates
COPY --from=builder /receiver /receiver
ENTRYPOINT ["/receiver"]

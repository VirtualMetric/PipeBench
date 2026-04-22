FROM golang:alpine AS builder
WORKDIR /build
# Copy only what's needed for the generator module
COPY containers/generator/go.mod containers/generator/go.sum* ./
RUN go mod download 2>/dev/null || true
COPY containers/generator/main.go .
RUN CGO_ENABLED=0 GOOS=linux go build -trimpath -ldflags="-s -w" -o /generator .

FROM alpine:3.21
RUN apk add --no-cache ca-certificates
COPY --from=builder /generator /generator
ENTRYPOINT ["/generator"]

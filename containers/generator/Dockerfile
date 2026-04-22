FROM golang:1.25-alpine@sha256:5caaf1cca9dc351e13deafbc3879fd4754801acba8653fa9540cea125d01a71f AS builder
WORKDIR /build
COPY containers/generator/go.mod containers/generator/go.sum* ./
RUN go mod download 2>/dev/null || true
COPY containers/generator/main.go .
RUN CGO_ENABLED=0 GOOS=linux go build -trimpath -ldflags="-s -w" -o /generator .

FROM alpine:3.21@sha256:48b0309ca019d89d40f670aa1bc06e426dc0931948452e8491e3d65087abc07d
RUN apk add --no-cache ca-certificates && adduser -D -u 10001 app
COPY --from=builder /generator /generator
USER app
ENTRYPOINT ["/generator"]

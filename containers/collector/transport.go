package main

import (
	"context"
	"net"
	"net/http"
)

// unixSocketTransport returns an HTTP transport that dials the Docker unix socket.
func unixSocketTransport(socketPath string) *http.Transport {
	return &http.Transport{
		DialContext: func(ctx context.Context, _, _ string) (net.Conn, error) {
			return (&net.Dialer{}).DialContext(ctx, "unix", socketPath)
		},
	}
}

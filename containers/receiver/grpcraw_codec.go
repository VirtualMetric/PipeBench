package main

// grpcraw_codec is a tiny copy of virtualmetric-backend/helper/grpcraw
// scoped to the bench receiver. We can't import the backend package
// from this module (the receiver has its own go.mod), but we still
// need to accept the "raw-otlp" content-subtype that vmetric's gRPC
// target advertises. So we register the same codec name here and
// rely on its proto.Message fallback path to deliver typed messages
// to the standard OTLP service handlers.
//
// Wire compatibility: the codec name controls the gRPC content-
// subtype header but not the wire bytes — those are still OTLP/proto.
// The standard server-side gRPC unmarshal path goes through the
// codec, so registering this codec lets typed handlers receive
// fully-decoded ExportXxxRequest messages without any client-side
// changes.

import (
	"fmt"

	"google.golang.org/grpc/encoding"
	"google.golang.org/protobuf/proto"
)

type rawOTLPCodec struct{}

func (rawOTLPCodec) Name() string { return "raw-otlp" }

func (rawOTLPCodec) Marshal(v any) ([]byte, error) {
	switch x := v.(type) {
	case *[]byte:
		if x == nil {
			return nil, nil
		}
		return *x, nil
	case []byte:
		return x, nil
	case proto.Message:
		return proto.Marshal(x)
	}
	return nil, fmt.Errorf("rawOTLPCodec: unsupported marshal type %T", v)
}

func (rawOTLPCodec) Unmarshal(data []byte, v any) error {
	switch x := v.(type) {
	case *[]byte:
		if x == nil {
			return fmt.Errorf("rawOTLPCodec: nil destination")
		}
		*x = append((*x)[:0], data...)
		return nil
	case proto.Message:
		return proto.Unmarshal(data, x)
	}
	return fmt.Errorf("rawOTLPCodec: unsupported unmarshal type %T", v)
}

func init() {
	// Register so the gRPC server's per-request codec lookup picks
	// it up by content-subtype. Stateless and concurrency-safe; the
	// init-time registration is the canonical way to wire a custom
	// codec into gRPC.
	encoding.RegisterCodec(rawOTLPCodec{})
}

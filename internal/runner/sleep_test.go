package runner

import (
	"context"
	"errors"
	"testing"
	"time"
)

func TestSleepCtxCancelled(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	start := time.Now()
	err := sleepCtx(ctx, 10*time.Second)
	if !errors.Is(err, context.Canceled) {
		t.Fatalf("want context.Canceled, got %v", err)
	}
	if elapsed := time.Since(start); elapsed > 100*time.Millisecond {
		t.Fatalf("pre-cancelled sleep took %s, want immediate return", elapsed)
	}
}

func TestSleepCtxCompletes(t *testing.T) {
	if err := sleepCtx(context.Background(), 10*time.Millisecond); err != nil {
		t.Fatalf("want nil, got %v", err)
	}
}

func TestSleepCtxCancelMidSleep(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	go func() {
		time.Sleep(20 * time.Millisecond)
		cancel()
	}()
	start := time.Now()
	err := sleepCtx(ctx, 10*time.Second)
	if !errors.Is(err, context.Canceled) {
		t.Fatalf("want context.Canceled, got %v", err)
	}
	if elapsed := time.Since(start); elapsed > time.Second {
		t.Fatalf("mid-sleep cancel took %s, want prompt return", elapsed)
	}
}

func TestSleepCtxNonPositive(t *testing.T) {
	if err := sleepCtx(context.Background(), 0); err != nil {
		t.Fatalf("want nil for d=0 on live ctx, got %v", err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	if err := sleepCtx(ctx, -time.Second); !errors.Is(err, context.Canceled) {
		t.Fatalf("want context.Canceled for negative d on cancelled ctx, got %v", err)
	}
}

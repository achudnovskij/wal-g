package postgres

import (
	"context"
	"testing"
	"time"

	"github.com/jackc/pglogrepl"
)

func TestContinuousDRTailInterval(t *testing.T) {
	cases := []struct {
		name string
		env  string
		want time.Duration
	}{
		{"default when empty", "", 2 * time.Second},
		{"explicit seconds", "5", 5 * time.Second},
		{"zero disables", "0", 0},
		{"invalid falls back to default", "nope", 2 * time.Second},
		{"negative falls back to default", "-3", 2 * time.Second},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			t.Setenv(ContinuousDRTailIntervalEnv, c.env)
			if got := continuousDRTailInterval(); got != c.want {
				t.Fatalf("continuousDRTailInterval()=%v want %v", got, c.want)
			}
		})
	}
}

// TestFlushDRTailDurableDisabled verifies the shared flush is a no-op (and never
// touches S3) when dr-tail S3 delivery is off — so the continuous flusher and the
// primary-loss flush both short-circuit cleanly on a sync-standby receiver that has
// no S3 backend configured.
func TestFlushDRTailDurableDisabled(t *testing.T) {
	t.Setenv(DRTailS3Env, "")
	n, durable, ok := flushDRTailDurable(context.Background(), "test")
	if ok || n != 0 || durable != 0 {
		t.Fatalf("flushDRTailDurable with dr-s3 disabled: n=%d durable=%s ok=%v want 0/0/false", n, durable, ok)
	}
}

// TestRunContinuousDRTailFlusherDisabled verifies the goroutine returns promptly
// (does not block on a ticker) when dr-tail S3 delivery is disabled.
func TestRunContinuousDRTailFlusherDisabled(t *testing.T) {
	t.Setenv(DRTailS3Env, "")
	done := make(chan struct{})
	go func() {
		runContinuousDRTailFlusher(context.Background())
		close(done)
	}()
	select {
	case <-done:
	case <-time.After(2 * time.Second):
		t.Fatal("runContinuousDRTailFlusher did not return when dr-s3 disabled")
	}
}

// TestRunContinuousDRTailFlusherZeroInterval verifies a 0 interval disables the
// flusher (returns immediately) even when dr-s3 is enabled.
func TestRunContinuousDRTailFlusherZeroInterval(t *testing.T) {
	t.Setenv(DRTailS3Env, "true")
	t.Setenv(ContinuousDRTailIntervalEnv, "0")
	done := make(chan struct{})
	go func() {
		runContinuousDRTailFlusher(context.Background())
		close(done)
	}()
	select {
	case <-done:
	case <-time.After(2 * time.Second):
		t.Fatal("runContinuousDRTailFlusher did not return with interval=0")
	}
}

// TestContiguousDurableGateNoUploadAboveHole proves the cross-segment contiguity
// fix: when a lower segment is MISSING from the retained set (a hole), the walk must
// NOT upload any segment above it (no "0B present, 0A missing" orphan), and the gate
// stays at the last contiguous segment below the hole. The missing segment is left
// to be retried on the next flush.
func TestContiguousDurableGateNoUploadAboveHole(t *testing.T) {
	tl := uint32(0x0E)
	toLSN := segLSN(0x0B) + pglogrepl.LSN(0x90) // in-flight gate inside 0x0B
	// Retained set: 06,07,08,09 then 0B (0A is MISSING — rotated away / failed prior PUT).
	segs := []segEntry{
		mkSeg(tl, 0x06), mkSeg(tl, 0x07), mkSeg(tl, 0x08), mkSeg(tl, 0x09), mkSeg(tl, 0x0B),
	}
	var uploaded []uint64
	put := func(p segEntry) (pglogrepl.LSN, bool) {
		uploaded = append(uploaded, p.segNo)
		return gateOf(toLSN)(p)
	}
	gate, err := contiguousDurableGate(context.Background(), segs, toLSN, put)
	if err != nil {
		t.Fatal(err)
	}
	want := segLSN(0x09) + pglogrepl.LSN(WalSegmentSize)
	if gate != want {
		t.Fatalf("gate=%s want end-of-0x09 %s (must not cross the 0x0A hole)", gate, want)
	}
	for _, s := range uploaded {
		if s == 0x0B {
			t.Fatalf("uploaded 0x0B ABOVE the missing 0x0A (orphan) — uploads=%v; must stop at the hole", uploaded)
		}
	}
	if len(uploaded) != 4 {
		t.Fatalf("uploaded=%v want exactly 06,07,08,09 (stop before the hole)", uploaded)
	}
}

// TestContiguousDurableGateNoUploadAboveFailedPut proves the failed-PUT variant: a
// PUT failure on a lower segment freezes the gate AND stops higher uploads, so the
// dr-tail never gains a segment above the failed (now-missing) one.
func TestContiguousDurableGateNoUploadAboveFailedPut(t *testing.T) {
	tl := uint32(0x0E)
	toLSN := segLSN(0x0B) + pglogrepl.LSN(0x90)
	segs := []segEntry{
		mkSeg(tl, 0x07), mkSeg(tl, 0x08), mkSeg(tl, 0x09), mkSeg(tl, 0x0A), mkSeg(tl, 0x0B),
	}
	var uploaded []uint64
	put := func(p segEntry) (pglogrepl.LSN, bool) {
		if p.segNo == 0x09 {
			return 0, false // PUT fails for 0x09
		}
		uploaded = append(uploaded, p.segNo)
		return gateOf(toLSN)(p)
	}
	gate, err := contiguousDurableGate(context.Background(), segs, toLSN, put)
	if err != nil {
		t.Fatal(err)
	}
	want := segLSN(0x08) + pglogrepl.LSN(WalSegmentSize)
	if gate != want {
		t.Fatalf("gate=%s want end-of-0x08 %s", gate, want)
	}
	for _, s := range uploaded {
		if s >= 0x0A {
			t.Fatalf("uploaded 0x%X above the failed 0x09 PUT — uploads=%v; must stop at the failure", s, uploaded)
		}
	}
	if len(uploaded) != 2 {
		t.Fatalf("uploaded=%v want exactly 07,08 (07,08 succeed, 09 fails, stop)", uploaded)
	}
}

// TestFlushDRTailDurableDedup verifies the de-dup short-circuit: once the watermark
// is at/above the current fsync frontier, the flush reports the watermark and does
// no upload. We drive this without S3 by pre-seeding the watermark above the (zero)
// test frontier with dr-s3 enabled.
func TestFlushDRTailDurableDedup(t *testing.T) {
	t.Setenv(DRTailS3Env, "true")
	lastDRTailFlushMu.Lock()
	saved := lastDRTailFlushLSN
	lastDRTailFlushLSN = pglogrepl.LSN(1 << 40) // far above any test frontier
	lastDRTailFlushMu.Unlock()
	t.Cleanup(func() {
		lastDRTailFlushMu.Lock()
		lastDRTailFlushLSN = saved
		lastDRTailFlushMu.Unlock()
	})
	n, durable, ok := flushDRTailDurable(context.Background(), "test")
	if !ok || n != 0 || durable != pglogrepl.LSN(1<<40) {
		t.Fatalf("flushDRTailDurable dedup: n=%d durable=%s ok=%v want 0/<watermark>/true", n, durable, ok)
	}
}

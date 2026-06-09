package postgres

import (
	"context"
	"fmt"
	"testing"

	"github.com/jackc/pglogrepl"
)

// segLSN returns the start LSN of segment segNo (16 MiB segments).
func segLSN(segNo uint64) pglogrepl.LSN {
	return pglogrepl.LSN(segNo * WalSegmentSize)
}

// segName builds a 24-char WAL filename on the given timeline + segment number.
func segName(timeline uint32, segNo uint64) string {
	return formatWALFileName(timeline, segNo)
}

func mkSeg(timeline uint32, segNo uint64) segEntry {
	return segEntry{
		path:     "/tmp/" + segName(timeline, segNo) + ".partial",
		segName:  segName(timeline, segNo),
		timeline: timeline,
		segNo:    segNo,
	}
}

// gateOf is a put func that always succeeds, reporting the segment end LSN —
// EXCEPT the in-flight segment (the one whose range contains toLSN) reports a
// floored end == toLSN (we use toLSN directly so the test math stays simple).
func gateOf(toLSN pglogrepl.LSN) func(segEntry) (pglogrepl.LSN, bool) {
	return func(p segEntry) (pglogrepl.LSN, bool) {
		if _, isInFlight := partialContainsLSN(p.segName, toLSN); isInFlight {
			return toLSN, true
		}
		return segLSN(p.segNo) + pglogrepl.LSN(WalSegmentSize), true
	}
}

func TestContiguousDurableGate(t *testing.T) {
	tl := uint32(0x0E)
	// gate sits inside segment 0x49 (so segments 0x41..0x49 are the tail).
	toLSN := segLSN(0x49) + pglogrepl.LSN(0x90)

	t.Run("full contiguous run reaches floored gate", func(t *testing.T) {
		var segs []segEntry
		for s := uint64(0x41); s <= 0x49; s++ {
			segs = append(segs, mkSeg(tl, s))
		}
		gate, err := contiguousDurableGate(context.Background(), segs, toLSN, gateOf(toLSN))
		if err != nil {
			t.Fatal(err)
		}
		if gate != toLSN {
			t.Fatalf("full run: gate=%s want floored frontier %s", gate, toLSN)
		}
	})

	t.Run("hole below gate freezes gate at last contiguous segment", func(t *testing.T) {
		// Segments 0x41,0x42,0x43 present, then 0x44..0x48 MISSING, then 0x49 (the
		// in-flight gate) present. The candidate cannot cross the 0x44 hole, so the
		// gate must be the END of segment 0x43, NOT the floored frontier in 0x49.
		segs := []segEntry{
			mkSeg(tl, 0x41), mkSeg(tl, 0x42), mkSeg(tl, 0x43), mkSeg(tl, 0x49),
		}
		gate, err := contiguousDurableGate(context.Background(), segs, toLSN, gateOf(toLSN))
		if err != nil {
			t.Fatal(err)
		}
		want := segLSN(0x43) + pglogrepl.LSN(WalSegmentSize)
		if gate != want {
			t.Fatalf("hole: gate=%s want end-of-0x43 %s (must not jump past the 0x44 hole to %s)", gate, want, toLSN)
		}
	})

	t.Run("failed PUT mid-run freezes gate at last successful segment", func(t *testing.T) {
		var segs []segEntry
		for s := uint64(0x41); s <= 0x49; s++ {
			segs = append(segs, mkSeg(tl, s))
		}
		// Fail the PUT for segment 0x45: only 0x41..0x44 are durable.
		put := func(p segEntry) (pglogrepl.LSN, bool) {
			if p.segNo == 0x45 {
				return 0, false
			}
			return gateOf(toLSN)(p)
		}
		gate, err := contiguousDurableGate(context.Background(), segs, toLSN, put)
		if err != nil {
			t.Fatal(err)
		}
		want := segLSN(0x44) + pglogrepl.LSN(WalSegmentSize)
		if gate != want {
			t.Fatalf("failed PUT: gate=%s want end-of-0x44 %s", gate, want)
		}
	})

	t.Run("single in-flight segment is its own complete run", func(t *testing.T) {
		segs := []segEntry{mkSeg(tl, 0x49)}
		gate, err := contiguousDurableGate(context.Background(), segs, toLSN, gateOf(toLSN))
		if err != nil {
			t.Fatal(err)
		}
		if gate != toLSN {
			t.Fatalf("single in-flight: gate=%s want %s", gate, toLSN)
		}
	})

	t.Run("lowest segment PUT fails => empty durable gate", func(t *testing.T) {
		segs := []segEntry{mkSeg(tl, 0x41), mkSeg(tl, 0x42)}
		put := func(p segEntry) (pglogrepl.LSN, bool) { return 0, false }
		gate, err := contiguousDurableGate(context.Background(), segs, toLSN, put)
		if err != nil {
			t.Fatal(err)
		}
		if gate != 0 {
			t.Fatalf("all fail: gate=%s want 0", gate)
		}
	})

	t.Run("gate never exceeds toLSN", func(t *testing.T) {
		// in-flight reports an end ABOVE toLSN (simulate a bad floor); must be clamped.
		segs := []segEntry{mkSeg(tl, 0x49)}
		put := func(p segEntry) (pglogrepl.LSN, bool) {
			return segLSN(0x49) + pglogrepl.LSN(WalSegmentSize), true // segment end > toLSN
		}
		gate, err := contiguousDurableGate(context.Background(), segs, toLSN, put)
		if err != nil {
			t.Fatal(err)
		}
		if gate != toLSN {
			t.Fatalf("clamp: gate=%s want %s", gate, toLSN)
		}
	})

	t.Run("ctx cancel returns prefix gate", func(t *testing.T) {
		ctx, cancel := context.WithCancel(context.Background())
		var segs []segEntry
		for s := uint64(0x41); s <= 0x49; s++ {
			segs = append(segs, mkSeg(tl, s))
		}
		uploaded := 0
		put := func(p segEntry) (pglogrepl.LSN, bool) {
			uploaded++
			if uploaded == 3 {
				cancel() // cancel after 0x43 lands; loop checks ctx next iteration
			}
			return gateOf(toLSN)(p)
		}
		gate, err := contiguousDurableGate(ctx, segs, toLSN, put)
		if err == nil {
			t.Fatal("expected ctx error")
		}
		want := segLSN(0x43) + pglogrepl.LSN(WalSegmentSize)
		if gate != want {
			t.Fatalf("ctx cancel: gate=%s want end-of-0x43 %s", gate, want)
		}
	})
}

func TestTimelineContaining(t *testing.T) {
	mk := func(tl uint32, segNo uint64) partialEntry {
		return partialEntry{segName: segName(tl, segNo)}
	}
	partials := []partialEntry{
		mk(2, 0x10), mk(2, 0x11), mk(3, 0x12), mk(3, 0x13),
		{segName: "not-a-wal-file"},
	}
	// LSN inside the timeline-3 segment 0x12.
	lsn := segLSN(0x12) + 0x100
	tl, ok := timelineContaining(partials, lsn)
	if !ok || tl != 3 {
		t.Fatalf("timelineContaining: tl=%d ok=%v want 3/true", tl, ok)
	}
	// LSN not covered by any retained segment.
	if _, ok := timelineContaining(partials, segLSN(0x99)); ok {
		t.Fatal("timelineContaining: expected ok=false for uncovered LSN")
	}
}

func TestSegmentsOnTimeline(t *testing.T) {
	mk := func(tl uint32, segNo uint64) partialEntry {
		return partialEntry{segName: segName(tl, segNo), path: fmt.Sprintf("/p/%s", segName(tl, segNo))}
	}
	partials := []partialEntry{
		mk(2, 0x10), mk(3, 0x12), mk(2, 0x11), mk(3, 0x13),
		{segName: "garbage"},
	}
	got := segmentsOnTimeline(partials, 3)
	if len(got) != 2 {
		t.Fatalf("segmentsOnTimeline(3): got %d want 2 (%+v)", len(got), got)
	}
	for _, s := range got {
		if s.timeline != 3 {
			t.Fatalf("segmentsOnTimeline returned wrong timeline: %+v", s)
		}
	}
}

func TestClampGate(t *testing.T) {
	if g := clampGate(100, 50); g != 50 {
		t.Fatalf("clampGate(100,50)=%d want 50", g)
	}
	if g := clampGate(40, 50); g != 40 {
		t.Fatalf("clampGate(40,50)=%d want 40", g)
	}
	if g := clampGate(0, 50); g != 0 {
		t.Fatalf("clampGate(0,50)=%d want 0", g)
	}
}

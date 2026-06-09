package postgres

import (
	"os"
	"path/filepath"
	"testing"
)

func TestShouldTriggerFallback(t *testing.T) {
	cases := []struct {
		name        string
		backlog     int64
		threshold   int64
		stallStreak int
		stallPolls  int
		want        bool
	}{
		{"under backlog, stalled", 100, 256, 5, 2, false},
		{"over backlog, not stalled enough", 300, 256, 1, 2, false},
		{"over backlog and stalled", 300, 256, 2, 2, true},
		{"exactly at threshold and stall", 256, 256, 2, 2, true},
		{"way over and very stalled", 1 << 30, 256, 10, 2, true},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			if got := shouldTriggerFallback(c.backlog, c.threshold, c.stallStreak, c.stallPolls); got != c.want {
				t.Fatalf("got %v, want %v", got, c.want)
			}
		})
	}
}

func TestNextStallStreak(t *testing.T) {
	// No backlog over threshold => streak resets to 0.
	if got := nextStallStreak(5, true, 10, 12, true, false); got != 0 {
		t.Fatalf("no backlog: got %d, want 0", got)
	}
	// Primary unreachable while backlog exists => increment.
	if got := nextStallStreak(1, true, 10, 0, false, true); got != 2 {
		t.Fatalf("unreachable: got %d, want 2", got)
	}
	// Archiver advanced => reset.
	if got := nextStallStreak(3, true, 10, 11, true, true); got != 0 {
		t.Fatalf("advanced: got %d, want 0", got)
	}
	// Archiver known but not advancing while backlog exists => increment.
	if got := nextStallStreak(2, true, 10, 10, true, true); got != 3 {
		t.Fatalf("not advancing: got %d, want 3", got)
	}
	// First observation (no prev), backlog over threshold => increment.
	if got := nextStallStreak(0, false, 0, 10, true, true); got != 1 {
		t.Fatalf("first observation: got %d, want 1", got)
	}
}

func TestBacklogAbove(t *testing.T) {
	segs := []retainedSeg{
		{walName: "a", segNo: 5, size: 100},
		{walName: "b", segNo: 6, size: 200},
		{walName: "c", segNo: 7, size: 300},
	}
	// floor known at 5 => only 6 and 7 count.
	bytes, gap := backlogAbove(segs, 5, true)
	if bytes != 500 || len(gap) != 2 || gap[0].segNo != 6 || gap[1].segNo != 7 {
		t.Fatalf("floor=5: bytes=%d gap=%v", bytes, gap)
	}
	// floor unknown => everything counts.
	bytes, gap = backlogAbove(segs, 0, false)
	if bytes != 600 || len(gap) != 3 {
		t.Fatalf("floor unknown: bytes=%d gap=%d", bytes, len(gap))
	}
	// floor above all => empty.
	bytes, gap = backlogAbove(segs, 99, true)
	if bytes != 0 || len(gap) != 0 {
		t.Fatalf("floor above all: bytes=%d gap=%d", bytes, len(gap))
	}
}

func TestRetainCompleteSegments(t *testing.T) {
	// Segment N occupies [N*WalSegmentSize, (N+1)*WalSegmentSize). A segment is
	// complete iff its END is at or below the fsync frontier.
	segs := []retainedSeg{
		{walName: "a", segNo: 5},
		{walName: "b", segNo: 6},
		{walName: "c", segNo: 7}, // in-flight in the cases below
	}
	// Frontier sits inside segment 7 (the in-flight one): 5 and 6 are complete, 7 excluded.
	frontier := 7*WalSegmentSize + 1234
	out := retainCompleteSegments(segs, frontier)
	if len(out) != 2 || out[0].segNo != 5 || out[1].segNo != 6 {
		t.Fatalf("inside seg 7: got %+v, want 5,6", out)
	}
	// Frontier exactly on the 6/7 boundary: 6 is complete (segEnd==frontier), 7 excluded.
	out = retainCompleteSegments(segs, 7*WalSegmentSize)
	if len(out) != 2 || out[1].segNo != 6 {
		t.Fatalf("on 6/7 boundary: got %+v, want 5,6", out)
	}
	// Frontier inside segment 5: nothing is complete (the very first retained
	// segment is still in flight) — must never upload a torn partial.
	out = retainCompleteSegments(segs, 5*WalSegmentSize+10)
	if len(out) != 0 {
		t.Fatalf("inside seg 5: got %+v, want none", out)
	}
	// Frontier past all segments: all complete.
	out = retainCompleteSegments(segs, 8*WalSegmentSize)
	if len(out) != 3 {
		t.Fatalf("past all: got %d, want 3", len(out))
	}
}

func TestSelfArchivedSet(t *testing.T) {
	s := &selfArchivedSet{m: make(map[uint64]struct{})}
	if s.has(7) {
		t.Fatal("empty set should not contain 7")
	}
	s.add(7)
	s.add(8)
	s.add(9)
	if !s.has(8) {
		t.Fatal("set should contain 8")
	}
	s.pruneAtOrBelow(8)
	if s.has(7) || s.has(8) {
		t.Fatal("7 and 8 should be pruned")
	}
	if !s.has(9) {
		t.Fatal("9 should remain")
	}
}

func TestListRetainedSegments(t *testing.T) {
	dir := t.TempDir()
	// Three retained completed segments (out of order on disk), one non-partial
	// file, and a subdir — only the three .partial WAL files should be listed,
	// sorted ascending by segment number.
	write := func(name string, size int) {
		if err := os.WriteFile(filepath.Join(dir, name), make([]byte, size), 0o600); err != nil {
			t.Fatal(err)
		}
	}
	write("000000010000000000000003.partial", 300)
	write("000000010000000000000001.partial", 100)
	write("000000010000000000000002.partial", 200)
	write("000000010000000000000002", 999) // completed-upload artifact, not .partial
	write("not-a-wal-name.partial", 10)    // unparseable, skipped
	if err := os.Mkdir(filepath.Join(dir, "sub"), 0o700); err != nil {
		t.Fatal(err)
	}

	segs, err := listRetainedSegments(dir)
	if err != nil {
		t.Fatal(err)
	}
	if len(segs) != 3 {
		t.Fatalf("got %d segments, want 3: %+v", len(segs), segs)
	}
	if !(segs[0].segNo < segs[1].segNo && segs[1].segNo < segs[2].segNo) {
		t.Fatalf("segments not sorted ascending: %+v", segs)
	}
	if segs[0].size != 100 || segs[1].size != 200 || segs[2].size != 300 {
		t.Fatalf("unexpected sizes: %+v", segs)
	}
	if segs[0].fileName != "000000010000000000000001.partial" {
		t.Fatalf("unexpected fileName: %q", segs[0].fileName)
	}

	// Missing dir => no error, no segments.
	segs, err = listRetainedSegments(filepath.Join(dir, "does-not-exist"))
	if err != nil || segs != nil {
		t.Fatalf("missing dir: segs=%v err=%v", segs, err)
	}
}

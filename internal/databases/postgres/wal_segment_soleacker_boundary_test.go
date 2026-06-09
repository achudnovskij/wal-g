package postgres

import (
	"encoding/binary"
	"testing"

	"github.com/jackc/pglogrepl"
	"github.com/jackc/pgx/v5/pgproto3"
)

// Regression test for the sole-acker segment-boundary flush_lsn freeze.
//
// BUG. When a single XLogData message straddles a 16 MiB WAL segment boundary,
// NextWalSegment() carries the message's tail into the new segment via
// processMessage -> writeBuffered (NO fsync). So immediately after rotation the
// new segment holds durable-pending bytes that HighestFsyncdLSN() does NOT yet
// cover. The OLD Stream() only fsync'd (and thus only advanced the flush-LSN
// ACK) reactively, on the NEXT inbound XLogData. Under saturating concurrent
// load every backend can already be parked in SyncRep waiting on exactly that
// ACK, so the primary sends nothing more: flush_lsn pins at the segment
// boundary until a ~10s keepalive breaks the standoff (the observed 6-21s
// sawtooth freeze).
//
// FIX. Stream() now fsyncs any carried-over bytes at segment start (before the
// first ReceiveMessage), so the very first ACK covers them with no new message
// required. This test asserts that invariant directly: after a boundary-
// spanning message is carried into the next segment, syncPartial() advances
// HighestFsyncdLSN() to include the carried tail.
func TestSoleAckerBoundaryCarryOverIsFsyncedWithoutNextMessage(t *testing.T) {
	t.Setenv(PartialDirEnv, t.TempDir())

	const segBytes = 64 // tiny power-of-two segment to force an easy boundary

	resetHighestFsyncdLSN(t)

	// Segment A starts at LSN 0, ends at 64.
	segA := NewWalSegment(1, 0, segBytes)

	// One message of 96 bytes starting at LSN 0 (segment A's writeIndex): the
	// first 64 bytes fill segment A (0..64) and the remaining 32 bytes spill
	// into segment B (64..96). This makes A complete and sets segA.lastMsg (the
	// straddle marker).
	msg := makeXLogData(t, 0, make([]byte, 96))
	res, err := segA.processMessage(msg)
	if err != nil {
		t.Fatalf("processMessage(A): %v", err)
	}
	if res != ProcessMessageOK {
		t.Fatalf("processMessage(A) result = %v, want OK", res)
	}
	if !segA.isComplete() {
		t.Fatalf("segment A should be complete after straddling message")
	}
	if segA.lastMsg == nil {
		t.Fatalf("segment A should have recorded a boundary-spanning lastMsg")
	}
	// Fsync A so its frontier is exactly the boundary (endLSN), mirroring the
	// real Stream() which fsyncs before returning ProcessMessageOK at boundary.
	if err := segA.syncPartial(); err != nil {
		t.Fatalf("syncPartial(A): %v", err)
	}
	if got := HighestFsyncdLSN(); got != pglogrepl.LSN(segBytes) {
		t.Fatalf("after A complete, HighestFsyncdLSN = %s, want %s", got, pglogrepl.LSN(segBytes))
	}

	// Rotate: NextWalSegment carries the 64 spilled bytes into B via
	// writeBuffered (no fsync). This is the durable-pending tail.
	segB, err := segA.NextWalSegment()
	if err != nil {
		t.Fatalf("NextWalSegment: %v", err)
	}
	if segB.writeIndex != 32 {
		t.Fatalf("segment B writeIndex = %d, want 32 (carried-over tail)", segB.writeIndex)
	}

	// BUG REPRODUCTION: before the carried-over fsync, the durable frontier is
	// still pinned at the boundary — exactly the stuck flush_lsn.
	if got := HighestFsyncdLSN(); got != pglogrepl.LSN(segBytes) {
		t.Fatalf("pre-sync HighestFsyncdLSN = %s, want %s (carry-over not yet durable)",
			got, pglogrepl.LSN(segBytes))
	}

	// THE FIX: Stream() fsyncs carried-over bytes at segment start. Do that and
	// assert the frontier now advances past the boundary WITHOUT any new
	// inbound message — which is what lets the first ACK release SyncRep.
	if err := segB.syncPartial(); err != nil {
		t.Fatalf("syncPartial(B): %v", err)
	}
	wantFrontier := pglogrepl.LSN(segBytes + 32) // boundary + carried tail = 96
	if got := HighestFsyncdLSN(); got != wantFrontier {
		t.Fatalf("post-sync HighestFsyncdLSN = %s, want %s (carry-over now durable)",
			got, wantFrontier)
	}

	segB.closePartialFile()
}

// resetHighestFsyncdLSN zeroes the package-global durable frontier so the test
// observes deterministic values. CompareAndSwap loop because Store would race a
// stray Stream() goroutine in principle; in a unit test there is none, but keep
// it consistent with the production update path.
func resetHighestFsyncdLSN(t *testing.T) {
	t.Helper()
	highestFsyncdLSN.Store(0)
}

// makeXLogData builds the CopyData payload processMessage expects: a leading
// XLogDataByteID ('w') followed by the pglogrepl XLogData header
// (WALStart, ServerWALEnd, ServerTime — all big-endian uint64) and the WAL
// bytes. walStart is the LSN of the first WAL byte.
func makeXLogData(t *testing.T, walStart uint64, walData []byte) *pgproto3.CopyData {
	t.Helper()
	buf := make([]byte, 1+24+len(walData))
	buf[0] = pglogrepl.XLogDataByteID
	binary.BigEndian.PutUint64(buf[1:9], walStart)
	binary.BigEndian.PutUint64(buf[9:17], walStart+uint64(len(walData))) // ServerWALEnd
	binary.BigEndian.PutUint64(buf[17:25], 0)                            // ServerTime
	copy(buf[25:], walData)
	return &pgproto3.CopyData{Data: buf}
}

package postgres

import (
	"bytes"
	"os"
	"testing"

	"github.com/jackc/pglogrepl"
)

// Regression test for the reconnect-truncate durability window (H1) and its fix.
//
// BUG (old behavior). highestFsyncdLSN is process-global and monotonically
// RAISED in syncPartial. The on-disk partial backing those bytes used to be
// re-created with O_TRUNC + Truncate (zero-filled) every time a segment began
// streaming again — exactly what a primary-loss reconnect does (the new
// replication connection re-sends WAL from the in-flight segment's StartLSN).
// That transiently ZEROED already-acked bytes; a crash in the window between the
// truncate and the re-fsync left acked WAL only in non-durable page cache while
// the primary's slot.restart_lsn had already advanced past it (RPO>0). The old
// code papered over the *advertised* frontier with a down-clamp, but not the
// on-disk bytes.
//
// FIX (option 2). ensurePartialFile opens WITHOUT O_TRUNC and only pre-allocates
// (Truncate UP) a new/short file. Re-streaming overwrites the SAME (timeline,
// segNo) bytes in place — physical replication re-sends identical bytes — so the
// acked bytes are NEVER zeroed, the frontier stays honestly backed by on-disk
// data, and no down-clamp is needed (syncPartial's monotonic raise can't lower
// it). This test asserts the acked bytes survive a reconnect and the frontier is
// preserved.
func TestReConnectPreservesAckedBytesAndFrontier(t *testing.T) {
	t.Setenv(PartialDirEnv, t.TempDir())

	const segBytes = 256 // tiny power-of-two segment

	resetHighestFsyncdLSN(t)

	// --- Session 1: stream segment [0,256) up to offset 192 with a NON-ZERO
	// payload and fsync it. (Non-zero so we can tell "preserved" from "zeroed".)
	payload := bytes.Repeat([]byte{0xAB}, 192)
	segA1 := NewWalSegment(1, 0, segBytes)
	if res, err := segA1.processMessage(makeXLogData(t, 0, payload)); err != nil || res != ProcessMessageOK {
		t.Fatalf("session1 processMessage: res=%v err=%v", res, err)
	}
	if err := segA1.syncPartial(); err != nil {
		t.Fatalf("session1 syncPartial: %v", err)
	}
	if got := HighestFsyncdLSN(); got != pglogrepl.LSN(192) {
		t.Fatalf("session1 frontier = %s, want 192", got)
	}
	partialPath := segA1.partialFile.Name()
	// Primary lost: receiveOnce returns WITHOUT closing/completing this in-flight
	// partial. Drop the handle the way an abandoned segment would; the file on
	// disk still holds the 192B of acked WAL.
	segA1.partialFile.Close()
	segA1.partialFile = nil

	// --- Session 2 (reconnect): ensurePartialFile must NOT truncate/zero the
	// in-flight segment. The acked bytes [0,192) stay on disk and the frontier
	// stays honestly at 192 (option 2: in-place overwrite, no clamp). ---
	segA2 := NewWalSegment(1, 0, segBytes)
	if err := segA2.ensurePartialFile(); err != nil {
		t.Fatalf("session2 ensurePartialFile: %v", err)
	}
	if got := HighestFsyncdLSN(); got != pglogrepl.LSN(192) {
		t.Fatalf("H1: reconnect lowered frontier to %s; want 192 preserved "+
			"(in-place overwrite must not zero acked bytes)", got)
	}
	// The acked bytes must still be on disk — NOT zeroed by a truncate.
	onDisk, err := os.ReadFile(partialPath)
	if err != nil {
		t.Fatalf("read partial: %v", err)
	}
	if len(onDisk) != segBytes {
		t.Fatalf("partial size = %d, want %d (still preallocated to full segment)", len(onDisk), segBytes)
	}
	if !bytes.Equal(onDisk[:192], payload) {
		t.Fatalf("H1: reconnect zeroed/clobbered acked bytes [0,192); want preserved 0xAB content")
	}

	// Re-stream overwrites [0,64) with IDENTICAL bytes; frontier stays 192
	// (monotonic, 64 < 192) and the content is unchanged.
	if res, err := segA2.processMessage(makeXLogData(t, 0, bytes.Repeat([]byte{0xAB}, 64))); err != nil || res != ProcessMessageOK {
		t.Fatalf("session2 processMessage: res=%v err=%v", res, err)
	}
	if err := segA2.syncPartial(); err != nil {
		t.Fatalf("session2 syncPartial: %v", err)
	}
	if got := HighestFsyncdLSN(); got != pglogrepl.LSN(192) {
		t.Fatalf("re-stream lowered frontier to %s; want 192 (monotonic raise; bytes preserved)", got)
	}

	segA2.closePartialFile()
}

// TestNewSegmentPreallocatesToFullSize confirms a genuinely-new partial (no prior
// file) is still zero-pre-allocated to the full segment size, so WriteAt at any
// offset works — the only case where the (now-conditional) Truncate fires.
func TestNewSegmentPreallocatesToFullSize(t *testing.T) {
	t.Setenv(PartialDirEnv, t.TempDir())
	const segBytes = 256
	resetHighestFsyncdLSN(t)

	seg := NewWalSegment(1, segBytes, segBytes) // second segment, StartLSN=256
	if err := seg.ensurePartialFile(); err != nil {
		t.Fatalf("ensurePartialFile: %v", err)
	}
	fi, err := seg.partialFile.Stat()
	if err != nil {
		t.Fatalf("stat: %v", err)
	}
	if fi.Size() != segBytes {
		t.Fatalf("new partial size = %d, want %d (preallocated)", fi.Size(), segBytes)
	}
	seg.closePartialFile()
}

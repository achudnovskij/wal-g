package postgres

import (
	"context"
	"fmt"
	"os"
	"path/filepath"
	"sort"
	"strconv"
	"strings"

	"github.com/jackc/pglogrepl"
	"github.com/wal-g/tracelog"
	"github.com/wal-g/wal-g/internal"
	"github.com/wal-g/wal-g/utility"
)

// S3 DR-tail delivery (alternative to the direct receiver->standby push).
//
// When WALG_WAL_RECEIVE_DR_S3 is set, the control API's /v1/dr-catchup uploads
// the receiver's retained tail (completed segments + the in-flight partial) to a
// dedicated DR-tail prefix in object storage instead of pushing it to the
// standby's wal-receive-serve. The promotion candidate then fetches it with the
// normal wal-g wal-fetch (a wal-g config rooted at <WALG_S3_PREFIX>/dr-tail), so
// the reliable restore path delivers the gap and no bespoke push channel is used.
//
// The DR-tail lane is kept strictly separate from the archive prefix: objects are
// written under their full segment names at <WALG_S3_PREFIX>/dr-tail/wal_<v>/, so
// an uploaded partial can never masquerade as (and poison) a complete segment in
// the real archive. The receiver is the only writer; writes are last-write-wins.
//
// See doc/walg-receiver-s3-dr-delivery.md. Direct push remains the default; this
// is flag-gated so the two can be compared.
const (
	// DRTailS3Env enables S3 DR-tail delivery on /v1/dr-catchup. Default off.
	DRTailS3Env = "WALG_WAL_RECEIVE_DR_S3"
	// drTailSubdir is the dedicated DR-tail prefix under WALG_S3_PREFIX.
	drTailSubdir = "dr-tail"
)

func drS3Enabled() bool {
	v, err := strconv.ParseBool(os.Getenv(DRTailS3Env))
	return err == nil && v
}

// uploadDRTailToS3 uploads the retained tail (completed segments + the in-flight
// partial) under each full segment name into <WALG_S3_PREFIX>/dr-tail/wal_<v>/,
// compressed/encrypted exactly as a normal archive object, last-write-wins.
// Returns the number of objects written AND the DURABLE gate LSN the control
// plane should wait for.
//
// DURABILITY INVARIANT (the RPO=0 contract). The returned gate must reflect ONLY
// WAL the promotion candidate can actually fetch+replay CONTIGUOUSLY from the
// dr-tail. The candidate runs `wal-fetch` per segment and replays in order; a
// SINGLE missing or un-fetchable segment in the chain stalls replay one segment
// short of the gate (the both-down / total-loss LOST_SEGMENTS wedge). Two ways
// the naive "report the floored frontier regardless" approach loses data:
//   - a PutObject FAILS mid-loop (S3 blip during the autonomous primary-loss
//     flush) — only a prefix of the tail lands, but the raw frontier is still
//     reported; the candidate then gates on an LSN whose backing segments were
//     never written (wal-fetch -> "Archive does not exist");
//   - the retained set is NON-CONTIGUOUS on the gate's timeline (a slot
//     fast-forward / timeline switch left a hole) — uploading every present
//     partial still leaves a gap the candidate cannot cross.
//
// So we upload the gate timeline's segments in ASCENDING order and report the end
// LSN of the LONGEST CONTIGUOUS run of SUCCESSFULLY-uploaded segments starting at
// the lowest retained segment on that timeline. The gate never claims past a hole
// or a failed PUT. If even the lowest segment fails, the gate stays at toLSN's
// floor only when that single in-flight segment is itself the whole run; callers
// must treat segments==0 / a regressed gate as "tail not durable" (the control
// plane keeps the strict block-and-page rather than promoting past lost WAL).
//
// We also STOP uploading at the first hole / failed PUT rather than continuing to
// upload higher segments across it: a segment uploaded ABOVE a missing lower one is
// an orphan the candidate can never reach (it stalls replaying at the hole), and it
// produces the misleading "0B present, 0A missing" dr-tail state. The missing lower
// segment is retried on the next flush; uploads resume past it only once it lands.
//
// toLSN is the requested gate (== the receiver's raw fsync'd frontier on the
// autonomous/failover paths). The in-flight partial carries valid WAL up to that
// raw byte frontier, and is shipped BYTE-FOR-BYTE (received bytes + the natural
// zero pad past writeIndex). wal-g does NOT parse the WAL to trim the torn tail to
// the last complete record: that frontier can sit INSIDE an incomplete record when
// the primary died mid-record, but the promotion candidate's own Postgres recovery
// stops at the last CRC-valid record natively and pg_promote() forks the timeline
// there, discarding the torn tail. See doc/walg-postgres-side-recovery-design.md.
// Completed retained segments are likewise uploaded byte-for-byte.
//
// The returned gate is the raw fsync frontier (toLSN), contiguity-capped: it never
// claims past a hole or a failed PUT, but it no longer floors to a record boundary
// — the CP gates the promote by ASSERTING replay reached this frontier, and replay
// (CRC-checked) lands on a complete-record boundary at or below it on its own.
func uploadDRTailToS3(ctx context.Context, toLSN pglogrepl.LSN) (int, pglogrepl.LSN, error) {
	allPartials, err := listPartials(walReceivePartialDir())
	if err != nil {
		return 0, toLSN, fmt.Errorf("list partials: %w", err)
	}
	if len(allPartials) == 0 {
		// Nothing retained: we have NOTHING durable to offer. Report a gate of 0 so
		// the caller (and CP) never gate a candidate on un-delivered WAL.
		return 0, 0, nil
	}

	// The gate lives on the in-flight segment's timeline. Restrict the durable run
	// to THAT timeline: a candidate promoting on the new timeline replays its own
	// timeline contiguously, and mixing old-timeline partials (left over from a
	// prior failover) into the contiguity walk would either fabricate a false gap
	// or let a stale-timeline segment masquerade as filling one.
	gateTimeline, haveGateTL := timelineContaining(allPartials, toLSN)
	if !haveGateTL {
		// The gate LSN is not covered by any retained segment — we cannot make it
		// durable. Report 0 (nothing to gate on) rather than an unreachable LSN.
		tracelog.WarningLogger.Printf("wal-receive: dr-tail upload: no retained segment contains gate %s; "+
			"reporting empty durable gate (tail not deliverable)", toLSN)
		return 0, 0, nil
	}
	partials := segmentsOnTimeline(allPartials, gateTimeline)
	sort.Slice(partials, func(i, j int) bool { return partials[i].segNo < partials[j].segNo })

	// DR-tail delivery requires a real S3 destination. In sync-standby mode the
	// receiver otherwise runs with ONLY local file storage (WALG_FILE_PREFIX), in
	// which case ConfigureUploader() yields a local-filesystem folder and this
	// "upload" would silently write into the pod's own disk, NOT S3 — the
	// candidate's wal-fetch would then find dr-tail empty and the failover would
	// wedge. Fail loudly instead of producing a hollow gate.
	// (See doc/walg-receiver-s3-dr-delivery.md — the receiver must be granted S3
	// write access to the tenant bucket for this path to work.)
	if os.Getenv("WALG_S3_PREFIX") == "" {
		return 0, toLSN, fmt.Errorf("dr-tail S3 delivery requires WALG_S3_PREFIX (+ S3 credentials) on the receiver, " +
			"but none is configured: the receiver has no object-storage backend (sync-standby/skip-upload mode " +
			"uses only local file storage). Grant the receiver S3 access to the tenant bucket")
	}
	// ConfigureUploader() returns an uploader at the storage ROOT (WALG_S3_PREFIX),
	// before any ChangeDirectory — so GetSubFolder gives <root>/dr-tail/wal_<v>/.
	up, err := internal.ConfigureUploader()
	if err != nil {
		return 0, toLSN, fmt.Errorf("configure uploader: %w", err)
	}
	folder := up.Folder().GetSubFolder(drTailSubdir).GetSubFolder(utility.WalPath)
	compressor, err := internal.ConfigureCompressor()
	if err != nil {
		return 0, toLSN, fmt.Errorf("configure compressor: %w", err)
	}
	crypter := internal.ConfigureCrypter()

	// putOne uploads a single retained segment BYTE-FOR-BYTE. Returns the
	// per-segment durable END lsn (segment end for a completed segment; the RAW
	// fsync frontier for the in-flight one) and whether the PUT succeeded.
	//
	// The in-flight partial is shipped UNTRIMMED — received bytes plus the natural
	// zero pad past writeIndex (the file is pre-truncated to WalSegmentSize). We do
	// NOT parse the WAL to floor the torn tail to the last complete record: Postgres
	// recovery on the promotion candidate stops at the last CRC-valid record
	// natively, and pg_promote() forks the timeline there, discarding the torn tail.
	// See doc/walg-postgres-side-recovery-design.md (get wal-g OUT of WAL parsing).
	// The durable end for the in-flight segment is therefore the raw fsync frontier
	// (toLSN); every byte beyond it in the object is a zero.
	putOne := func(p segEntry) (endLSN pglogrepl.LSN, ok bool) {
		objName := utility.SanitizePath(p.segName + "." + compressor.FileExtension())
		segStart := pglogrepl.LSN(p.segNo * WalSegmentSize)
		segEnd := segStart + pglogrepl.LSN(WalSegmentSize)
		_, isInFlight := partialContainsLSN(p.segName, toLSN)
		endLSN = segEnd
		if isInFlight {
			endLSN = toLSN
		}
		f, oerr := os.Open(p.path)
		if oerr != nil {
			tracelog.WarningLogger.Printf("wal-receive: dr-tail upload: open %s failed: %v", p.segName, oerr)
			return 0, false
		}
		reader := internal.CompressAndEncrypt(f, compressor, crypter)
		perr := folder.PutObjectWithContext(ctx, objName, reader)
		_ = f.Close()
		if perr != nil {
			tracelog.WarningLogger.Printf("wal-receive: dr-tail put %s failed: %v", p.segName, perr)
			return 0, false
		}
		return endLSN, true
	}

	n := 0
	durableGate, ctxErr := contiguousDurableGate(ctx, partials, toLSN, func(p segEntry) (pglogrepl.LSN, bool) {
		endLSN, ok := putOne(p)
		if ok {
			n++
		}
		return endLSN, ok
	})
	tracelog.InfoLogger.Printf("wal-receive: dr-tail S3 upload PUT %d object(s) to %s/%s/%s (timeline %08X, "+
		"raw frontier %s, durable+contiguous gate @ %s)",
		n, os.Getenv("WALG_S3_PREFIX"), drTailSubdir, utility.WalPath, gateTimeline, toLSN, durableGate)
	return n, durableGate, ctxErr
}

// contiguousDurableGate walks segs (ascending, single timeline) uploading each via
// put, and returns the END lsn of the longest CONTIGUOUS run of
// successfully-uploaded segments starting at the lowest segment — never claiming
// past a gap or a failed PUT, and never above toLSN. put returns the segment's
// durable end LSN (segment end, or the floored end for the in-flight segment) and
// whether the upload succeeded. Stops early on ctx cancellation, returning the
// prefix gate reached so far plus ctx.Err().
func contiguousDurableGate(ctx context.Context, segs []segEntry, toLSN pglogrepl.LSN,
	put func(segEntry) (pglogrepl.LSN, bool)) (pglogrepl.LSN, error) {
	durableGate := pglogrepl.LSN(0)
	prevSegNo := uint64(0)
	havePrev := false
	for _, p := range segs {
		select {
		case <-ctx.Done():
			return clampGate(durableGate, toLSN), ctx.Err()
		default:
		}
		// Contiguity gate: a hole in the retained set on this timeline ends the
		// durable run. We STOP here rather than upload across the hole — uploading a
		// segment ABOVE a missing lower one leaves an orphaned object the candidate
		// can never reach (it stalls at the hole), and it lets a later flush's
		// "present 0B, missing 0A" state masquerade as progress. The missing lower
		// segment is retried on the NEXT flush (its partial is still retained, or it
		// will be re-fsync'd on reconnect); only once it lands does the run — and the
		// uploads — advance past it. The gate never claims past the hole regardless.
		contiguous := !havePrev || p.segNo == prevSegNo+1
		if havePrev && !contiguous {
			break
		}
		endLSN, ok := put(p)
		if !ok {
			// A failed PUT breaks the durable run: every segment above it is now
			// unreachable for the candidate too, so stop — the gate stays at the last
			// successfully-uploaded contiguous segment. The failed segment is retried
			// on the next flush. We do NOT upload higher segments above this gap.
			break
		}
		_, isInFlight := partialContainsLSN(p.segName, toLSN)
		durableGate = endLSN
		if isInFlight {
			// The in-flight (gate) segment tops the contiguous run: the gate is at
			// the floored frontier and nothing above it matters.
			break
		}
		havePrev = true
		prevSegNo = p.segNo
	}
	return clampGate(durableGate, toLSN), nil
}

// clampGate caps a computed gate at the requested floored frontier (never report
// more than asked) while leaving 0 (nothing durable) intact.
func clampGate(gate, toLSN pglogrepl.LSN) pglogrepl.LSN {
	if gate > toLSN {
		return toLSN
	}
	return gate
}

// segEntry is a retained partial paired with its parsed timeline + segment number
// for contiguity reasoning.
type segEntry struct {
	path     string
	segName  string
	timeline uint32
	segNo    uint64
}

// timelineContaining returns the timeline of the retained segment whose LSN range
// contains lsn (the in-flight gate segment). ok=false when no retained segment
// covers it — meaning the tail we would report is not actually held.
func timelineContaining(partials []partialEntry, lsn pglogrepl.LSN) (uint32, bool) {
	for _, p := range partials {
		tl, segNo, err := ParseWALFilename(p.segName)
		if err != nil {
			continue
		}
		start := pglogrepl.LSN(segNo * WalSegmentSize)
		end := start + pglogrepl.LSN(WalSegmentSize)
		if lsn >= start && lsn < end {
			return tl, true
		}
	}
	return 0, false
}

// segmentsOnTimeline filters partials to a single timeline and parses their
// segment numbers, discarding any that don't parse.
func segmentsOnTimeline(partials []partialEntry, timeline uint32) []segEntry {
	var out []segEntry
	for _, p := range partials {
		tl, segNo, err := ParseWALFilename(p.segName)
		if err != nil || tl != timeline {
			continue
		}
		out = append(out, segEntry{path: p.path, segName: p.segName, timeline: tl, segNo: segNo})
	}
	return out
}

// partialContainsLSN reports whether the WAL segment named segName is the one
// whose LSN range [segStart, segStart+WalSegmentSize) contains lsn, returning the
// segment's first LSN. The in-flight partial is exactly this segment.
func partialContainsLSN(segName string, lsn pglogrepl.LSN) (segStart pglogrepl.LSN, ok bool) {
	no, err := newWalSegmentNoFromFilename(segName)
	if err != nil {
		return 0, false
	}
	start := uint64(no) * WalSegmentSize
	end := start + WalSegmentSize
	l := uint64(lsn)
	if l >= start && l < end {
		return pglogrepl.LSN(start), true
	}
	return 0, false
}

type partialEntry struct {
	path    string // absolute path on local disk
	segName string // 24-char bare WAL filename, no .partial suffix
}

func listPartials(dir string) ([]partialEntry, error) {
	entries, err := os.ReadDir(dir)
	if os.IsNotExist(err) {
		return nil, nil
	}
	if err != nil {
		return nil, err
	}
	var out []partialEntry
	for _, e := range entries {
		if e.IsDir() {
			continue
		}
		name := e.Name()
		if !strings.HasSuffix(name, ".partial") {
			continue
		}
		seg := strings.TrimSuffix(name, ".partial")
		if _, _, err := ParseWALFilename(seg); err != nil {
			continue // not a WAL file, skip silently
		}
		out = append(out, partialEntry{
			path:    filepath.Join(dir, name),
			segName: seg,
		})
	}
	return out, nil
}

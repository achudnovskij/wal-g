package postgres

import (
	"context"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"sort"
	"strconv"
	"sync"
	"time"

	"github.com/wal-g/tracelog"
	"github.com/wal-g/wal-g/internal"
	"github.com/wal-g/wal-g/pkg/storages/storage"
	"github.com/wal-g/wal-g/utility"
)

// Autonomous fallback WAL archival.
//
// In sync-standby mode (WALG_WAL_RECEIVE_SKIP_UPLOAD) the receiver normally only
// *retains* completed segments and lets the primary's archive_command be the
// sole uploader to object storage. This module adds an autonomous fallback: when
// the receiver's un-archived backlog grows past a threshold AND the primary's
// archiver looks stalled, the receiver uploads the missing completed segments
// itself, in order, using a conditional (create-if-absent) PUT so it never
// double-uploads what the primary already placed and never disrupts the primary.
//
// No control plane is involved — the decision is made locally from the on-disk
// backlog size and pg_stat_archiver.last_archived_wal.
const (
	// FallbackUploadEnv enables autonomous fallback archival. Default off.
	FallbackUploadEnv = "WALG_WAL_RECEIVE_FALLBACK_UPLOAD"
	// FallbackBacklogBytesEnv: upload only once the un-archived retained backlog
	// exceeds this many bytes (i.e. the primary is behind enough to matter).
	FallbackBacklogBytesEnv = "WALG_WAL_RECEIVE_FALLBACK_BACKLOG_BYTES"
	// FallbackPollIntervalEnv: how often to evaluate the trigger (seconds).
	FallbackPollIntervalEnv = "WALG_WAL_RECEIVE_FALLBACK_POLL_INTERVAL_SECONDS"
	// FallbackStallPollsEnv: consecutive polls the archiver must look stalled
	// (last_archived_wal not advancing while a backlog exists, or the primary
	// unreachable) before the fallback fires. Avoids racing a primary that is
	// merely catching up.
	FallbackStallPollsEnv = "WALG_WAL_RECEIVE_FALLBACK_STALL_POLLS"
)

const (
	defaultFallbackBacklogBytes int64 = 256 * 1024 * 1024 // 256 MiB
	defaultFallbackPollInterval       = 15 * time.Second
	defaultFallbackStallPolls         = 2
)

func fallbackUploadEnabled() bool {
	v := os.Getenv(FallbackUploadEnv)
	if v == "" {
		// Default ON in sync-standby (skip-upload) mode. There the receiver only
		// RETAINS completed segments locally and the primary is the sole archiver,
		// so when the primary falls behind or dies the receiver must take over
		// archiving the completed segments to the MAIN archive itself. This is the
		// reliable standard wal-upload path (per-segment conditional create-if-absent
		// PUT) — NOT the bespoke dr-tail — gated on the behind-detection below; the
		// trailing partial is delivered separately/raw via the dr-tail flush. Set
		// the env explicitly to false to disable.
		return skipUpload()
	}
	b, err := strconv.ParseBool(v)
	return err == nil && b
}

func fallbackBacklogBytes() int64 {
	v := os.Getenv(FallbackBacklogBytesEnv)
	if v == "" {
		return defaultFallbackBacklogBytes
	}
	n, err := strconv.ParseInt(v, 10, 64)
	if err != nil || n < 0 {
		tracelog.WarningLogger.Printf("%s=%q invalid, using %d", FallbackBacklogBytesEnv, v, defaultFallbackBacklogBytes)
		return defaultFallbackBacklogBytes
	}
	return n
}

func fallbackPollInterval() time.Duration {
	v := os.Getenv(FallbackPollIntervalEnv)
	if v == "" {
		return defaultFallbackPollInterval
	}
	n, err := strconv.Atoi(v)
	if err != nil || n < 0 {
		tracelog.WarningLogger.Printf("%s=%q invalid, using %v", FallbackPollIntervalEnv, v, defaultFallbackPollInterval)
		return defaultFallbackPollInterval
	}
	return time.Duration(n) * time.Second
}

func fallbackStallPolls() int {
	v := os.Getenv(FallbackStallPollsEnv)
	if v == "" {
		return defaultFallbackStallPolls
	}
	n, err := strconv.Atoi(v)
	if err != nil || n < 1 {
		tracelog.WarningLogger.Printf("%s=%q invalid, using %d", FallbackStallPollsEnv, v, defaultFallbackStallPolls)
		return defaultFallbackStallPolls
	}
	return n
}

// retainedSeg is one completed segment held on local disk as <walName>.partial.
type retainedSeg struct {
	walName  string // 24-char WAL filename (no .partial)
	fileName string // on-disk name, i.e. walName + ".partial"
	segNo    uint64
	size     int64
}

// listRetainedSegments returns the completed segments retained in dir, sorted
// ascending by segment number.
func listRetainedSegments(dir string) ([]retainedSeg, error) {
	entries, err := os.ReadDir(dir)
	if os.IsNotExist(err) {
		return nil, nil
	}
	if err != nil {
		return nil, fmt.Errorf("read partial dir: %w", err)
	}
	var segs []retainedSeg
	for _, e := range entries {
		name := e.Name()
		if e.IsDir() || filepath.Ext(name) != ".partial" {
			continue
		}
		walName := name[:len(name)-len(".partial")]
		_, segNo, perr := ParseWALFilename(walName)
		if perr != nil {
			continue
		}
		var size int64
		if fi, serr := e.Info(); serr == nil {
			size = fi.Size()
		}
		segs = append(segs, retainedSeg{walName: walName, fileName: name, segNo: segNo, size: size})
	}
	sort.Slice(segs, func(i, j int) bool { return segs[i].segNo < segs[j].segNo })
	return segs, nil
}

// backlogAbove returns the total size and the (already-sorted) subset of segs
// with segNo strictly greater than floorSegNo — the segments the fallback would
// upload because the primary has not archived them.
func backlogAbove(segs []retainedSeg, floorSegNo uint64, floorKnown bool) (int64, []retainedSeg) {
	var bytes int64
	var gap []retainedSeg
	for _, s := range segs {
		if floorKnown && s.segNo <= floorSegNo {
			continue
		}
		bytes += s.size
		gap = append(gap, s)
	}
	return bytes, gap
}

// shouldTriggerFallback reports whether the autonomous fallback should fire: the
// un-archived backlog is past the threshold AND the primary archiver has looked
// stalled for enough consecutive polls.
func shouldTriggerFallback(backlogBytes, thresholdBytes int64, stallStreak, stallPolls int) bool {
	return backlogBytes >= thresholdBytes && stallStreak >= stallPolls
}

// nextStallStreak advances the archiver-stall counter. The archiver is "stalled"
// for a poll when there is a meaningful backlog AND either the primary is
// unreachable (archivedKnown=false) or last_archived_wal did not advance since
// the previous poll. A clear advance of last_archived_wal resets the streak.
func nextStallStreak(prev int, havePrevArchived bool, prevArchived, archivedSegNo uint64,
	archivedKnown, backlogOverThreshold bool) int {
	if !backlogOverThreshold {
		return 0
	}
	if !archivedKnown {
		return prev + 1 // can't reach the primary: it is not archiving
	}
	if havePrevArchived && archivedSegNo > prevArchived {
		return 0 // archiver advanced: healthy
	}
	return prev + 1
}

// fallbackUploader runs the autonomous fallback poller for the process lifetime.
type fallbackUploader struct {
	uploader     *WalUploader
	dir          string
	backlogBytes int64
	stallPolls   int

	prevArchived     uint64
	havePrevArchived bool
	stallStreak      int

	conditionalUnsupported bool
}

// runFallbackUploadPoller starts the autonomous fallback archival loop. It is a
// no-op (returns immediately) unless WALG_WAL_RECEIVE_FALLBACK_UPLOAD is set.
func runFallbackUploadPoller(ctx context.Context, uploader *WalUploader) {
	if !fallbackUploadEnabled() {
		return
	}
	interval := fallbackPollInterval()
	if interval == 0 {
		return
	}
	fu := &fallbackUploader{
		uploader:     uploader,
		dir:          walReceivePartialDir(),
		backlogBytes: fallbackBacklogBytes(),
		stallPolls:   fallbackStallPolls(),
	}
	tracelog.InfoLogger.Printf("wal-receive: autonomous fallback archival enabled (backlog>%d bytes, stall>=%d polls, every %v)",
		fu.backlogBytes, fu.stallPolls, interval)

	t := time.NewTicker(interval)
	defer t.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-t.C:
		}
		if err := fu.tick(ctx); err != nil {
			tracelog.WarningLogger.Printf("wal-receive: fallback archival tick failed: %v", err)
		}
	}
}

// tick evaluates the trigger once and, if it fires, uploads the gap in order.
func (fu *fallbackUploader) tick(ctx context.Context) error {
	segs, err := listRetainedSegments(fu.dir)
	if err != nil {
		return err
	}
	// Only fully-fsync'd COMPLETE segments may go to the MAIN archive. Exclude the
	// in-flight segment (and any not-yet-fully-received one): the receiver stores
	// every segment — including the one currently being written — as <name>.partial
	// with no rename-on-complete, so listRetainedSegments returns the torn in-flight
	// segment too. Uploading it under its full segment name via a create-if-absent
	// PUT could poison the archive (the incomplete copy landing before the primary
	// archives the real complete segment → recovery reads a short/zero-tail segment
	// mid-stream). The in-flight partial's tail is delivered raw via the dr-tail
	// flush below, where Postgres recovery trims it natively.
	segs = retainCompleteSegments(segs, uint64(HighestFsyncdLSN()))
	if len(segs) == 0 {
		fu.stallStreak = 0
		return nil
	}

	archivedSegNo, archivedKnown := fu.queryArchivedSegNo()
	floorSegNo, floorKnown := fu.uploadFloor(archivedSegNo, archivedKnown)
	backlog, gap := backlogAbove(segs, floorSegNo, floorKnown)
	overThreshold := backlog >= fu.backlogBytes

	fu.stallStreak = nextStallStreak(fu.stallStreak, fu.havePrevArchived, fu.prevArchived,
		archivedSegNo, archivedKnown, overThreshold)
	if archivedKnown {
		fu.prevArchived = archivedSegNo
		fu.havePrevArchived = true
	}

	if !shouldTriggerFallback(backlog, fu.backlogBytes, fu.stallStreak, fu.stallPolls) {
		return nil
	}

	tracelog.WarningLogger.Printf("wal-receive: fallback archival firing — backlog=%d bytes (%d segment(s)) above floor, "+
		"archiver stalled %d poll(s); uploading", backlog, len(gap), fu.stallStreak)
	uploaded, skipped, err := fu.uploadSegments(ctx, gap)
	tracelog.InfoLogger.Printf("wal-receive: fallback archival uploaded %d, skipped %d already-archived segment(s)",
		uploaded, skipped)
	// The primary is behind: besides archiving the COMPLETED segments above, make
	// the in-flight TAIL durable now too (raw, in the dr-tail prefix) so a failover
	// or total loss immediately after this tick can recover the most-recently-acked
	// WAL — not just the last completed segment. No-op when WALG_WAL_RECEIVE_DR_S3
	// is off; de-duplicated against the continuous flusher's watermark.
	flushDRTailDurable(ctx, "fallback (primary behind)")
	return err
}

// retainCompleteSegments keeps only segments whose bytes are ALL durably fsync'd
// (segEnd <= frontier), dropping the in-flight segment currently being written and
// any not-yet-fully-received one. frontier is HighestFsyncdLSN() in bytes. This is
// the guard that keeps a torn partial out of the MAIN archive (see tick).
func retainCompleteSegments(segs []retainedSeg, frontier uint64) []retainedSeg {
	var out []retainedSeg
	for _, s := range segs {
		if (s.segNo+1)*WalSegmentSize <= frontier {
			out = append(out, s)
		}
	}
	return out
}

// queryArchivedSegNo returns the primary's last_archived_wal segment number.
// ok=false when the primary is unreachable or has never archived — both are
// treated by the caller as "archiver not making progress".
func (fu *fallbackUploader) queryArchivedSegNo() (segNo uint64, ok bool) {
	conn, err := ConnectWithTimeout(10 * time.Second)
	if err != nil {
		return 0, false
	}
	defer conn.Close(context.Background())
	qr, err := NewPgQueryRunner(conn)
	if err != nil {
		return 0, false
	}
	name, archivedOK, err := qr.LastArchivedWALFilename()
	if err != nil || !archivedOK {
		return 0, false
	}
	_, segNo, err = ParseWALFilename(name)
	if err != nil {
		return 0, false
	}
	return segNo, true
}

// uploadFloor decides the segment number below/at which we must NOT upload.
// When the primary's archived position is known, that is the floor. When it is
// unknown (primary down), fall back to the last position we did know, or 0 —
// uploading everything retained, which the conditional PUT dedups against
// whatever the primary already managed to archive.
func (fu *fallbackUploader) uploadFloor(archivedSegNo uint64, archivedKnown bool) (uint64, bool) {
	if archivedKnown {
		return archivedSegNo, true
	}
	if fu.havePrevArchived {
		return fu.prevArchived, true
	}
	return 0, false
}

// uploadSegments uploads each gap segment in order, stopping at the first hard
// error (segments must be archived contiguously). Returns counts of freshly
// uploaded vs already-present (someone else won the race) segments.
func (fu *fallbackUploader) uploadSegments(ctx context.Context, gap []retainedSeg) (uploaded, skipped int, err error) {
	for _, s := range gap {
		select {
		case <-ctx.Done():
			return uploaded, skipped, ctx.Err()
		default:
		}
		alreadyPresent, uerr := fu.uploadOne(ctx, s)
		if uerr != nil {
			return uploaded, skipped, fmt.Errorf("upload %s: %w", s.walName, uerr)
		}
		// Present-or-uploaded both mean the segment is now durable in S3, so the
		// janitor may free our local copy without waiting for the primary's
		// pg_stat_archiver to catch up.
		selfArchived.add(s.segNo)
		if alreadyPresent {
			skipped++
		} else {
			uploaded++
		}
	}
	return uploaded, skipped, nil
}

// uploadOne compresses+encrypts a single retained segment exactly as the primary
// would and writes it conditionally (create-if-absent). A 412/already-exists is
// reported as alreadyPresent=true (not an error). If the backend can't do a
// conditional write it falls back to a plain last-write-wins PUT — safe because
// the segment bytes are deterministic.
func (fu *fallbackUploader) uploadOne(ctx context.Context, s retainedSeg) (alreadyPresent bool, err error) {
	compressor := fu.uploader.Compression()
	objName := utility.SanitizePath(s.walName + "." + compressor.FileExtension())
	folder := fu.uploader.Folder()
	srcPath := filepath.Join(fu.dir, s.fileName)

	if cond, ok := folder.(storage.ConditionalPutObjectFolder); ok && !fu.conditionalUnsupported {
		f, oerr := os.Open(srcPath)
		if oerr != nil {
			return false, oerr
		}
		reader := internal.CompressAndEncrypt(f, compressor, internal.ConfigureCrypter())
		perr := cond.PutObjectIfAbsent(ctx, objName, reader)
		_ = f.Close()
		switch {
		case perr == nil:
			return false, nil
		case errors.Is(perr, storage.ErrObjectExists):
			return true, nil
		case errors.Is(perr, storage.ErrConditionalPutUnsupported):
			// Drain the unread compression pipe so its goroutine doesn't leak,
			// then remember to use plain PUT for the rest of this run.
			if c, ok := reader.(interface{ Close() error }); ok {
				_ = c.Close()
			}
			fu.conditionalUnsupported = true
			tracelog.WarningLogger.Printf("wal-receive: backend has no conditional PUT; fallback archival will use plain (last-write-wins) PUT")
		default:
			return false, perr
		}
	}

	// Plain PUT fallback (last-write-wins). Identical compressed bytes make a
	// redundant overwrite harmless.
	f, oerr := os.Open(srcPath)
	if oerr != nil {
		return false, oerr
	}
	defer f.Close()
	reader := internal.CompressAndEncrypt(f, compressor, internal.ConfigureCrypter())
	return false, folder.PutObjectWithContext(ctx, objName, reader)
}

// selfArchivedSet tracks segment numbers the receiver itself uploaded to object
// storage (or found already present). The janitor consults it so retention can
// free a self-archived segment without waiting for the primary's
// pg_stat_archiver.last_archived_wal to advance over it.
type selfArchivedSet struct {
	mu sync.Mutex
	m  map[uint64]struct{}
}

var selfArchived = &selfArchivedSet{m: make(map[uint64]struct{})}

func (s *selfArchivedSet) add(segNo uint64) {
	s.mu.Lock()
	s.m[segNo] = struct{}{}
	s.mu.Unlock()
}

func (s *selfArchivedSet) has(segNo uint64) bool {
	s.mu.Lock()
	_, ok := s.m[segNo]
	s.mu.Unlock()
	return ok
}

// pruneAtOrBelow drops entries the primary has now archived (segNo <= cutoff),
// so the set stays bounded by the genuinely-un-archived window.
func (s *selfArchivedSet) pruneAtOrBelow(cutoff uint64) {
	s.mu.Lock()
	for segNo := range s.m {
		if segNo <= cutoff {
			delete(s.m, segNo)
		}
	}
	s.mu.Unlock()
}

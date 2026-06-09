package postgres

import (
	"context"
	"fmt"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"time"

	"github.com/jackc/pglogrepl"
	"github.com/jackc/pgx/v5/pgconn"
	"github.com/pkg/errors"
	"github.com/wal-g/tracelog"
	"github.com/wal-g/wal-g/internal"
	"github.com/wal-g/wal-g/internal/ioextensions"
	"github.com/wal-g/wal-g/utility"
)

const (
	// Sets standbyMessageTimeout in Streaming Replication Protocol. This is
	// now just the keepalive floor: Stream() also ACKs immediately on every
	// writeIndex advance (i.e. after each fsync'd XLogData), so commit
	// latency on the primary is dominated by fsync time, not this timer.
	StandbyMessageTimeout = time.Second * 10
)

/*
NOTE: Preventing a WAL gap is a complex one (also not 100% fixed with arch_command).
* Using replication slot helps, but that should be created and maintained
  by wal-g on standby's too (making sure unconsumed wals are preserved on
  potential new masters too)
* Using sync replication is another option, but non-promotable, and we
  should locally cache to disconnect S3 performance from database performance
* Making something that checks 'what is in wal-g s repo' vs 'where postgres is
  is another option, but when wal-g is no longer running there would be nothing
  preventing postgres from advancing and cleaning, which is what slots are for.
Cleanest would probably be to create the slot on all postgres instances and advance all of them.
Can be done, but first, lets focus on creating wal files from repl msg...

Things to do (future):
* unittests for queryrunner code
* upgrade to pgx/v4
* we might want to add a feature to have wal-g advance multiple slots to support HA setups natively
* Test with different wal size (>=pg11)
*/

type genericWalReceiveError struct {
	error
}

func (err genericWalReceiveError) Error() string {
	return fmt.Sprintf(tracelog.GetErrorFormatter(), err.error)
}

// SkipUploadEnv controls whether wal-receive writes completed segments to
// the configured storage backend. When set to a truthy value, the handler
// runs as a pure fsync+ACK sync-standby: bytes are durably written to the
// local partial file (per-XLogData), but the storage uploader is never
// called. Intended for setups where the primary already runs its own
// archive (wal-push) so receiver-side uploads would be redundant.
const SkipUploadEnv = "WALG_WAL_RECEIVE_SKIP_UPLOAD"

func skipUpload() bool {
	v := strings.ToLower(os.Getenv(SkipUploadEnv))
	return v == "1" || v == "true" || v == "yes" || v == "on"
}

// HandleWALReceive is the long-running wal-receive entry point. It owns
// (a) the streaming connection to the primary, (b) the partial-fsync
// janitor goroutine, and (c) the dr-tail S3 flush-on-primary-loss path.
// When the primary disappears (TCP drop, walsender termination, etc.), we
// flush the un-archived tail to the S3 dr-tail prefix and then loop with
// backoff to re-establish the stream — which will succeed against
// either the original primary (transient blip) or a newly-promoted one
// (after the control plane rewrites our conninfo).
func HandleWALReceive(ctx context.Context, uploader *WalUploader) {
	uploader.ChangeDirectory(utility.WalPath)

	// Janitor + tenant push paths only need to be started once for the
	// lifetime of the process.
	go runPartialJanitor(ctx)

	// Sole-acker poller: watches whether a peer standby is active and flips
	// the receiver between drain-batched (peer present) and per-message fsync
	// (sole sync acker) so commit latency stays low whenever we are the
	// deciding acker, without paying the per-message IOPS cost the rest of
	// the time.
	go runSoleAckerPoller(ctx)

	// Autonomous fallback archival: when the receiver's un-archived backlog
	// grows past a threshold and the primary's archiver looks stalled, upload
	// the missing completed segments ourselves (conditional create-if-absent so
	// we never double-upload or disrupt the primary). No control plane needed;
	// no-op unless WALG_WAL_RECEIVE_FALLBACK_UPLOAD is set.
	go runFallbackUploadPoller(ctx, uploader)

	// Continuous dr-tail flusher: keep the S3 dr-tail prefix within one tick of the
	// flush_lsn ACK during steady streaming so a TRUE total loss (both PG nodes die
	// at once, candidate recovers ONLY from S3) loses at most one interval of acked
	// WAL instead of the whole un-uploaded backlog. No-op unless WALG_WAL_RECEIVE_DR_S3
	// is set; the flush-on-primary-loss path remains the final top-up at the stream
	// drop. See runContinuousDRTailFlusher.
	go runContinuousDRTailFlusher(ctx)

	// Control-plane-orchestrated failover (Option B): when the control listen
	// addr is configured, serve the receiver control API (/v1/status and
	// /v1/dr-catchup) so the control plane can query lastAcceptedLsn and drive
	// a targeted S3 dr-tail catch-up.
	if addr := os.Getenv(WalReceiveControlListenEnv); addr != "" {
		go func() {
			if err := HandleWALReceiveControl(ctx, addr,
				os.Getenv(WalReceiveControlTLSCertEnv),
				os.Getenv(WalReceiveControlTLSKeyEnv),
				os.Getenv(WalReceiveControlClientCAEnv)); err != nil {
				tracelog.ErrorLogger.Printf("wal-receive-control: server exited: %v", err)
			}
		}()
	}

	if skipUpload() {
		tracelog.InfoLogger.Printf("wal-receive: %s set; segments will not be uploaded (sync-standby mode)", SkipUploadEnv)
	}

	backoff := initialReconnectBackoff
	for {
		err := receiveOnce(ctx, uploader)
		if err == nil {
			return // loop only exits cleanly on ctx cancellation propagating from receiveOnce
		}
		if !isPrimaryLost(err) {
			tracelog.ErrorLogger.FatalOnError(err)
		}
		tracelog.WarningLogger.Printf("wal-receive: replication connection lost: %v", err)
		// Autonomous DR-tail flush: make the un-archived tail (incl. the frozen
		// in-flight partial) durable in S3 the moment the primary drops, BEFORE we
		// know whether any node will be promoted. This is the total-loss durability
		// guarantee — with no surviving PG node the control-plane dr-catchup /
		// failover-primary triggers never fire — and the behavior the DR-tail design
		// always specified. Idempotent S3 upload (no-op unless WALG_WAL_RECEIVE_DR_S3
		// is set), de-duplicated and best-effort.
		flushDRTailOnPrimaryLoss(ctx)
		tracelog.InfoLogger.Printf("wal-receive: backing off %v before reconnect", backoff)
		select {
		case <-ctx.Done():
			return
		case <-failoverReconnect:
			// A failover-primary call re-targeted us at a newly-promoted primary;
			// reconnect immediately and reset the backoff rather than waiting it out.
			tracelog.InfoLogger.Printf("wal-receive: failover-primary signalled; reconnecting to %s now", os.Getenv("PGHOST"))
			backoff = initialReconnectBackoff
		case <-time.After(backoff):
			backoff = nextReconnectBackoff(backoff)
		}
	}
}

const (
	initialReconnectBackoff = 2 * time.Second
	maxReconnectBackoff     = 60 * time.Second
)

func nextReconnectBackoff(prev time.Duration) time.Duration {
	if next := prev * 2; next < maxReconnectBackoff {
		return next
	}
	return maxReconnectBackoff
}

// isPrimaryLost classifies an error as a primary-stream-drop (recoverable
// by reconnect + push catch-up) vs an unexpected internal error.
//
// We default to "is primary lost" for any error returned by the
// replication path — false positives just mean an extra push that
// idempotently no-ops on the standby; false negatives mean we fatal
// and the process supervisor restarts us, which is also fine. The
// explicit ProcessMessagePrimaryLost wrapping in wal_segment.go gives
// us a typed signal for the common case.
func isPrimaryLost(err error) bool {
	if err == nil {
		return false
	}
	msg := err.Error()
	// Streaming-side: explicit wrappers from wal_segment.go.
	if strings.Contains(msg, "wal-receive: replication ReceiveMessage") ||
		strings.Contains(msg, "wal-receive: SendStandbyStatusUpdate") {
		return true
	}
	// Setup-side: getCurrentWalInfo's ConnectWithTimeout returning, or
	// the replication-connection setup (pgconn.Connect/IdentifySystem)
	// returning with our wrap. Any of these mean "we couldn't talk to
	// the primary" — same trigger.
	if strings.Contains(msg, "ConnectWithTimeout: connection failed") ||
		strings.Contains(msg, "wal-receive: setup") {
		return true
	}
	return false
}

// receiveOnce wraps one "connect to primary, stream until disconnect or
// timeline change" cycle. Returns nil if ctx canceled, an error otherwise.
func receiveOnce(ctx context.Context, uploader *WalUploader) error {
	var XLogPos pglogrepl.LSN
	var segment *WalSegment

	slot, walSegmentBytes, err := getCurrentWalInfo()
	if err != nil {
		return errors.Wrap(err, "wal-receive: setup getCurrentWalInfo")
	}
	tracelog.DebugLogger.Printf("WAL segment bytes: %d", walSegmentBytes)

	// Bound the connect; otherwise a primary VM that's up but with PG
	// stopped (silent SYN drops, no RST) blocks us for ~2 min on TCP
	// timeout. 10s is plenty for an in-region peer to handshake.
	connCtx, connCancel := context.WithTimeout(context.Background(), 10*time.Second)
	conn, err := pgconn.Connect(connCtx, "replication=yes")
	connCancel()
	if err != nil {
		return errors.Wrap(err, "wal-receive: setup replication conn")
	}
	defer conn.Close(context.Background())

	sysident, err := pglogrepl.IdentifySystem(context.Background(), conn)
	if err != nil {
		return errors.Wrap(err, "wal-receive: setup IdentifySystem")
	}

	if slot.Exists {
		XLogPos = slot.RestartLSN
	} else {
		tracelog.InfoLogger.Println("Trying to create the replication slot")
		_, err = pglogrepl.CreateReplicationSlot(context.Background(), conn, slot.Name, "",
			pglogrepl.CreateReplicationSlotOptions{Mode: pglogrepl.PhysicalReplication})
		if err != nil {
			return errors.Wrap(err, "create replication slot")
		}
		XLogPos = sysident.XLogPos
	}

	timeline, err := getStartTimeline(ctx, conn, uploader, uint32(sysident.Timeline), XLogPos)
	if err != nil {
		return err
	}

	noUpload := skipUpload()

	segment = NewWalSegment(timeline, XLogPos, walSegmentBytes)
	segment.retain = noUpload
	startReplication(conn, segment, slot.Name)
	for {
		streamResult, sErr := segment.Stream(conn, StandbyMessageTimeout)
		if sErr != nil {
			return sErr
		}
		tracelog.DebugLogger.Printf("Successfully received wal segment %s: ", segment.Name())

		switch streamResult {
		case ProcessMessageOK:
			if !noUpload {
				if err := uploader.UploadWalFile(ctx, ioextensions.NewNamedReaderImpl(segment, segment.Name())); err != nil {
					return errors.Wrap(err, "upload wal file")
				}
				if err := uploadRemoteWalMetadata(ctx, segment.Name(), uploader.Uploader); err != nil {
					return errors.Wrap(err, "upload wal metadata")
				}
			}
			if noUpload {
				// Durability back-pressure (sync-standby mode): completed segments
				// are RETAINED until S3-durable. If retained WAL has reached the
				// per-tenant budget, stop accepting new WAL and freeze the flush ACK
				// so the primary's synchronous_commit blocks -- rather than overrun
				// the local disk or drop un-durable WAL. Resumes when the janitor
				// reaps S3-durable segments. The control server keeps serving
				// dr-catchup throughout (its own goroutine).
				applyRetentionBackpressure(conn, walReceivePartialDir(), segment.endLSN)
			}
			XLogPos = segment.endLSN
			segment, err = segment.NextWalSegment()
			if err != nil {
				return err
			}
		case ProcessMessageCopyDone:
			if !noUpload {
				if err := uploader.UploadWalFile(ctx, ioextensions.NewNamedReaderImpl(segment, segment.Name())); err != nil {
					return errors.Wrap(err, "upload wal file (copydone)")
				}
				if err := uploadRemoteWalMetadata(ctx, segment.Name(), uploader.Uploader); err != nil {
					return errors.Wrap(err, "upload wal metadata (copydone)")
				}
			}
			timeline++
			timelinehistfile, err := pglogrepl.TimelineHistory(context.Background(), conn, int32(timeline))
			if err != nil {
				return errors.Wrap(err, "timeline history")
			}
			tlh, err := NewTimeLineHistFile(timeline, timelinehistfile.FileName, timelinehistfile.Content)
			if err != nil {
				return err
			}
			if !noUpload {
				if err := uploader.UploadWalFile(ctx, ioextensions.NewNamedReaderImpl(tlh, tlh.Name())); err != nil {
					return errors.Wrap(err, "upload tlh")
				}
				if err := uploadRemoteWalMetadata(ctx, tlh.Name(), uploader.Uploader); err != nil {
					return errors.Wrap(err, "upload tlh metadata")
				}
			}
			segment.closePartialFile()
			segment = NewWalSegment(timeline, XLogPos, walSegmentBytes)
			segment.retain = noUpload
			startReplication(conn, segment, slot.Name)
		default:
			return errors.Errorf("Unexpected result from WalSegment.Stream() %v", streamResult)
		}
	}
}

// MaxRetainedBytesEnv caps the on-disk retained (not-yet-S3-durable) WAL per
// tenant in sync-standby mode. At/over the cap the receiver stops accepting new
// WAL and freezes its flush ACK (back-pressure) so the primary blocks commits,
// rather than overrunning the local NVMe or dropping un-durable WAL. The local
// instance-store NVMe is shared across co-tenant receivers via hostPath, so this
// must be sized so per-tenant budgets sum to less than the node disk. Default
// 10 GiB; set to 0 to disable the cap.
const MaxRetainedBytesEnv = "WALG_WAL_RECEIVE_MAX_RETAINED_BYTES"
const defaultMaxRetainedBytes int64 = 10 * 1024 * 1024 * 1024 // 10 GiB

// retentionBackpressureWarnPct is the fraction (percent) of the retention
// budget at/above which applyRetentionBackpressure logs a WARNING on every
// segment, so an impending budget-driven freeze is visible well before commits
// actually stall. Below it the per-segment line is debug-level.
const retentionBackpressureWarnPct int64 = 75

func maxRetainedBytes() int64 {
	v := os.Getenv(MaxRetainedBytesEnv)
	if v == "" {
		return defaultMaxRetainedBytes
	}
	n, err := strconv.ParseInt(v, 10, 64)
	if err != nil || n < 0 {
		return defaultMaxRetainedBytes
	}
	return n // 0 disables
}

// retainedBytes is the total size of all files in the partial dir (retained
// completed segments + the in-flight partial) = the on-disk WAL we are holding.
func retainedBytes(dir string) int64 {
	entries, err := os.ReadDir(dir)
	if err != nil {
		return 0
	}
	var total int64
	for _, e := range entries {
		if info, ierr := e.Info(); ierr == nil && !info.IsDir() {
			total += info.Size()
		}
	}
	return total
}

// backpressurePollInterval is how often, while back-pressured, we re-send the
// frozen flush ACK to keep the walsender alive (well under StandbyMessageTimeout)
// and re-check whether the janitor has freed enough S3-durable WAL to resume.
const backpressurePollInterval = 2 * time.Second

// applyRetentionBackpressure blocks while retained WAL is at/over the budget,
// keeping the replication connection alive by re-sending the FROZEN flush ACK
// (frozenLSN) so the primary does not time out the walsender -- but commits past
// frozenLSN stay blocked because we are not acking new WAL. Returns once the
// janitor has reaped enough S3-durable segments to drop back under budget. If S3
// archival is broken the janitor never frees space and this blocks indefinitely
// -- by design: durability-first, primary writes stall until archival recovers.
func applyRetentionBackpressure(conn *pgconn.PgConn, dir string, frozenLSN pglogrepl.LSN) {
	budget := maxRetainedBytes()
	if budget <= 0 {
		return // cap disabled
	}
	// Always surface the retained-WAL headroom so a budget-driven freeze is
	// never invisible after the fact. Historically the ONLY back-pressure log
	// lines fired once we were already frozen, so a slow creep toward the budget
	// (or a freeze that later cleared) left no trace — making it impossible to
	// tell whether a commit stall was retention back-pressure or something else.
	// Debug-log every segment; warn loudly once we cross a soft fraction of the
	// budget, well before the hard freeze.
	if cur := retainedBytes(dir); cur > 0 {
		pct := cur * 100 / budget
		if pct >= retentionBackpressureWarnPct {
			tracelog.WarningLogger.Printf("wal-receive: retained WAL %d/%d bytes (%d%% of budget %s); "+
				"approaching back-pressure freeze at 100%%", cur, budget, pct, MaxRetainedBytesEnv)
		} else {
			tracelog.DebugLogger.Printf("wal-receive: retained WAL %d/%d bytes (%d%% of budget); no back-pressure",
				cur, budget, pct)
		}
	}
	warned := false
	for retainedBytes(dir) >= budget {
		if !warned {
			tracelog.WarningLogger.Printf("wal-receive: retained WAL %d bytes >= budget %d (%s); applying "+
				"back-pressure -- freezing flush ACK at %s so the primary blocks commits until S3 archival "+
				"catches up and the janitor frees space", retainedBytes(dir), budget, MaxRetainedBytesEnv, frozenLSN)
			warned = true
		}
		if err := pglogrepl.SendStandbyStatusUpdate(context.Background(), conn,
			pglogrepl.StandbyStatusUpdate{
				WALWritePosition: frozenLSN,
				WALFlushPosition: frozenLSN,
				WALApplyPosition: frozenLSN,
			}); err != nil {
			// Connection gone; let the outer loop reconnect. The slot retains the
			// un-acked WAL on the primary, so nothing is lost.
			tracelog.WarningLogger.Printf("wal-receive: back-pressure keepalive failed: %v", err)
			return
		}
		time.Sleep(backpressurePollInterval)
	}
	if warned {
		tracelog.InfoLogger.Printf("wal-receive: retained WAL back under budget; resuming stream")
	}
}

// RetainWarnSegmentsEnv is the soft threshold (count of retained completed
// segments not yet durable elsewhere) above which the janitor logs a WARNING
// instead of a debug line. Defaults to 64 (~1 GiB at 16 MiB segments). 0 = off.
const RetainWarnSegmentsEnv = "WALG_WAL_RECEIVE_RETAIN_WARN_SEGMENTS"

func retainWarnSegments() int {
	v := os.Getenv(RetainWarnSegmentsEnv)
	if v == "" {
		return 64
	}
	n, err := strconv.Atoi(v)
	if err != nil || n < 0 {
		tracelog.WarningLogger.Printf("%s=%q invalid, using 64", RetainWarnSegmentsEnv, v)
		return 64
	}
	return n
}

// JanitorIntervalEnv overrides the partial-cleanup poll interval. Defaults
// to 60s. Set to 0 to disable the janitor.
const JanitorIntervalEnv = "WALG_WAL_RECEIVE_JANITOR_INTERVAL_SECONDS"

func janitorInterval() time.Duration {
	v := os.Getenv(JanitorIntervalEnv)
	if v == "" {
		return 60 * time.Second
	}
	n, err := strconv.Atoi(v)
	if err != nil || n < 0 {
		tracelog.WarningLogger.Printf("%s=%q invalid, using 60s", JanitorIntervalEnv, v)
		return 60 * time.Second
	}
	return time.Duration(n) * time.Second
}

// runPartialJanitor runs as a goroutine for the lifetime of wal-receive.
// On each tick it queries pg_stat_archiver.last_archived_wal and removes
// any *.partial files in WALG_WAL_RECEIVE_PARTIAL_DIR whose segment is at or
// below the archived high-water mark — i.e., the primary has confirmed those
// segments are durable somewhere else, so our local copy is redundant.
func runPartialJanitor(ctx context.Context) {
	interval := janitorInterval()
	if interval == 0 {
		tracelog.InfoLogger.Printf("wal-receive: janitor disabled via %s=0", JanitorIntervalEnv)
		return
	}
	dir := walReceivePartialDir()
	t := time.NewTicker(interval)
	defer t.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-t.C:
		}
		if err := janitorSweep(dir); err != nil {
			tracelog.WarningLogger.Printf("wal-receive: janitor sweep failed: %v", err)
		}
	}
}

func janitorSweep(dir string) error {
	entries, err := os.ReadDir(dir)
	if os.IsNotExist(err) {
		return nil
	}
	if err != nil {
		return errors.Wrap(err, "read partial dir")
	}
	var partials []string
	for _, e := range entries {
		if !e.IsDir() && strings.HasSuffix(e.Name(), ".partial") {
			partials = append(partials, e.Name())
		}
	}
	if len(partials) == 0 {
		return nil
	}

	// One temporary SQL connection per sweep — keeps the janitor decoupled
	// from the replication connection and avoids long-lived idle conns.
	// Bounded so the janitor can fail-fast and reschedule when the primary
	// is unreachable (e.g., during the very outage the receiver is meant
	// to ride out by pushing to standbys).
	tmpConn, err := ConnectWithTimeout(10 * time.Second)
	if err != nil {
		return errors.Wrap(err, "janitor connect")
	}
	defer tmpConn.Close(context.TODO())
	qr, err := NewPgQueryRunner(tmpConn)
	if err != nil {
		return errors.Wrap(err, "janitor query runner")
	}
	// A retained segment is safe to delete ONLY once it is durable in S3, i.e.
	// the primary has archived it: segNo <= last_archived_wal. We deliberately do
	// NOT free on "another standby has it" -- a standby is a replica, not a durable
	// store; if it dies while S3 is behind, that WAL is lost (the both-primary-and-
	// standby-die hole). The receiver is the durability backstop, so it holds every
	// completed segment until S3 has it. The per-tenant retention budget
	// (WALG_WAL_RECEIVE_MAX_RETAINED_BYTES) bounds the resulting growth during an
	// archival outage by back-pressuring the primary -- never by dropping WAL.
	archivedName, archivedOK, err := qr.LastArchivedWALFilename()
	if err != nil {
		return errors.Wrap(err, "janitor get last_archived_wal")
	}
	var archivedSegNo uint64
	if archivedOK {
		// last_archived_wal may be a backup label rather than a plain WAL
		// segment name, e.g. "000000020000000000000028.00000028.backup" right
		// after a base backup. Its 24-char WAL-segment prefix is still the
		// archived high-water mark, so parse that prefix instead of failing the
		// whole sweep on the unparseable full label (which would leave retained
		// partials un-reaped until the next plain segment is archived).
		parseName := archivedName
		if dot := strings.IndexByte(parseName, '.'); dot >= 0 {
			parseName = parseName[:dot]
		}
		if _, archivedSegNo, err = ParseWALFilename(parseName); err != nil {
			tracelog.WarningLogger.Printf("wal-receive: janitor skipping unparseable last_archived_wal=%q "+
				"(treating as no archived floor this sweep): %v", archivedName, err)
			archivedOK = false
			archivedSegNo = 0
		}
	}

	deleted, retained := 0, 0
	var retainedBytes int64
	for _, name := range partials {
		walName := strings.TrimSuffix(name, ".partial")
		_, segNo, perr := ParseWALFilename(walName)
		if perr != nil {
			tracelog.DebugLogger.Printf("wal-receive: janitor skipping unparseable %q: %v", name, perr)
			continue
		}
		// Durable in S3 if the primary archived it (segNo <= last_archived_wal)
		// OR the receiver uploaded it itself via autonomous fallback archival.
		durableInS3 := (archivedOK && segNo <= archivedSegNo) || selfArchived.has(segNo)
		path := filepath.Join(dir, name)
		if !durableInS3 {
			retained++
			if fi, serr := os.Stat(path); serr == nil {
				retainedBytes += fi.Size()
			}
			continue
		}
		if rerr := os.Remove(path); rerr != nil {
			tracelog.WarningLogger.Printf("wal-receive: janitor remove %s: %v", path, rerr)
			continue
		}
		deleted++
	}
	if deleted > 0 {
		tracelog.InfoLogger.Printf("wal-receive: janitor deleted %d segment(s) durable in S3 (last_archived=%s)",
			deleted, archivedName)
	}
	// Bounded-disk safety: retention is durability-first — we never drop a
	// segment that is not durable elsewhere — so an extended archival+standby
	// outage grows the partial dir. Warn loudly (above a soft threshold) so ops
	// and the disk-full timer can act before the volume fills; a hard cap that
	// dropped un-durable WAL would reintroduce the data loss this prevents.
	if retained > 0 {
		logger := tracelog.DebugLogger
		if warn := retainWarnSegments(); warn > 0 && retained >= warn {
			logger = tracelog.WarningLogger
		}
		logger.Printf("wal-receive: retaining %d completed segment(s) (%d MiB) not yet durable elsewhere",
			retained, retainedBytes/(1024*1024))
	}
	// Once the primary has archived up to archivedSegNo, drop those entries from
	// the self-archived set — they are covered by last_archived_wal now, so the
	// set stays bounded by the genuinely-un-archived window.
	if archivedOK {
		selfArchived.pruneAtOrBelow(archivedSegNo)
	}
	return nil
}

func getStartTimeline(ctx context.Context,
	conn *pgconn.PgConn,
	uploader *WalUploader,
	systemTimeline uint32,
	xLogPos pglogrepl.LSN) (uint32, error) {
	if systemTimeline < 2 {
		return 1, nil
	}
	timelinehistfile, err := pglogrepl.TimelineHistory(context.Background(), conn, int32(systemTimeline))
	if err == nil {
		tlh, err := NewTimeLineHistFile(systemTimeline, timelinehistfile.FileName, timelinehistfile.Content)
		tracelog.ErrorLogger.FatalOnError(err)
		err = uploader.UploadWalFile(ctx, ioextensions.NewNamedReaderImpl(tlh, tlh.Name()))
		tracelog.ErrorLogger.FatalOnError(err)
		return tlh.LSNToTimeLine(xLogPos)
	}
	if pgErr, ok := err.(*pgconn.PgError); ok {
		if pgErr.Code == "58P01" {
			return systemTimeline, nil
		}
	}
	return 0, nil
}

func startReplication(conn *pgconn.PgConn, segment *WalSegment, slotName string) {
	tracelog.DebugLogger.Printf("Starting replication from %s: ", segment.StartLSN)
	err := pglogrepl.StartReplication(context.Background(), conn, slotName, segment.StartLSN,
		pglogrepl.StartReplicationOptions{Timeline: int32(segment.TimeLine), Mode: pglogrepl.PhysicalReplication})
	tracelog.ErrorLogger.FatalOnError(err)
	tracelog.DebugLogger.Println("Started replication")
}

func getCurrentWalInfo() (slot PhysicalSlot, walSegmentBytes uint64, err error) {
	slotName := internal.GetPgSlotName()

	// Creating a temporary connection to read slot info and wal_segment_size.
	// Bounded — see ConnectWithTimeout doc comment.
	tmpConn, err := ConnectWithTimeout(10 * time.Second)
	if err != nil {
		return
	}
	defer tmpConn.Close(context.TODO())

	queryRunner, err := NewPgQueryRunner(tmpConn)
	if err != nil {
		return
	}

	slot, err = queryRunner.GetPhysicalSlotInfo(slotName)
	if err != nil {
		return
	}

	walSegmentBytes, err = queryRunner.GetWalSegmentBytes()
	if err != nil {
		return
	}

	if slot.Exists {
		slot, err = maybeAdvanceSlot(queryRunner, slot, walSegmentBytes)
	}
	return
}

// CatchupThresholdEnv selects how many bytes of WAL behind the primary the
// slot may be before we fast-forward instead of streaming the gap.
// Default 0 means: always try to advance whenever a target ahead of
// restart_lsn is available.
const CatchupThresholdEnv = "WALG_WAL_RECEIVE_CATCHUP_THRESHOLD_BYTES"

func getCatchupThresholdBytes() uint64 {
	v := os.Getenv(CatchupThresholdEnv)
	if v == "" {
		return 0
	}
	n, err := strconv.ParseUint(v, 10, 64)
	if err != nil {
		tracelog.WarningLogger.Printf("%s=%q is not a valid uint, ignoring", CatchupThresholdEnv, v)
		return 0
	}
	return n
}

// maybeAdvanceSlot fast-forwards this wal-g slot on startup to avoid
// re-streaming gigabytes of WAL the other (PG) standby and/or the next-segment
// boundary already covers.
//
// Target = MIN(other_standby.flush_lsn, current_segment_start)
//   - other_standby.flush_lsn guarantees durability: if we skip past it, the
//     other standby still has those bytes flushed.
//   - current_segment_start aligns our first partial file to a WAL segment
//     boundary, so a crash before this segment finishes still leaves us with
//     a usable whole-segment partial.
//
// If no other streaming standby exists, we do NOT advance — wal-g would be
// the sole durability path for the gap, so it must replay everything.
func maybeAdvanceSlot(queryRunner *PgQueryRunner, slot PhysicalSlot, walSegmentBytes uint64) (PhysicalSlot, error) {
	currentLSN, err := queryRunner.GetCurrentWalLSN()
	if err != nil {
		return slot, err
	}
	if currentLSN <= slot.RestartLSN {
		return slot, nil
	}

	otherLSN, hasOther, err := queryRunner.MinOtherStandbyFlushLSN(internal.GetPgSlotName())
	if err != nil {
		return slot, err
	}
	if !hasOther {
		tracelog.InfoLogger.Printf("wal-receive: no other streaming standby; will catch up from slot.restart_lsn=%s",
			slot.RestartLSN)
		return slot, nil
	}

	segmentStart := currentLSN - pglogrepl.LSN(uint64(currentLSN)%walSegmentBytes)
	target := otherLSN
	if segmentStart < target {
		target = segmentStart
	}
	if target <= slot.RestartLSN {
		return slot, nil
	}

	gap := uint64(target) - uint64(slot.RestartLSN)
	if gap < getCatchupThresholdBytes() {
		tracelog.InfoLogger.Printf("wal-receive: gap %d < threshold; streaming from slot.restart_lsn=%s",
			gap, slot.RestartLSN)
		return slot, nil
	}

	tracelog.InfoLogger.Printf("wal-receive: advancing slot %s from %s to %s (other_standby=%s segment_start=%s current=%s gap=%d)",
		slot.Name, slot.RestartLSN, target, otherLSN, segmentStart, currentLSN, gap)
	newLSN, err := queryRunner.AdvanceReplicationSlot(slot.Name, target)
	if err != nil {
		return slot, err
	}
	tracelog.InfoLogger.Printf("wal-receive: slot %s advanced to %s", slot.Name, newLSN)
	slot.RestartLSN = newLSN
	return slot, nil
}

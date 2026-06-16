package postgres

import (
	"context"
	"fmt"
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
	// Sets standbyMessageTimeout in Streaming Replication Protocol.
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

// HandleWALReceive is the entry point of `wal-g wal-receive`. It opens a Postgres
// streaming-replication connection and archives WAL one segment at a time.
//
// BIG PICTURE — what this command is (and is NOT):
//   - It is an ARCHIVER built on the replication protocol. It pulls WAL exactly the
//     way a streaming replica would, but instead of replaying it, it buffers each
//     16 MiB WAL file fully in memory (a WalSegment) and uploads it to wal-g storage
//     (S3/GCS/...) via the WalUploader, then moves to the next segment.
//   - It is NOT a durable synchronous standby. The only feedback it gives Postgres is
//     a standby-status message advertising `seg.StartLSN` (see WalSegment.Stream) — a
//     coarse, already-passed position. It never fsyncs received bytes and never ACKs
//     the real flush position, so Postgres cannot rely on it for `synchronous_commit`.
//     (The sync-standby fork is exactly what adds partial-file fsync + a real flush_lsn ACK.)
//
// LIFECYCLE: connect → identify system → get/create the physical slot → resolve the
// starting timeline → build the first WalSegment → START_REPLICATION → loop{ fill one
// segment, upload it, rotate }. Errors are fatal (the process exits and is expected to
// be restarted by its supervisor); there is no in-process retry.
func HandleWALReceive(ctx context.Context, uploader *WalUploader) {
	// XLogPos is the LSN we (re)start streaming from; segment is the file currently being filled.
	var XLogPos pglogrepl.LSN
	var segment *WalSegment

	// All uploads land under the storage prefix's `wal_005/` directory (utility.WalPath).
	uploader.ChangeDirectory(utility.WalPath)

	// Read the configured physical slot (name from WALG_SLOTNAME) and the server's
	// wal_segment_size (usually 16 MiB) over a short-lived normal SQL connection.
	slot, walSegmentBytes, err := getCurrentWalInfo()
	tracelog.ErrorLogger.FatalOnError(err) // FatalOnError = log + os.Exit on any error (no recovery here).
	tracelog.DebugLogger.Printf("WAL segment bytes: %d", walSegmentBytes)

	// Open the REPLICATION connection (libpq `replication=yes`). This is a separate,
	// special connection that speaks the streaming-replication sub-protocol (COPY mode),
	// distinct from the normal SQL connection used above.
	conn, err := pgconn.Connect(context.Background(), "replication=yes")
	tracelog.ErrorLogger.FatalOnError(err)
	defer conn.Close(context.Background())

	// IDENTIFY_SYSTEM returns the server's systemid, current timeline, and current
	// WAL flush position (XLogPos) — used as the start point when there's no slot yet.
	sysident, err := pglogrepl.IdentifySystem(context.Background(), conn)
	tracelog.ErrorLogger.FatalOnError(err)

	if slot.Exists {
		// A physical slot already exists: resume from where it left off (its restart_lsn).
		// The slot is what keeps Postgres from recycling WAL we haven't consumed yet.
		XLogPos = slot.RestartLSN
	} else {
		// First run for this slot: create a PHYSICAL replication slot so Postgres starts
		// retaining WAL for us, then start from the server's current position.
		tracelog.InfoLogger.Println("Trying to create the replication slot")
		_, err = pglogrepl.CreateReplicationSlot(context.Background(), conn, slot.Name, "",
			pglogrepl.CreateReplicationSlotOptions{Mode: pglogrepl.PhysicalReplication})
		tracelog.ErrorLogger.FatalOnError(err)
		XLogPos = sysident.XLogPos
	}

	// Resolve which timeline XLogPos belongs to (and archive the .history file). For a
	// fresh cluster (timeline 1) this is trivially 1; after a promotion it walks the
	// timeline-history file to find the right one. See getStartTimeline below.
	timeline, err := getStartTimeline(ctx, conn, uploader, uint32(sysident.Timeline), XLogPos)
	tracelog.ErrorLogger.FatalOnError(err)

	// Build the in-memory buffer for the WAL file that contains XLogPos, then issue
	// START_REPLICATION so the server begins streaming WAL to us.
	segment = NewWalSegment(timeline, XLogPos, walSegmentBytes)
	startReplication(conn, segment, slot.Name)

	// MAIN LOOP: each iteration fills exactly one WAL segment (Stream blocks until the
	// segment is full or the stream ends), then uploads it and advances to the next.
	for {
		// Stream pumps replication messages into `segment` until it's complete
		// (ProcessMessageOK) or Postgres ends the copy on a timeline switch (CopyDone).
		streamResult, err := segment.Stream(conn, StandbyMessageTimeout)
		tracelog.ErrorLogger.FatalOnError(err)
		tracelog.DebugLogger.Printf("Successfully received wal segment %s: ", segment.Name())

		switch streamResult {
		case ProcessMessageOK:
			// A full 16 MiB segment was received. Upload the segment file itself, then a
			// tiny metadata sidecar (.json) used by wal-fetch/backups to know it exists.
			// NOTE: upload is synchronous and inline here — the next segment is not read
			// until this one is durably in storage, so storage latency directly throttles
			// receiving (the comment at the top of the file calls this out as a design cost).
			err = uploader.UploadWalFile(ctx, ioextensions.NewNamedReaderImpl(segment, segment.Name()))
			tracelog.ErrorLogger.FatalOnError(err)
			err = uploadRemoteWalMetadata(ctx, segment.Name(), uploader.Uploader)
			tracelog.ErrorLogger.FatalOnError(err)
			XLogPos = segment.endLSN
			// Rotate to the next segment on the SAME timeline. NextWalSegment also carries
			// over any single message that straddled the segment boundary (see wal_segment.go).
			segment, err = segment.NextWalSegment()
			tracelog.ErrorLogger.FatalOnError(err)
		case ProcessMessageCopyDone:
			// Postgres ended the COPY stream — this happens at a TIMELINE SWITCH (e.g. the
			// primary was promoted). The current segment is incomplete (a `.partial`); upload
			// it as-is, then bump to the next timeline.
			err = uploader.UploadWalFile(ctx, ioextensions.NewNamedReaderImpl(segment, segment.Name()))
			tracelog.ErrorLogger.FatalOnError(err)
			err = uploadRemoteWalMetadata(ctx, segment.Name(), uploader.Uploader)
			tracelog.ErrorLogger.FatalOnError(err)
			timeline++
			// Fetch + archive the .history file for the new timeline so restores can follow
			// the timeline lineage, then resume streaming on the new timeline.
			timelinehistfile, err := pglogrepl.TimelineHistory(context.Background(), conn, int32(timeline))
			tracelog.ErrorLogger.FatalOnError(err)
			tlh, err := NewTimeLineHistFile(timeline, timelinehistfile.FileName, timelinehistfile.Content)
			tracelog.ErrorLogger.FatalOnError(err)
			err = uploader.UploadWalFile(ctx, ioextensions.NewNamedReaderImpl(tlh, tlh.Name()))
			tracelog.ErrorLogger.FatalOnError(err)
			err = uploadRemoteWalMetadata(ctx, tlh.Name(), uploader.Uploader)
			tracelog.ErrorLogger.FatalOnError(err)
			// Start a fresh segment at XLogPos on the new timeline and re-issue START_REPLICATION.
			segment = NewWalSegment(timeline, XLogPos, walSegmentBytes)
			startReplication(conn, segment, slot.Name)
		default:
			// SegmentGap / Mismatch / Unknown all bubble up as fatal — the WAL stream is
			// inconsistent and the only safe move is to exit and restart from the slot.
			tracelog.ErrorLogger.FatalOnError(errors.Errorf("Unexpected result from WalSegment.Stream() %v", streamResult))
		}
	}
}

// getStartTimeline figures out which timeline `xLogPos` lives on, so streaming starts on
// the correct timeline (Postgres forks a new timeline on every promotion). On a brand-new
// cluster (server timeline < 2) the answer is trivially 1. Otherwise it downloads the
// timeline-history file via the replication protocol (TIMELINE_HISTORY), archives it to
// storage (restores need it to walk the timeline lineage), and maps xLogPos → timeline.
func getStartTimeline(ctx context.Context,
	conn *pgconn.PgConn,
	uploader *WalUploader,
	systemTimeline uint32,
	xLogPos pglogrepl.LSN) (uint32, error) {
	if systemTimeline < 2 {
		return 1, nil // No history file exists for timeline 1.
	}
	// TIMELINE_HISTORY replication command: returns the .history file's name + contents.
	timelinehistfile, err := pglogrepl.TimelineHistory(context.Background(), conn, int32(systemTimeline))
	if err == nil {
		tlh, err := NewTimeLineHistFile(systemTimeline, timelinehistfile.FileName, timelinehistfile.Content)
		tracelog.ErrorLogger.FatalOnError(err)
		// Archive the .history file too — backups/restores need the full timeline lineage.
		err = uploader.UploadWalFile(ctx, ioextensions.NewNamedReaderImpl(tlh, tlh.Name()))
		tracelog.ErrorLogger.FatalOnError(err)
		// Parse the history to map our LSN onto the timeline it actually belongs to.
		return tlh.LSNToTimeLine(xLogPos)
	}
	// 58P01 = undefined_file: the history file doesn't exist yet → just use the system timeline.
	if pgErr, ok := err.(*pgconn.PgError); ok {
		if pgErr.Code == "58P01" {
			return systemTimeline, nil
		}
	}
	return 0, nil
}

// startReplication issues START_REPLICATION on the replication connection: it tells Postgres
// "stream physical WAL to me on timeline T, beginning at segment.StartLSN". After this the
// connection is in COPY mode and WalSegment.Stream can start consuming XLogData messages.
func startReplication(conn *pgconn.PgConn, segment *WalSegment, slotName string) {
	tracelog.DebugLogger.Printf("Starting replication from %s: ", segment.StartLSN)
	err := pglogrepl.StartReplication(context.Background(), conn, slotName, segment.StartLSN,
		pglogrepl.StartReplicationOptions{Timeline: int32(segment.TimeLine), Mode: pglogrepl.PhysicalReplication})
	tracelog.ErrorLogger.FatalOnError(err)
	tracelog.DebugLogger.Println("Started replication")
}

// getCurrentWalInfo reads, over a SHORT-LIVED NORMAL SQL connection (not the replication one),
// the two facts the receiver needs before it can start: the physical slot's state
// (does it exist? what's its restart_lsn?) and the server's wal_segment_size (the WAL file
// size, almost always 16 MiB) which sizes the in-memory segment buffer.
func getCurrentWalInfo() (slot PhysicalSlot, walSegmentBytes uint64, err error) {
	slotName := internal.GetPgSlotName() // from WALG_SLOTNAME (default "walg").

	ctx := context.Background()
	tmpConn, err := Connect(ctx) // wal-g's standard SQL connect (libpq env / WALG_* config).
	if err != nil {
		return
	}
	defer tmpConn.Close(ctx)

	// PgQueryRunner wraps the connection with wal-g's catalog queries.
	queryRunner, err := NewPgQueryRunner(ctx, tmpConn)
	if err != nil {
		return
	}

	// SELECT against pg_replication_slots for this slot (Exists / RestartLSN / Active).
	slot, err = queryRunner.GetPhysicalSlotInfo(ctx, slotName)
	if err != nil {
		return
	}

	// SHOW wal_segment_size — the per-file byte count used to allocate/align segments.
	walSegmentBytes, err = queryRunner.GetWalSegmentBytes(ctx)
	return
}

package postgres

/*
This object represents a wal segment. A wal segment is a memory location that holds all wal for a wal file.
wal-g receivewal reads wal from Postgres one wal segment at a time,
and writes it out using the WalUploader before reading the next wal segment.
*/

import (
	"context"
	"io"
	"os"
	"path/filepath"
	"strings"
	"sync/atomic"
	"time"

	"github.com/jackc/pglogrepl"
	"github.com/jackc/pgx/v5/pgconn"
	"github.com/jackc/pgx/v5/pgproto3"
	"github.com/pkg/errors"
	"github.com/wal-g/tracelog"
)

// highestFsyncdLSN is the highest LSN whose bytes are durably on the
// receiver's local disk (post-fsync in writeAndSync). The control API's
// /v1/status and /v1/dr-catchup snapshot this value as the receiver's
// durable frontier (lastAcceptedLsn) and the dr-tail gate target. Atomic
// because Stream() updates it from one goroutine while the control
// handlers read it from another.
var highestFsyncdLSN atomic.Uint64

// HighestFsyncdLSN returns the most recent durably-fsynced WAL LSN the
// receiver has seen, or 0 if none yet.
func HighestFsyncdLSN() pglogrepl.LSN {
	return pglogrepl.LSN(highestFsyncdLSN.Load())
}

// PartialDirEnv names the env var that selects where wal-receive fsyncs each
// incoming XLogData payload before it ACKs flush_lsn to the primary. The
// default keeps wal-receive usable without extra configuration. The same
// path is reused across runs so a crash followed by restart can in principle
// re-stream from the slot's restart_lsn — the partials themselves are not
// recovered by wal-g today; they exist purely so the fsync of received WAL
// happens before the synchronous-standby ACK goes out.
const PartialDirEnv = "WALG_WAL_RECEIVE_PARTIAL_DIR"

func walReceivePartialDir() string {
	if dir := os.Getenv(PartialDirEnv); dir != "" {
		return dir
	}
	return filepath.Join(os.TempDir(), "wal-g-receive-partials")
}

type segmentError struct {
	error
}

// The WalSegment object represents a Postgres Wal Segment, holding all wal data for a wal file.
type WalSegment struct {
	TimeLine        uint32
	StartLSN        pglogrepl.LSN
	endLSN          pglogrepl.LSN
	walSegmentBytes uint64
	data            []byte
	readIndex       int
	writeIndex      int
	lastMsg         *pgproto3.BackendMessage

	// partialFile is the on-disk backing for this segment, pre-allocated to
	// walSegmentBytes, opened lazily on the first received XLogData. Every
	// received message is WriteAt'd at its segment offset and fsync'd before
	// we ACK flush_lsn back to the primary. This is what lets wal-receive
	// serve as a synchronous standby with real durability semantics.
	partialFile *os.File

	// retain, when true (sync-standby / skip-upload mode), keeps this segment's
	// on-disk partial after the segment completes instead of deleting it on
	// rotation. The janitor reaps it only once the bytes are durable elsewhere —
	// the primary has archived the segment OR another standby's slot has flushed
	// past it. This makes the receiver a real durable holder of *completed* WAL
	// (and lets dr-catchup ship the whole gap), not just the in-flight segment.
	retain bool
}

// The ProcessMessageResult is an enum representing possible results from the methods
// processing the messages as received from Postgres into the wal segment.
type ProcessMessageResult int

// These are the multiple results that the methods can return
const (
	ProcessMessageOK ProcessMessageResult = iota
	ProcessMessageUnknown
	ProcessMessageCopyDone
	ProcessMessageReplyRequested
	ProcessMessageSegmentGap
	ProcessMessageMismatch
	// ProcessMessagePrimaryLost is returned by Stream when the replication
	// connection to the primary terminates unexpectedly (TCP drop, walsender
	// terminated by primary because of demotion, network partition). The
	// receiver-side handler treats this as the trigger to push its tail
	// partials to registered standbys, then re-establish the stream.
	ProcessMessagePrimaryLost
)

// NewWalSegment is a helper function to declare a new WalSegment.
func NewWalSegment(timeline uint32, location pglogrepl.LSN, walSegmentBytes uint64) *WalSegment {
	//We could test validity of walSegmentBytes (not implemented):
	//  https://www.postgresql.org/docs/11/app-initdb.html:
	//    Set the WAL segment size, in megabytes...
	//    The value must be a power of 2 between 1 and 1024 (megabytes)

	segment := &WalSegment{TimeLine: timeline, walSegmentBytes: walSegmentBytes}
	// Calculate start byte of file from location (which could be anywhere in this file)
	segment.StartLSN = pglogrepl.LSN((uint64(location) / walSegmentBytes) * walSegmentBytes)
	// Calculate end form start and number of bytes in this file
	segment.endLSN = segment.StartLSN + pglogrepl.LSN(walSegmentBytes)
	// Allocate data
	segment.data = make([]byte, walSegmentBytes)
	return segment
}

// NextWalSegment is a helper function to create the next wal segment which comes after this wal segment.
// Note that this will be on the same timeline. the convenience is that it also automatically processes
// a message that crosses the boundary between the two segments.
func (seg *WalSegment) NextWalSegment() (*WalSegment, error) {
	// Next on this timeline, but read rest of msg
	if !seg.isComplete() {
		return nil, segmentError{
			errors.Errorf("Cannot run NextWalSegment until isComplete")}
	}
	// At this point Stream() has already returned ProcessMessageOK. In upload
	// mode the handler has uploaded this segment, so the on-disk partial is
	// redundant and closePartialFile drops it. In retain (sync-standby) mode
	// closePartialFile keeps it as a durable copy for the janitor to reap later.
	seg.closePartialFile()
	nextSegment := NewWalSegment(seg.TimeLine, seg.endLSN, seg.walSegmentBytes)
	nextSegment.retain = seg.retain
	if seg.lastMsg != nil {
		// Apparaently the last message crossed the border between the two segments,
		// so lets have it processed into the next segment too.
		result, err := nextSegment.processMessage(*seg.lastMsg)
		if err != nil {
			return nil, err
		}
		if result != ProcessMessageOK {
			return nil, segmentError{
				errors.Errorf("Unexpected result from processMessage in NextWalSegment")}
		}
	}
	return nextSegment, nil
}

// ensurePartialFile opens (lazily) a sparse, pre-truncated file sized to the
// full WAL segment. We allocate the whole segment up front so that WriteAt at
// any offset within the segment Just Works without intermediate appends.
func (seg *WalSegment) ensurePartialFile() error {
	if seg.partialFile != nil {
		return nil
	}
	dir := walReceivePartialDir()
	if err := os.MkdirAll(dir, 0o755); err != nil {
		return errors.Wrap(err, "create wal-receive partial dir")
	}
	segID := uint64(seg.StartLSN) / seg.walSegmentBytes
	path := filepath.Join(dir, formatWALFileName(seg.TimeLine, segID)+".partial")
	// Open WITHOUT O_TRUNC. On a primary-loss reconnect the receiver re-streams
	// THIS in-flight segment from its start; physical replication re-sends the
	// IDENTICAL bytes for the same (timeline, segNo), so we OVERWRITE IN PLACE
	// instead of zeroing first. That keeps already-acked bytes durable on disk
	// throughout the re-stream — a crash mid-overwrite leaves either the
	// old-correct or the new-identical bytes, never zeros — closing the
	// reconnect-truncate window where acked WAL could exist only in non-durable
	// page cache while the primary's slot.restart_lsn had already advanced past
	// it. (Previously this opened O_TRUNC and re-zeroed the whole segment, then
	// lowered the durable frontier — transiently exposing acked bytes as zeros.)
	f, err := os.OpenFile(path, os.O_RDWR|os.O_CREATE, 0o600)
	if err != nil {
		return errors.Wrap(err, "open wal-receive partial")
	}
	// Pre-allocate the full segment only when the file is new or short. Truncate
	// UP never discards existing bytes (it zero-extends), so an existing full-size
	// partial keeps its received WAL; a freshly-created (size 0) file is
	// zero-filled to the segment size so WriteAt at any offset Just Works.
	fi, statErr := f.Stat()
	if statErr != nil {
		f.Close()
		return errors.Wrap(statErr, "stat wal-receive partial")
	}
	if fi.Size() < int64(seg.walSegmentBytes) {
		if err := f.Truncate(int64(seg.walSegmentBytes)); err != nil {
			f.Close()
			return errors.Wrap(err, "preallocate wal-receive partial")
		}
	}
	seg.partialFile = f
	// No frontier clamp here. Because bytes are overwritten in place (never
	// zeroed), the global HighestFsyncdLSN stays honestly backed by on-disk
	// bytes, and syncPartial only ever RAISES it (monotonic CAS), so re-streaming
	// this segment from offset 0 cannot lower a frontier whose backing bytes are
	// still present. The acked-but-zeroed RPO>0 window the clamp guarded against
	// no longer exists.
	return nil
}

// writeBuffered persists `buf` at `offset` within the segment's partial
// file but DOES NOT fsync. The drain-batching path in Stream() calls this
// for each received XLogData, then issues a single syncPartial() after
// draining whatever the kernel had buffered. The flush_lsn ACK is bumped
// by syncPartial(), not here — only fsync'd bytes count as flushed.
func (seg *WalSegment) writeBuffered(buf []byte, offset int) error {
	if err := seg.ensurePartialFile(); err != nil {
		return err
	}
	if _, err := seg.partialFile.WriteAt(buf, int64(offset)); err != nil {
		return errors.Wrap(err, "write wal-receive partial")
	}
	return nil
}

// syncPartial fsyncs whatever is in the partial file and bumps the global
// highest-fsynced LSN to the current `writeIndex` (i.e., everything that
// has been writeBuffered'd up to this call is now durable). Returns nil
// silently if the partial file hasn't been opened yet (no bytes received).
func (seg *WalSegment) syncPartial() error {
	if seg.partialFile == nil {
		return nil
	}
	if err := seg.partialFile.Sync(); err != nil {
		return errors.Wrap(err, "fsync wal-receive partial")
	}
	// Everything written up to writeIndex is now durable. Bump the
	// global high-water mark used by push-on-drop's marker LSN and by
	// Stream()'s ACK path.
	fsyncdLSN := uint64(seg.StartLSN) + uint64(seg.writeIndex)
	for {
		cur := highestFsyncdLSN.Load()
		if fsyncdLSN <= cur || highestFsyncdLSN.CompareAndSwap(cur, fsyncdLSN) {
			break
		}
	}
	return nil
}

// closePartialFile closes the on-disk partial handle. In upload mode it then
// removes the file (the uploader has written the final object to storage). In
// retain (sync-standby) mode it leaves the file in place as a durable copy of
// the completed segment for the janitor to reap once it is durable elsewhere.
func (seg *WalSegment) closePartialFile() {
	if seg.partialFile == nil {
		return
	}
	name := seg.partialFile.Name()
	_ = seg.partialFile.Close()
	seg.partialFile = nil
	if seg.retain {
		// sync-standby mode: keep the completed segment on disk as a retained
		// durable copy. The janitor removes it only once it is durable elsewhere
		// (archived by the primary, or flushed past by another standby's slot).
		// This is what lets dr-catchup deliver completed, un-archived segments.
		return
	}
	_ = os.Remove(name)
}

// Name returns the filename of this wal segment.
// This is also used by the WalUploader to set the name of the destination file during upload of the wal segment.
func (seg *WalSegment) Name() string {
	// Example LSN -> Name:
	// '0/2A33FE00' -> '00000001000000000000002A'
	segID := uint64(seg.StartLSN) / seg.walSegmentBytes
	if seg.isComplete() {
		return formatWALFileName(seg.TimeLine, segID)
	}
	return formatWALFileName(seg.TimeLine, segID) + ".partial"
}

// processMessage is a method that processes a message from Postgres and copies its data
// into the right location of the wal segment.
func (seg *WalSegment) processMessage(message pgproto3.BackendMessage) (ProcessMessageResult, error) {
	var messageOffset pglogrepl.LSN
	switch msg := message.(type) {
	case *pgproto3.CopyData:
		switch msg.Data[0] {
		case pglogrepl.PrimaryKeepaliveMessageByteID:
			pkm, err := pglogrepl.ParsePrimaryKeepaliveMessage(msg.Data[1:])
			tracelog.ErrorLogger.FatalOnError(err)
			tracelog.DebugLogger.Println("Primary Keepalive Message =>",
				"ServerWALEnd:", pkm.ServerWALEnd, "ServerTime:", pkm.ServerTime,
				"ReplyRequested:", pkm.ReplyRequested)

			if pkm.ReplyRequested {
				return ProcessMessageReplyRequested, nil
			}
		case pglogrepl.XLogDataByteID:
			xld, err := pglogrepl.ParseXLogData(msg.Data[1:])
			tracelog.ErrorLogger.FatalOnError(err)
			if xld.WALStart > seg.endLSN {
				// This message started after this segment ended
				return ProcessMessageMismatch, segmentError{
					errors.Errorf("Message mismatch: Message started after end of this segment")}
			}
			walEnd := pglogrepl.LSN(uint64(xld.WALStart) + uint64(len(xld.WALData)))
			if walEnd < seg.StartLSN {
				// This message ended before this segment started
				return ProcessMessageMismatch, segmentError{
					errors.Errorf("Message mismatch: Message ended before start of this segment")}
			}
			if xld.WALStart < seg.StartLSN {
				// This message started before this segment started, but should still have a piece for this segment
				messageOffset = seg.StartLSN - xld.WALStart
			}
			tracelog.DebugLogger.Println("XLogData =>", "WALStart", xld.WALStart, "WALEnd", walEnd,
				"LenWALData", len(xld.WALData), "ServerWALEnd", xld.ServerWALEnd,
				"messageOffset", messageOffset) //, "ServerTime:", xld.ServerTime)
			if seg.StartLSN+pglogrepl.LSN(seg.writeIndex) != (xld.WALStart + messageOffset) {
				return ProcessMessageSegmentGap, segmentError{
					errors.Errorf("WAL segment error: CopyData WALStart does not fit to segment writeIndex")}
			}
			copiedBytes := copy(seg.data[seg.writeIndex:], xld.WALData[messageOffset:])
			// Buffer-write only — Stream() drains a burst of XLogData
			// messages and then issues a single fsync via syncPartial(),
			// instead of fsyncing each message. This is the "socket-drain"
			// batching that trades zero added commit latency for a
			// significant reduction in fsync rate when PG sends WAL
			// in bursts (which it does at any non-trivial commit rate).
			if copiedBytes > 0 {
				start := int(messageOffset)
				if err := seg.writeBuffered(xld.WALData[start:start+copiedBytes], seg.writeIndex); err != nil {
					return ProcessMessageMismatch, segmentError{err}
				}
			}
			seg.writeIndex += copiedBytes
			if copiedBytes < len(xld.WALData[messageOffset:]) {
				seg.lastMsg = &message
			}
		}
	case *pgproto3.CopyDone:
		return ProcessMessageCopyDone, nil
	case *pgproto3.CommandComplete, *pgproto3.ErrorResponse, *pgproto3.ReadyForQuery:
		// Primary ended the COPY mode and/or the walsender — typical when
		// `pg_ctl stop`, `pg_ctl restart`, or a graceful demotion happens
		// on the primary side. Treat the same as a TCP drop: surface as
		// ProcessMessagePrimaryLost so the handler can run push catch-up
		// and reconnect.
		return ProcessMessagePrimaryLost,
			errors.Errorf("wal-receive: replication ReceiveMessage: primary ended walsender (%T)", msg)
	default:
		return ProcessMessageUnknown, segmentError{errors.Errorf("Received unexpected message: %#v\n", msg)}
	}
	return ProcessMessageOK, nil
}

// drainPeekTimeout is how long Stream() waits trying to drain another
// XLogData from the kernel TCP buffer after processing the current one.
// Set in microseconds — long enough that an in-the-same-burst message
// will arrive, short enough that we don't add measurable commit latency
// when the burst is actually over.
const drainPeekTimeout = 250 * time.Microsecond

// ackCadence bounds how long the receive loop will block in a single
// ReceiveMessage before waking to re-evaluate the flush-LSN ACK. It is the
// upper bound on how stale the standby-status-update the primary sees can get
// while NO new WAL is arriving — which is exactly the segment-boundary freeze
// case: under saturating concurrent load every backend can be parked in
// SyncRep waiting on the ACK for the bytes we just fsync'd, so the primary
// sends nothing more and the old message-driven ACK never fires. Capping the
// receive deadline here turns the ACK loop from purely reactive (only ACKs on
// the next inbound message) into proactive (re-ACKs the fsync frontier every
// few ms regardless), which is what prevents a slow segment transition from
// pinning flush_lsn at the 16 MiB boundary for a keepalive interval (~10-21s).
//
// Kept small (a few ms) so it adds no measurable steady-state commit latency:
// in the common case an inbound message arrives well before the deadline and
// the loop never waits this long.
const ackCadence = 2 * time.Millisecond

// drainMaxBytes / drainMaxWindow bound the size and wall-clock duration
// of a single drain batch. Safety floors only — the primary signal to
// stop draining is the socket emptying out (which fires drainPeekTimeout).
const (
	drainMaxBytes  = 4 * 1024 * 1024
	drainMaxWindow = 2 * time.Millisecond
)

// DrainBatchingEnv toggles the drain-batching fsync path. Default is
// drain-batched: processMessage only writes to memory, Stream() drains
// additional XLogDatas from the kernel buffer with a short peek
// timeout, then fsyncs once per drain. Set to "false" / "0" / "no" /
// "off" to revert to the original behavior — fsync after every
// received XLogData, no drain. Useful for A/B comparisons against the
// per-message-fsync baseline.
const DrainBatchingEnv = "WALG_WAL_RECEIVE_DRAIN_BATCHING"

func drainBatchingEnabled() bool {
	v := strings.ToLower(os.Getenv(DrainBatchingEnv))
	switch v {
	case "0", "false", "no", "off":
		return false
	default:
		return true
	}
}

// Stream retrieves messages from Postgres and processes them. The flush
// (and the corresponding flush_lsn ACK) happens in batches: after
// receiving one XLogData we drain whatever else is already in the
// kernel TCP buffer, write all of it to the partial file, then fsync
// once. This is what lets the receiver keep up with a saturated 16+
// vCPU primary without driving the local NVMe into single-fsync-per-
// message limits.
func (seg *WalSegment) Stream(conn *pgconn.PgConn, standbyMessageTimeout time.Duration) (ProcessMessageResult, error) {
	var err error
	var msg pgproto3.BackendMessage
	nextStandbyMessageDeadline := time.Now()
	// lastAckedFsyncIndex is the writeIndex *as of the last fsync we
	// ACK'd*. The ACK reflects flush_lsn semantics — only fsync'd bytes
	// count — so we compare against the on-disk frontier, not the
	// in-memory writeIndex which may be ahead of fsync during a drain.
	lastAckedFsyncIndex := int64(-1)
	drainConfigured := drainBatchingEnabled()
	tracelog.InfoLogger.Printf("wal-receive Stream(): drain-batching configured = %v "+
		"(sole-acker poller overrides to per-message fsync when no peer standby is active)", drainConfigured)

	// Proactively fsync anything NextWalSegment carried over from the previous
	// segment's boundary-spanning message. Those tail bytes were writeBuffered
	// (no fsync) when this segment was constructed, so HighestFsyncdLSN() does
	// not yet include them. Without this, the only thing that would fsync+ACK
	// them is the NEXT inbound XLogData — but under saturating concurrent load
	// every backend can already be parked in SyncRep waiting on exactly that
	// ACK, so the primary sends nothing more and flush_lsn pins at the 16 MiB
	// segment boundary for a whole keepalive interval. Syncing here means the
	// first loop iteration ACKs the carried-over bytes immediately.
	if seg.partialFile != nil {
		if syncErr := seg.syncPartial(); syncErr != nil {
			return ProcessMessagePrimaryLost, errors.Wrap(syncErr, "wal-receive: fsync carried-over boundary bytes")
		}
	}

	for {
		fsyncedIndex := int64(HighestFsyncdLSN()) - int64(seg.StartLSN)
		shouldAck := fsyncedIndex != lastAckedFsyncIndex || time.Now().After(nextStandbyMessageDeadline)
		if shouldAck {
			currentPos := pglogrepl.LSN(int64(seg.StartLSN) + fsyncedIndex)
			if fsyncedIndex < 0 {
				currentPos = seg.StartLSN
			}
			err = pglogrepl.SendStandbyStatusUpdate(context.Background(),
				conn,
				pglogrepl.StandbyStatusUpdate{
					WALWritePosition: currentPos,
					WALFlushPosition: currentPos,
					WALApplyPosition: currentPos,
				})
			if err != nil {
				return ProcessMessagePrimaryLost, errors.Wrap(err, "wal-receive: SendStandbyStatusUpdate")
			}
			tracelog.DebugLogger.Println("Sent Standby status message")
			lastAckedFsyncIndex = fsyncedIndex
			nextStandbyMessageDeadline = time.Now().Add(standbyMessageTimeout)
		}

		// Bound the blocking receive at ackCadence so the loop wakes to
		// re-evaluate (and proactively re-emit) the flush-LSN ACK every few ms
		// even when NO message arrives — see ackCadence. nextStandbyMessageDeadline
		// (the ~10s keepalive floor) is still respected as the outer cap; we just
		// never block past the next ACK-cadence tick. The receive deadline firing
		// is a normal timeout (handled below as a continue), not a primary loss.
		recvDeadline := time.Now().Add(ackCadence)
		if nextStandbyMessageDeadline.Before(recvDeadline) {
			recvDeadline = nextStandbyMessageDeadline
		}
		ctx, cancel := context.WithDeadline(context.Background(), recvDeadline)
		msg, err = conn.ReceiveMessage(ctx)
		cancel()
		if pgconn.Timeout(err) {
			continue
		}
		if err != nil {
			return ProcessMessagePrimaryLost, errors.Wrap(err, "wal-receive: replication ReceiveMessage")
		}

		result, perr := seg.processMessage(msg)

		// Drain-batching: if this was a normal XLogData, try to pull more
		// XLogDatas off the socket without blocking. Each adds to seg.data
		// via writeBuffered (no fsync). We stop draining when the socket
		// is empty (drainPeekTimeout fires), the batch hits drainMaxBytes,
		// or we exceed drainMaxWindow, or processMessage returns a non-OK
		// result that needs handling by the switch below.
		//
		// Skipped entirely when drain-batching is disabled — outer-loop
		// syncPartial then fires once per received message (per-message
		// fsync, the original behavior).
		if drainConfigured && !isSoleAcker() && result == ProcessMessageOK && perr == nil && !seg.isComplete() {
			batchStartIdx := seg.writeIndex
			batchStart := time.Now()
		drainLoop:
			for {
				if seg.writeIndex-batchStartIdx >= drainMaxBytes {
					break
				}
				if time.Since(batchStart) >= drainMaxWindow {
					break
				}
				// Stop draining at segment boundary — any further message
				// would have WALStart > seg.endLSN and trigger
				// ProcessMessageMismatch. Let the outer switch return
				// ProcessMessageOK so the handler advances to NextWalSegment.
				if seg.isComplete() {
					break
				}
				drainCtx, drainCancel := context.WithTimeout(context.Background(), drainPeekTimeout)
				drainMsg, drainErr := conn.ReceiveMessage(drainCtx)
				drainCancel()
				if drainErr != nil {
					// Socket empty (timeout) is the common stop signal.
					// Real errors will surface again on the next blocking
					// ReceiveMessage at the top of the outer loop.
					break drainLoop
				}
				dResult, dPerr := seg.processMessage(drainMsg)
				if dResult != ProcessMessageOK || dPerr != nil {
					// Carry the special result out to the switch below so
					// it's handled normally (CopyDone, ReplyRequested, etc.)
					result, perr = dResult, dPerr
					break drainLoop
				}
			}
		}

		// Fsync whatever buffered bytes the drain accumulated (or that
		// the initial processMessage wrote, if drain wasn't reached).
		if seg.partialFile != nil {
			if syncErr := seg.syncPartial(); syncErr != nil {
				return ProcessMessagePrimaryLost, errors.Wrap(syncErr, "wal-receive: fsync drain batch")
			}
		}

		switch result {
		case ProcessMessageOK:
			if seg.isComplete() {
				return ProcessMessageOK, nil
			}
		case ProcessMessagePrimaryLost:
			return result, perr
		case ProcessMessageUnknown:
			return result, perr
		case ProcessMessageCopyDone:
			cdr, err := pglogrepl.SendStandbyCopyDone(context.Background(), conn)
			tracelog.ErrorLogger.FatalOnError(err)
			tracelog.DebugLogger.Printf("CopyDoneResult => %v", cdr)
			return result, nil
		case ProcessMessageReplyRequested:
			if seg.isComplete() {
				return ProcessMessageOK, nil
			}
			nextStandbyMessageDeadline = time.Time{}
		case ProcessMessageSegmentGap:
			return result, perr
		case ProcessMessageMismatch:
			return result, perr
		default:
			tracelog.DebugLogger.Printf("Unexpected processMessage result => %v", result)
			return result, perr
		}
	}
}

// isComplete is a helper function which returns true when all data is added
func (seg *WalSegment) isComplete() bool {
	return seg.StartLSN+pglogrepl.LSN(seg.writeIndex) >= seg.endLSN
}

// Read is what makes the WalSegment an io.Reader, which can be handled by WalUploader.UploadWalFile to write to a file.
func (seg *WalSegment) Read(p []byte) (n int, err error) {
	n = copy(p, seg.data[seg.readIndex:])
	seg.readIndex += n
	if len(seg.data) <= seg.readIndex {
		return n, io.EOF
	}
	return n, nil
}

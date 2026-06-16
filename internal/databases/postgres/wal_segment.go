package postgres

/*
This object represents a wal segment. A wal segment is a memory location that holds all wal for a wal file.
wal-g receivewal reads wal from Postgres one wal segment at a time,
and writes it out using the WalUploader before reading the next wal segment.
*/

import (
	"context"
	"io"
	"time"

	"github.com/jackc/pglogrepl"
	"github.com/jackc/pgx/v5/pgconn"
	"github.com/jackc/pgx/v5/pgproto3"
	"github.com/pkg/errors"
	"github.com/wal-g/tracelog"
)

type segmentError struct {
	error
}

// The WalSegment object represents a Postgres Wal Segment, holding all wal data for a wal file.
//
// It is an IN-MEMORY, write-once buffer for exactly one WAL file (StartLSN .. endLSN,
// `walSegmentBytes` wide). The receive side fills `data` from replication messages
// (writeIndex advances); once full, the upload side reads it back out (readIndex advances,
// see Read — WalSegment is an io.Reader). There is no disk file and no fsync here: the
// whole segment lives in RAM until it is uploaded to storage. (The sync-standby fork is
// precisely what adds an on-disk partial + fsync so received bytes are durable before ACK.)
type WalSegment struct {
	TimeLine        uint32                   // timeline this segment belongs to.
	StartLSN        pglogrepl.LSN            // LSN of the first byte of this WAL file (segment-aligned).
	endLSN          pglogrepl.LSN            // StartLSN + walSegmentBytes (one past the last byte).
	walSegmentBytes uint64                   // WAL file size (e.g. 16 MiB); the width of `data`.
	data            []byte                   // the segment's bytes, written by processMessage, read by Read.
	readIndex       int                      // upload cursor (io.Reader position).
	writeIndex      int                      // receive cursor (how many bytes received so far).
	lastMsg         *pgproto3.BackendMessage // a message that straddled this segment's end, replayed into the next.
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
)

// NewWalSegment is a helper function to declare a new WalSegment.
func NewWalSegment(timeline uint32, location pglogrepl.LSN, walSegmentBytes uint64) *WalSegment {
	//We could test validity of walSegmentBytes (not implemented):
	//  https://www.postgresql.org/docs/11/app-initdb.html:
	//    Set the WAL segment size, in megabytes...
	//    The value must be a power of 2 between 1 and 1024 (megabytes)

	segment := &WalSegment{TimeLine: timeline, walSegmentBytes: walSegmentBytes}
	// `location` can be any LSN inside the file; round DOWN to the segment boundary so
	// StartLSN is always the first byte of a WAL file (integer-divide then multiply).
	segment.StartLSN = pglogrepl.LSN((uint64(location) / walSegmentBytes) * walSegmentBytes)
	// One past the last byte of this file.
	segment.endLSN = segment.StartLSN + pglogrepl.LSN(walSegmentBytes)
	// Allocate the whole-file buffer up front (zero-filled).
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
	// The next file starts exactly where this one ended, on the same timeline.
	nextSegment := NewWalSegment(seg.TimeLine, seg.endLSN, seg.walSegmentBytes)
	if seg.lastMsg != nil {
		// A single XLogData message can carry bytes for two adjacent segments. processMessage
		// saved it as lastMsg when it could only copy the head of it into this segment; here we
		// feed that SAME message into the new segment so its tail (the bytes past the boundary)
		// land at writeIndex 0 of the next file. processMessage uses messageOffset to skip the
		// already-consumed head — so no bytes are lost or duplicated across the boundary.
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
	// messageOffset is how many bytes at the front of this message belong to the PREVIOUS
	// segment and must be skipped (set only for a boundary-crossing message).
	var messageOffset pglogrepl.LSN
	switch msg := message.(type) {
	case *pgproto3.CopyData:
		// In replication COPY mode, every payload is a CopyData whose first byte is a tag.
		switch msg.Data[0] {
		case pglogrepl.PrimaryKeepaliveMessageByteID:
			// 'k' — a keepalive from the primary. It carries the server's current WAL end and,
			// importantly, a ReplyRequested flag: when set, the primary wants a standby-status
			// reply soon (otherwise it may consider us dead). We don't store any WAL here.
			pkm, err := pglogrepl.ParsePrimaryKeepaliveMessage(msg.Data[1:])
			tracelog.ErrorLogger.FatalOnError(err)
			tracelog.DebugLogger.Println("Primary Keepalive Message =>",
				"ServerWALEnd:", pkm.ServerWALEnd, "ServerTime:", pkm.ServerTime,
				"ReplyRequested:", pkm.ReplyRequested)

			if pkm.ReplyRequested {
				return ProcessMessageReplyRequested, nil // Stream will force an immediate status reply.
			}
		case pglogrepl.XLogDataByteID:
			// 'w' — actual WAL bytes. WALStart is the LSN of the first byte; WALData is the payload.
			xld, err := pglogrepl.ParseXLogData(msg.Data[1:])
			tracelog.ErrorLogger.FatalOnError(err)
			// Sanity bound 1: the message can't begin after this segment's end.
			if xld.WALStart > seg.endLSN {
				return ProcessMessageMismatch, segmentError{
					errors.Errorf("Message mismatch: Message started after end of this segment")}
			}
			walEnd := pglogrepl.LSN(uint64(xld.WALStart) + uint64(len(xld.WALData)))
			// Sanity bound 2: the message can't end before this segment's start.
			if walEnd < seg.StartLSN {
				return ProcessMessageMismatch, segmentError{
					errors.Errorf("Message mismatch: Message ended before start of this segment")}
			}
			// Boundary-crossing case: the message begins in the previous segment. Skip the head
			// that already landed there; only the bytes from seg.StartLSN onward are ours.
			if xld.WALStart < seg.StartLSN {
				messageOffset = seg.StartLSN - xld.WALStart
			}
			tracelog.DebugLogger.Println("XLogData =>", "WALStart", xld.WALStart, "WALEnd", walEnd,
				"LenWALData", len(xld.WALData), "ServerWALEnd", xld.ServerWALEnd,
				"messageOffset", messageOffset) //, "ServerTime:", xld.ServerTime)
			// GAP CHECK — the safety invariant: the next byte we expect to write
			// (StartLSN + writeIndex) MUST equal where this message's payload begins
			// (WALStart + messageOffset). If they differ, WAL was skipped/duplicated and the
			// segment would be corrupt → bail out (Stream turns this into a fatal exit).
			if seg.StartLSN+pglogrepl.LSN(seg.writeIndex) != (xld.WALStart + messageOffset) {
				return ProcessMessageSegmentGap, segmentError{
					errors.Errorf("WAL segment error: CopyData WALStart does not fit to segment writeIndex")}
			}
			// Copy the payload (past any boundary offset) into the buffer at writeIndex. `copy`
			// is bounded by the remaining space in `data`, so a message that overruns the file end
			// is truncated here...
			copiedBytes := copy(seg.data[seg.writeIndex:], xld.WALData[messageOffset:])
			seg.writeIndex += copiedBytes
			// ...and if we couldn't take the whole payload, the tail belongs to the NEXT segment:
			// remember this message so NextWalSegment can replay its remainder there.
			if copiedBytes < len(xld.WALData[messageOffset:]) {
				seg.lastMsg = &message
			}
		}
	case *pgproto3.CopyDone:
		// Server ended the stream (timeline switch) — no more WAL on this connection's copy.
		return ProcessMessageCopyDone, nil
	default:
		return ProcessMessageUnknown, segmentError{errors.Errorf("Received unexpected message: %#v\n", msg)}
	}
	return ProcessMessageOK, nil
}

// Stream is a helper function to retrieve messages from Postgres and have them processed by processMessage().
func (seg *WalSegment) Stream(conn *pgconn.PgConn, standbyMessageTimeout time.Duration) (ProcessMessageResult, error) {
	// Inspired by https://github.com/jackc/pglogrepl/blob/master/example/pglogrepl_demo/main.go
	// And https://www.postgresql.org/docs/12/protocol-replication.html

	var err error
	var msg pgproto3.BackendMessage
	nextStandbyMessageDeadline := time.Now() // due immediately so we send a status update on entry.
	for {
		// (1) KEEPALIVE / STATUS REPLY. At most every `standbyMessageTimeout` (10s), tell the
		// primary where we are so it doesn't drop us and so it can advance the slot.
		//
		// >>> THE KEY BASELINE LIMITATION <<<
		// We advertise WALWritePosition = seg.StartLSN — the START of the file we're CURRENTLY
		// filling, i.e. a position we passed long ago, never the bytes we just received. We send
		// no write/flush/apply LSN reflecting real progress, and we never fsync received bytes.
		// So Postgres cannot use this receiver for synchronous_commit, and the slot only advances
		// a whole segment at a time. (The sync-standby fork replaces this with a real, fsync'd
		// flush_lsn ACK — that single change is what turns this archiver into a durable standby.)
		if time.Now().After(nextStandbyMessageDeadline) {
			err = pglogrepl.SendStandbyStatusUpdate(context.Background(),
				conn,
				pglogrepl.StandbyStatusUpdate{WALWritePosition: seg.StartLSN})
			tracelog.ErrorLogger.FatalOnError(err)
			tracelog.DebugLogger.Println("Sent Standby status message")
			nextStandbyMessageDeadline = time.Now().Add(standbyMessageTimeout)
		}

		// (2) RECEIVE one message, but no later than the next keepalive deadline. A timeout here
		// just means "no message arrived in time" → loop back so step (1) can send the keepalive.
		ctx, cancel := context.WithDeadline(context.Background(), nextStandbyMessageDeadline)
		msg, err = conn.ReceiveMessage(ctx)
		cancel()
		if pgconn.Timeout(err) {
			continue
		}
		tracelog.ErrorLogger.FatalOnError(err) // any non-timeout error is fatal.

		// (3) PROCESS the message into the segment buffer, then act on the outcome.
		result, err := seg.processMessage(msg)
		switch result {
		case ProcessMessageOK:
			// Bytes copied. Return only once the WHOLE 16 MiB file is filled; otherwise keep
			// looping to receive more. (The handler then uploads + rotates.)
			if seg.isComplete() {
				return ProcessMessageOK, nil
			}
		case ProcessMessageUnknown:
			return result, err
		case ProcessMessageCopyDone:
			// Timeline switch: acknowledge the server's CopyDone and return so the handler can
			// upload this `.partial` and resume on the next timeline.
			cdr, err := pglogrepl.SendStandbyCopyDone(context.Background(), conn)
			tracelog.ErrorLogger.FatalOnError(err)
			tracelog.DebugLogger.Printf("CopyDoneResult => %v", cdr)
			return result, nil
		case ProcessMessageReplyRequested:
			// The primary asked for a prompt status reply. If the segment happens to be complete,
			// return as OK; otherwise zero the deadline so step (1) fires a status update on the
			// very next iteration.
			if seg.isComplete() {
				return ProcessMessageOK, nil
			}
			nextStandbyMessageDeadline = time.Time{}
		case ProcessMessageSegmentGap:
			return result, err // WAL gap → fatal in the handler (stream is inconsistent).
		case ProcessMessageMismatch:
			return result, err
		default:
			tracelog.DebugLogger.Printf("Unexpected processMessage result => %v", result)
			return result, err
		}
	}
}

// isComplete returns true once the receive cursor has reached the file's end LSN, i.e. the
// whole 16 MiB segment has been received and it can be uploaded + rotated.
func (seg *WalSegment) isComplete() bool {
	return seg.StartLSN+pglogrepl.LSN(seg.writeIndex) >= seg.endLSN
}

// Read makes WalSegment an io.Reader so the upload side (WalUploader.UploadWalFile) can stream the
// buffered bytes straight into the storage object — compressing/encrypting on the way, exactly
// like wal-push of a normal WAL file. It hands out `data` from readIndex and reports io.EOF at the
// end. NOTE: it reads the entire backing array (the full `walSegmentBytes`); for a `.partial`
// (timeline switch) that means the unfilled tail is uploaded as the zero-fill it was allocated with.
func (seg *WalSegment) Read(p []byte) (n int, err error) {
	n = copy(p, seg.data[seg.readIndex:])
	seg.readIndex += n
	if len(seg.data) <= seg.readIndex {
		return n, io.EOF
	}
	return n, nil
}

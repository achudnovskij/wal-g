package postgres

import (
	"context"
	"encoding/json"
	"net/http"
	"os"
	"strconv"
	"sync"
	"time"

	"github.com/jackc/pglogrepl"
	"github.com/wal-g/tracelog"
)

// Control-plane-orchestrated PRIMARY FAILOVER (Option B, extended).
//
// Problem this solves: when the primary fails over, the receiver historically
// had to be re-pointed at the new primary by the operator ROLLING the pod (a
// process restart), which resets the in-memory fsync frontier (HighestFsyncdLSN)
// to 0/0. A 0/0 frontier makes prepare_promote distrust the receiver and fall
// back to a stale cache -> the recv-down promote wedge. Separately, in true
// total loss nobody is left to ask the receiver to flush its un-archived tail to
// S3, so a rebuilt standby can only reach the S3 archive watermark (RPO>0).
//
// failover-primary fixes both WITHOUT a restart:
//  1. flush the un-archived tail to S3 (durable independent of any live PG node),
//     returning the flushed LSN so the control plane knows the tail is safe
//     BEFORE it promotes;
//  2. re-point the streaming source at the newly-promoted primary by rewriting
//     the libpq PGHOST/PGPORT the next reconnect reads, and waking the reconnect
//     loop so it re-targets immediately.
//
// The process stays alive throughout, so HighestFsyncdLSN is preserved and the
// promote fastpath can trust the receiver's live frontier.

// failoverReconnect wakes HandleWALReceive's backoff so a freshly re-targeted
// receiver reconnects to the new primary immediately instead of waiting out the
// (up to 60s) backoff. Buffered (cap 1) + non-blocking send: a signal that
// arrives mid-stream is coalesced and consumed at the next reconnect wait.
var failoverReconnect = make(chan struct{}, 1)

type failoverPrimaryRequest struct {
	NewPrimary struct {
		Host string `json:"host"`
		Port string `json:"port"`
	} `json:"newPrimary"`
	// FromLSN is advisory; the flush always covers everything the receiver has
	// durably fsync'd (HighestFsyncdLSN), which is the authoritative gate.
	FromLSN string `json:"fromLsn"`
}

// controlFailoverPrimary handles POST /v1/failover-primary. It is synchronous
// and acknowledged: it returns only after the tail is durable in S3 and the
// new primary target is set, so the control plane can order the promote after.
func controlFailoverPrimary(w http.ResponseWriter, req *http.Request) {
	var body failoverPrimaryRequest
	if err := json.NewDecoder(req.Body).Decode(&body); err != nil {
		http.Error(w, "decode: "+err.Error(), http.StatusBadRequest)
		return
	}
	if body.NewPrimary.Host == "" {
		http.Error(w, "missing newPrimary.host", http.StatusBadRequest)
		return
	}

	// 1. Flush the un-archived tail to S3 (<prefix>/dr-tail) so it survives even
	//    total loss of both PG nodes. Synchronous; the flushed LSN is returned.
	flushed := HighestFsyncdLSN()
	segs := 0
	if drS3Enabled() {
		n, durable, err := uploadDRTailToS3(req.Context(), flushed)
		if err != nil {
			http.Error(w, "failover-primary s3 flush: "+err.Error(), http.StatusInternalServerError)
			return
		}
		segs = n
		// Report the DURABLE+CONTIGUOUS gate (== the raw fsync frontier, capped at
		// the longest hole-free uploaded run). The tail is shipped untrimmed; the
		// candidate's Postgres recovery stops at the last CRC-valid record on its
		// own, so the raw frontier no longer wedges the promote — the CP asserts
		// replay reached this gate. See doc/walg-postgres-side-recovery-design.md.
		flushed = durable
	} else {
		tracelog.WarningLogger.Printf("wal-receive-control: failover-primary with S3 dr-tail disabled; " +
			"re-targeting only, the un-archived tail is NOT made durable (set WALG_WAL_RECEIVE_DR_S3)")
	}

	// 2. Re-target the stream to the new primary without restarting the process,
	//    preserving HighestFsyncdLSN.
	retargetPrimary(body.NewPrimary.Host, body.NewPrimary.Port)

	tracelog.InfoLogger.Printf("wal-receive-control: failover-primary -> %s:%s; flushed %d tail object(s) through %s",
		body.NewPrimary.Host, body.NewPrimary.Port, segs, flushed)
	writeControlJSON(w, http.StatusOK, drCatchupResponse{PushedThroughLSN: flushed.String(), Segments: segs})
}

// lastDRTailFlushLSN tracks the fsync frontier the autonomous dr-tail flush last
// made durable in S3, so neither a sustained primary outage (reconnect-loop
// flushes) nor the continuous flusher's steady-state ticks re-PUT the same tail.
// Guarded by lastDRTailFlushMu because it is now touched from TWO goroutines: the
// HandleWALReceive reconnect loop (flushDRTailOnPrimaryLoss) and the continuous
// dr-tail flusher (runContinuousDRTailFlusher).
var (
	lastDRTailFlushLSN pglogrepl.LSN
	lastDRTailFlushMu  sync.Mutex
)

// flushDRTailDurable uploads the receiver's retained un-archived tail (completed
// segments + the in-flight partial) to S3's dr-tail prefix, de-duplicated against
// lastDRTailFlushLSN under lastDRTailFlushMu so concurrent callers (the continuous
// flusher and the primary-loss flush) never double-PUT the same tail or race the
// watermark. `reason` only colors the log line. Returns the number of objects
// written and the durable+contiguous gate uploadDRTailToS3 verified.
//
// The de-dup watermark is advanced ONLY to the DURABLE gate, never the raw fsync
// frontier: uploadDRTailToS3 reports only the contiguous, verifiably-uploaded tail,
// so if a PUT failed (S3 blip) or the retained set had a hole, `durable` is BELOW
// the frontier — advancing the watermark to the raw frontier would suppress the
// retry that could complete the tail on the next tick, permanently leaving acked
// WAL un-durable (RPO>0). Advancing only to `durable` lets the next flush re-attempt
// the missing segments. Best-effort: a failure just logs.
func flushDRTailDurable(ctx context.Context, reason string) (n int, durable pglogrepl.LSN, ok bool) {
	if !drS3Enabled() {
		return 0, 0, false
	}
	lastDRTailFlushMu.Lock()
	defer lastDRTailFlushMu.Unlock()

	frontier := HighestFsyncdLSN()
	if frontier <= lastDRTailFlushLSN {
		return 0, lastDRTailFlushLSN, true // nothing new fsync'd since the last flush
	}
	n, durable, err := uploadDRTailToS3(ctx, frontier)
	if err != nil {
		tracelog.WarningLogger.Printf("wal-receive: dr-tail flush (%s) failed: %v", reason, err)
		return 0, lastDRTailFlushLSN, false
	}
	if durable > lastDRTailFlushLSN {
		lastDRTailFlushLSN = durable
	}
	if durable < frontier {
		tracelog.WarningLogger.Printf("wal-receive: dr-tail flush (%s) wrote %d object(s) but only made WAL "+
			"durable through %s (raw frontier %s); the tail is INCOMPLETE in S3 and will be retried on the next "+
			"tick — do NOT gate a promote past %s", reason, n, durable, frontier, durable)
		return n, durable, true
	}
	if n > 0 {
		tracelog.InfoLogger.Printf("wal-receive: dr-tail flush (%s) wrote %d object(s) "+
			"(raw frontier %s, durable gate %s)", reason, n, frontier, durable)
	}
	return n, durable, true
}

// flushDRTailOnPrimaryLoss makes the receiver's un-archived tail durable in S3's
// dr-tail prefix the moment the primary connection is lost. This is the documented
// "upload the frozen partial when the receiver loses the primary connection"
// behavior (doc/walg-receiver-s3-dr-delivery.md, "The trailing partial") and a
// backstop on the total-loss durability guarantee: when BOTH PG nodes are gone,
// prepare_promote never runs, so the control-plane dr-catchup / failover-primary
// flush triggers never fire. The continuous flusher (runContinuousDRTailFlusher)
// keeps S3 within one tick of the ACK during steady streaming; this final flush at
// the stream drop captures whatever the last tick missed. Idempotent S3 upload, so
// safe to run autonomously even under control orchestration.
func flushDRTailOnPrimaryLoss(ctx context.Context) {
	flushDRTailDurable(ctx, "primary loss")
}

// ContinuousDRTailIntervalEnv overrides how often the continuous dr-tail flusher
// uploads the un-archived tail to S3 during steady streaming. Defaults to 2s. Set
// to 0 to disable the continuous flusher (falling back to flush-on-primary-loss
// only — the pre-continuous behavior, which leaves an unbounded RPO window on
// total loss).
const ContinuousDRTailIntervalEnv = "WALG_WAL_RECEIVE_DR_S3_INTERVAL_SECONDS"

func continuousDRTailInterval() time.Duration {
	v := os.Getenv(ContinuousDRTailIntervalEnv)
	if v == "" {
		return 2 * time.Second
	}
	n, err := strconv.Atoi(v)
	if err != nil || n < 0 {
		tracelog.WarningLogger.Printf("%s=%q invalid, using 2s", ContinuousDRTailIntervalEnv, v)
		return 2 * time.Second
	}
	return time.Duration(n) * time.Second
}

// runContinuousDRTailFlusher runs as a goroutine for the lifetime of wal-receive.
// On each tick it uploads the un-archived tail to S3's dr-tail prefix so the dr-tail
// store keeps pace with the flush_lsn ACK the receiver sends the primary.
//
// WHY THIS EXISTS (the total-loss RPO=0 gap). The receiver ACKs flush_lsn the moment
// it fsyncs a drain batch locally (syncPartial, every few ms), so under sync-standby
// the primary commits as soon as the receiver's LOCAL disk has the WAL. But before
// this flusher the dr-tail S3 upload fired ONLY on primary-loss / control request, so
// during steady streaming S3 lagged the ACK arbitrarily far. On TRUE total loss
// (both PG nodes die at once) the candidate recovers ONLY from S3 (restore_command
// reads <prefix>/dr-tail then the archive), so any acked-but-not-yet-uploaded WAL is
// unrecoverable — RPO>0. The single flush-on-primary-loss is best-effort and, at
// exactly the moment of a crash, can miss the most-recently-acked segment(s).
//
// Bounding the upload to one tick behind the ACK bounds the worst-case total-loss
// RPO to one interval's worth of WAL (default 2s) instead of an unbounded backlog.
// For STRICT RPO=0 the control plane must additionally gate the ACK on dr-tail
// durability (see doc) — this flusher makes that gate cheap by keeping S3 hot.
func runContinuousDRTailFlusher(ctx context.Context) {
	if !drS3Enabled() {
		return
	}
	interval := continuousDRTailInterval()
	if interval == 0 {
		tracelog.InfoLogger.Printf("wal-receive: continuous dr-tail flusher disabled via %s=0 "+
			"(dr-tail uploads only on primary loss / control request — unbounded total-loss RPO window)",
			ContinuousDRTailIntervalEnv)
		return
	}
	tracelog.InfoLogger.Printf("wal-receive: continuous dr-tail flusher every %v (keeps S3 dr-tail within one "+
		"tick of the flush_lsn ACK for total-loss recoverability)", interval)
	t := time.NewTicker(interval)
	defer t.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-t.C:
		}
		flushDRTailDurable(ctx, "continuous")
	}
}

// retargetPrimary rewrites the libpq env the next replication reconnect reads
// and wakes the reconnect loop. os.Setenv is goroutine-safe; receiveOnce reads
// PGHOST/PGPORT at connect time, so the next reconnect (triggered by the dying
// old primary's stream drop, or the signal below) targets the new primary.
func retargetPrimary(host, port string) {
	if host != "" {
		_ = os.Setenv("PGHOST", host)
	}
	if port != "" {
		_ = os.Setenv("PGPORT", port)
	}
	select {
	case failoverReconnect <- struct{}{}:
	default:
	}
}

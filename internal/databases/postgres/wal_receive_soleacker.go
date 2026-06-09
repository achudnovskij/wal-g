package postgres

import (
	"context"
	"os"
	"strconv"
	"sync/atomic"
	"time"

	"github.com/jackc/pglogrepl"
	"github.com/jackc/pgx/v5"
	"github.com/wal-g/tracelog"
	"github.com/wal-g/wal-g/internal"
)

// PeerPollIntervalEnv controls how often the receiver polls the primary to
// learn whether another standby is currently streaming. When no other standby
// is active, the receiver is the sole sync acker on every commit's critical
// path, so it switches from drain-batched fsync (low IOPS, up to drainMaxWindow
// of added commit latency) to per-message fsync (lowest commit latency). When a
// peer reappears it switches back to drain-batching to recover the IOPS savings.
//
// Default 500ms — worst-case detection lag for a peer going away is one
// interval. The poll is an in-memory pg_replication_slots read; at 500ms that
// is ~2 trivial queries/sec plus one idle backend connection. Set to 0 to
// disable the poller, in which case drain mode follows
// WALG_WAL_RECEIVE_DRAIN_BATCHING statically.
const PeerPollIntervalEnv = "WALG_WAL_RECEIVE_PEER_POLL_INTERVAL_MS"

const defaultPeerPollInterval = 500 * time.Millisecond

// PeerStalenessWindowEnv controls how long a peer's acked LSN may remain frozen
// — WHILE the receiver itself is still durably advancing new WAL — before we
// treat the peer as effectively gone (a partitioned or hung standby whose slot
// is still active=true because Postgres has not yet hit wal_sender_timeout).
//
// This is the key lever that decouples sole-acker detection from
// wal_sender_timeout (default ~60s). A partitioned standby's slot stays active
// for up to wal_sender_timeout, so the plain active-count check keeps reporting
// "peer present" the whole time, leaving the receiver in drain-batched mode as
// the unknowing sole acker. The staleness check fires within
// stalenessWindow + one poll interval instead.
//
// Default 1500ms: long enough that ordinary scheduling/network jitter on a
// healthy busy standby (which re-acks every few hundred ms under load) never
// trips it, short enough to bound the degradation window to ~1-2s.
const PeerStalenessWindowEnv = "WALG_WAL_RECEIVE_PEER_STALENESS_MS"

const defaultPeerStalenessWindow = 1500 * time.Millisecond

// backToBatchConfirmations is how many consecutive "peer present" polls we
// require before switching back from per-message to drain-batched. Switching TO
// per-message is eager (a single "no peer" poll), switching back is lazy, so a
// peer slot that briefly flickers active->inactive->active does not thrash the
// fsync mode. At the 500ms default, 3 confirmations ≈ 1.5s of stable peer
// presence before we re-enable batching.
const backToBatchConfirmations = 3

// soleAcker is true when the receiver believes it is the only active standby
// (no peer to co-satisfy the sync quorum). Read by Stream() on every batch via
// isSoleAcker(); written only by runSoleAckerPoller. Starts false (assume a
// peer exists) so a fresh receiver does not default to per-message before its
// first poll — the steady state with a healthy standby is drain-batched.
var soleAcker atomic.Bool

// isSoleAcker reports the latest poller verdict. Cheap atomic load; safe to
// call on the Stream() hot path.
func isSoleAcker() bool { return soleAcker.Load() }

func peerPollInterval() time.Duration {
	return msEnvDuration(PeerPollIntervalEnv, defaultPeerPollInterval)
}

func peerStalenessWindow() time.Duration {
	return msEnvDuration(PeerStalenessWindowEnv, defaultPeerStalenessWindow)
}

func msEnvDuration(env string, def time.Duration) time.Duration {
	v := os.Getenv(env)
	if v == "" {
		return def
	}
	n, err := strconv.Atoi(v)
	if err != nil || n < 0 {
		tracelog.WarningLogger.Printf("%s=%q invalid, using %v", env, v, def)
		return def
	}
	return time.Duration(n) * time.Millisecond
}

// runSoleAckerPoller runs for the lifetime of wal-receive. It maintains one
// long-lived SQL connection to the primary and, every peerPollInterval, checks
// whether any other physical replication slot is active. The result drives the
// soleAcker flag that Stream() consults to choose per-message vs drain-batched
// fsync.
//
// Fail-safe: any poll/connection error is treated as "assume sole acker" — we
// favor low commit latency (and slightly higher IOPS) over the risk of leaving
// the receiver in drain-batched mode while it is unknowingly the deciding acker.
func runSoleAckerPoller(ctx context.Context) {
	interval := peerPollInterval()
	if interval == 0 {
		tracelog.InfoLogger.Printf("wal-receive: sole-acker poller disabled via %s=0; drain mode is static (%s)",
			PeerPollIntervalEnv, DrainBatchingEnv)
		return
	}
	staleness := peerStalenessWindow()
	selfSlot := internal.GetPgSlotName()
	tracelog.InfoLogger.Printf("wal-receive: sole-acker poller every %v, peer-staleness window %v (self slot %q)",
		interval, staleness, selfSlot)

	t := time.NewTicker(interval)
	defer t.Stop()

	var conn *pgx.Conn
	defer func() {
		if conn != nil {
			_ = conn.Close(context.Background())
		}
	}()

	tracker := &peerLivenessTracker{staleness: staleness}
	peerPresentStreak := 0
	for {
		select {
		case <-ctx.Done():
			return
		case <-t.C:
		}

		active, peerLSN, err := pollPeer(ctx, &conn, selfSlot)
		if err != nil {
			peerPresentStreak = 0
			tracker.reset()
			if soleAcker.CompareAndSwap(false, true) {
				tracelog.WarningLogger.Printf("wal-receive: sole-acker poll failed (%v); assuming sole acker -> per-message fsync", err)
			}
			continue
		}

		// A peer counts as a usable co-acker only if its slot is active AND it
		// is keeping pace. The staleness evaluation needs the receiver's own
		// durable frontier so it can tell "peer frozen because no WAL is
		// flowing" (idle, peer is fine) apart from "peer frozen while WAL IS
		// flowing" (partitioned/hung peer).
		hasUsablePeer := tracker.observe(active, peerLSN, HighestFsyncdLSN(), time.Now())

		if hasUsablePeer {
			peerPresentStreak++
			if peerPresentStreak >= backToBatchConfirmations && soleAcker.CompareAndSwap(true, false) {
				tracelog.InfoLogger.Printf("wal-receive: peer standby keeping pace -> drain-batched fsync")
			}
		} else {
			peerPresentStreak = 0
			if soleAcker.CompareAndSwap(false, true) {
				if active {
					tracelog.InfoLogger.Printf("wal-receive: peer standby slot active but acked LSN frozen for >%v while WAL advancing (partitioned/hung) -> per-message fsync (sole sync acker)", staleness)
				} else {
					tracelog.InfoLogger.Printf("wal-receive: no peer standby active -> per-message fsync (sole sync acker)")
				}
			}
		}
	}
}

// peerLivenessTracker decides, across successive polls, whether the active peer
// standby is actually keeping pace or has silently frozen (partitioned/hung
// slot still marked active=true by the primary until wal_sender_timeout).
//
// It is "eager to demote, lazy to credit" only via the surrounding streak
// logic; this struct just answers the single question "is the peer a usable
// co-acker right now?" using LSN staleness gated on receiver progress:
//
//   - No active peer slot                          -> not usable.
//   - Active peer whose LSN advanced since last obs -> usable, clock reset.
//   - Active peer whose LSN is frozen, BUT the receiver's own durable frontier
//     has NOT advanced either                       -> usable (idle system; the
//     peer SHOULD be flat, so flatness is not evidence of a dead peer).
//   - Active peer whose LSN is frozen WHILE the receiver's frontier advanced,
//     for at least `staleness`                       -> NOT usable (the peer
//     should be moving but isn't -> treat as gone).
//
// peerRecoveryAdvances is how many consecutive polls a peer that we have ALREADY
// demoted (declared frozen/gone) must show fresh LSN advancement before we trust
// it again as a co-acker. A demoted standby is one we positively believe is
// partitioned/hung; the partition teardown (TCP buffers draining, a one-off
// delayed feedback packet) can make its slot restart_lsn jump forward ONCE even
// while it is still cut off. Crediting it on that single jump would flip us back
// to drain-batched for a full wal_sender_timeout window before the slot finally
// goes inactive — the exact degradation we are trying to kill. Requiring N
// consecutive advances filters that one-shot artifact: a truly recovered standby
// streams continuously and clears this easily, a still-partitioned one cannot.
const peerRecoveryAdvances = 3

type peerLivenessTracker struct {
	staleness time.Duration

	have          bool          // have a baseline observation
	lastPeerLSN   pglogrepl.LSN // peer's acked LSN at last observation
	lastSelfLSN   pglogrepl.LSN // receiver's durable frontier at last observation
	frozenSince   time.Time     // when peerLSN first stopped advancing (while self advanced)
	frozenPending bool          // peer LSN currently frozen-while-self-advancing
	demoted       bool          // we have declared the peer gone; require sustained recovery
	recoverStreak int           // consecutive fresh-advance polls while demoted
}

func (p *peerLivenessTracker) reset() {
	p.have = false
	p.frozenPending = false
	p.demoted = false
	p.recoverStreak = 0
}

// observe folds one poll sample into the tracker and returns whether the peer
// is a usable co-acker. now is passed in for testability.
func (p *peerLivenessTracker) observe(active bool, peerLSN, selfLSN pglogrepl.LSN, now time.Time) bool {
	if !active {
		p.reset()
		return false
	}

	if !p.have {
		// First observation of an active peer: establish baselines, credit it.
		p.have = true
		p.lastPeerLSN = peerLSN
		p.lastSelfLSN = selfLSN
		p.frozenPending = false
		p.demoted = false
		p.recoverStreak = 0
		return true
	}

	peerAdvanced := peerLSN > p.lastPeerLSN
	selfAdvanced := selfLSN > p.lastSelfLSN
	prevPeerLSN := p.lastPeerLSN
	p.lastPeerLSN = peerLSN
	p.lastSelfLSN = selfLSN

	// Once demoted, demand SUSTAINED recovery before re-crediting: the peer must
	// advance on consecutive polls (a single restart_lsn jump from partition
	// teardown is not enough). Until then it stays unusable.
	if p.demoted {
		if peerLSN > prevPeerLSN {
			p.recoverStreak++
			if p.recoverStreak >= peerRecoveryAdvances {
				p.demoted = false
				p.frozenPending = false
				p.recoverStreak = 0
				return true // sustained recovery -> trust the peer again
			}
		} else {
			p.recoverStreak = 0 // any stall resets the recovery streak
		}
		return false // still proving itself; keep us in sole-acker mode
	}

	if peerAdvanced {
		// Peer is making progress: healthy co-acker, clear any freeze clock.
		p.frozenPending = false
		return true
	}

	// Peer LSN did not advance this interval.
	if !selfAdvanced {
		// The whole system is idle (we received no new durable WAL either), so
		// the peer being flat is expected, not a fault. Do not start/extend the
		// freeze clock; keep crediting the peer.
		return true
	}

	// Self advanced but peer did not: the peer SHOULD have moved but didn't.
	if !p.frozenPending {
		p.frozenPending = true
		p.frozenSince = now
	}
	if now.Sub(p.frozenSince) >= p.staleness {
		p.demoted = true // declare gone; recovery now requires sustained advance
		p.recoverStreak = 0
		return false
	}
	return true // within grace window; still credit it
}

// pollPeer runs one peer check, lazily (re)establishing the long-lived
// connection. On any error it drops the connection so the next tick reconnects,
// and returns the error to the caller (which fail-safes to sole-acker). It
// returns whether a peer slot is active and that peer's furthest acked LSN.
func pollPeer(ctx context.Context, conn **pgx.Conn, selfSlot string) (bool, pglogrepl.LSN, error) {
	if *conn == nil {
		c, err := ConnectWithTimeout(10 * time.Second)
		if err != nil {
			return false, 0, err
		}
		*conn = c
	}
	qr, err := NewPgQueryRunner(*conn)
	if err != nil {
		_ = (*conn).Close(ctx)
		*conn = nil
		return false, 0, err
	}
	active, progress, err := qr.OtherActiveStandbyProgress(selfSlot)
	if err != nil {
		_ = (*conn).Close(ctx)
		*conn = nil
		return false, 0, err
	}
	return active, progress, nil
}

package postgres

import (
	"testing"
	"time"

	"github.com/jackc/pglogrepl"
)

// lsn is a tiny helper so the table tests read in byte offsets.
func lsn(n uint64) pglogrepl.LSN { return pglogrepl.LSN(n) }

// TestPeerLivenessNoActivePeerIsNotUsable: with no active peer slot, the peer is
// never a usable co-acker regardless of LSNs.
func TestPeerLivenessNoActivePeerIsNotUsable(t *testing.T) {
	p := &peerLivenessTracker{staleness: 1500 * time.Millisecond}
	if p.observe(false, 0, 0, time.Now()) {
		t.Fatal("inactive peer must not be usable")
	}
}

// TestPeerLivenessHealthyPeerStaysUsable: an active peer whose LSN keeps
// advancing alongside the receiver's frontier is always a usable co-acker and
// never trips the staleness clock.
func TestPeerLivenessHealthyPeerStaysUsable(t *testing.T) {
	p := &peerLivenessTracker{staleness: 1500 * time.Millisecond}
	base := time.Now()
	peer, self := uint64(1000), uint64(1000)
	for i := 0; i < 20; i++ {
		now := base.Add(time.Duration(i) * 500 * time.Millisecond)
		peer += 4096
		self += 4096
		if !p.observe(true, lsn(peer), lsn(self), now) {
			t.Fatalf("healthy advancing peer marked unusable at iter %d", i)
		}
	}
	if p.frozenPending {
		t.Fatal("freeze clock should never engage for an advancing peer")
	}
}

// TestPeerLivenessIdleSystemNoFalseFlip: an active peer whose LSN is flat
// BECAUSE the receiver is also idle (no new durable WAL) must stay usable — a
// quiescent-but-healthy standby must not be misdetected as gone.
func TestPeerLivenessIdleSystemNoFalseFlip(t *testing.T) {
	p := &peerLivenessTracker{staleness: 1500 * time.Millisecond}
	base := time.Now()
	const peer, self = uint64(5000), uint64(5000)
	if !p.observe(true, lsn(peer), lsn(self), base) {
		t.Fatal("baseline observation should credit the peer")
	}
	// 30s of total idle: both peer and self frozen. Never flip.
	for i := 1; i <= 60; i++ {
		now := base.Add(time.Duration(i) * 500 * time.Millisecond)
		if !p.observe(true, lsn(peer), lsn(self), now) {
			t.Fatalf("idle healthy peer wrongly marked unusable at %v", now.Sub(base))
		}
	}
	if p.frozenPending {
		t.Fatal("freeze clock must not engage when the system is idle")
	}
}

// TestPeerLivenessFrozenPeerWhileReceiverAdvancesFlips: the core fix — peer slot
// stays active=true but its acked LSN freezes while the receiver keeps durably
// advancing WAL. The peer must be demoted within ~staleness, NOT held usable
// until wal_sender_timeout.
func TestPeerLivenessFrozenPeerWhileReceiverAdvancesFlips(t *testing.T) {
	staleness := 1500 * time.Millisecond
	p := &peerLivenessTracker{staleness: staleness}
	base := time.Now()
	peer, self := uint64(8000), uint64(8000)

	// Baseline + one healthy advance.
	if !p.observe(true, lsn(peer), lsn(self), base) {
		t.Fatal("baseline should credit peer")
	}
	peer += 4096
	self += 4096
	if !p.observe(true, lsn(peer), lsn(self), base.Add(500*time.Millisecond)) {
		t.Fatal("advancing peer should be usable")
	}

	// Partition: peer LSN frozen, receiver keeps advancing every 500ms.
	frozenPeer := peer
	flipAt := time.Duration(0)
	for i := 2; i <= 12; i++ {
		now := base.Add(time.Duration(i) * 500 * time.Millisecond)
		self += 4096
		usable := p.observe(true, lsn(frozenPeer), lsn(self), now)
		if !usable && flipAt == 0 {
			flipAt = now.Sub(base.Add(500 * time.Millisecond))
		}
	}
	if flipAt == 0 {
		t.Fatal("frozen peer was never demoted to sole-acker")
	}
	// First freeze sample is at +1000ms (relative to last-advance at +500ms);
	// it must flip no earlier than `staleness` after freeze began and within
	// one extra poll interval of it.
	if flipAt < staleness {
		t.Fatalf("flipped too early: %v < staleness %v", flipAt, staleness)
	}
	if flipAt > staleness+700*time.Millisecond {
		t.Fatalf("flipped too late: %v > staleness+1poll", flipAt)
	}
}

// TestPeerLivenessFreezeThenRecover: a peer that freezes briefly (below the
// staleness window) and then resumes advancing must be re-credited and the
// freeze clock cleared — no demotion.
func TestPeerLivenessFreezeThenRecover(t *testing.T) {
	p := &peerLivenessTracker{staleness: 1500 * time.Millisecond}
	base := time.Now()
	peer, self := uint64(2000), uint64(2000)
	p.observe(true, lsn(peer), lsn(self), base)

	// One frozen sample (self advances) at +500ms — within grace.
	self += 4096
	if !p.observe(true, lsn(peer), lsn(self), base.Add(500*time.Millisecond)) {
		t.Fatal("brief freeze inside grace must stay usable")
	}
	if !p.frozenPending {
		t.Fatal("freeze clock should have engaged")
	}
	// Peer resumes at +1000ms.
	peer += 4096
	self += 4096
	if !p.observe(true, lsn(peer), lsn(self), base.Add(1000*time.Millisecond)) {
		t.Fatal("recovered peer must be usable")
	}
	if p.frozenPending {
		t.Fatal("freeze clock must clear once peer advances again")
	}
}

// TestPeerLivenessDemotedSingleJumpDoesNotRecredit reproduces the observed
// partition-teardown artifact: after we demote a frozen peer, its slot
// restart_lsn makes ONE spurious forward jump (a delayed/buffered feedback
// landing) and then re-freezes while it is still partitioned. A single jump must
// NOT flip us back to drain-batched; we must stay in sole-acker mode.
func TestPeerLivenessDemotedSingleJumpDoesNotRecredit(t *testing.T) {
	staleness := 1500 * time.Millisecond
	p := &peerLivenessTracker{staleness: staleness}
	base := time.Now()
	peer, self := uint64(8000), uint64(8000)
	p.observe(true, lsn(peer), lsn(self), base)

	// Drive the peer to demotion: frozen while self advances past staleness.
	frozenPeer := peer
	demoted := false
	for i := 1; i <= 6; i++ {
		self += 4096
		if !p.observe(true, lsn(frozenPeer), lsn(self), base.Add(time.Duration(i)*500*time.Millisecond)) {
			demoted = true
			break
		}
	}
	if !demoted || !p.demoted {
		t.Fatal("peer should be demoted after sustained freeze")
	}

	// ONE spurious advance, then re-freeze for many polls.
	tk := 7
	self += 4096
	frozenPeer += 4096 // the single jump
	if p.observe(true, lsn(frozenPeer), lsn(self), base.Add(time.Duration(tk)*500*time.Millisecond)) {
		t.Fatal("a single post-demotion jump must NOT re-credit the peer")
	}
	for i := 8; i <= 20; i++ {
		self += 4096 // self keeps advancing; peer stays frozen at the jumped value
		if p.observe(true, lsn(frozenPeer), lsn(self), base.Add(time.Duration(i)*500*time.Millisecond)) {
			t.Fatalf("still-partitioned peer re-credited on poll %d after one jump", i)
		}
	}
	if !p.demoted {
		t.Fatal("peer must remain demoted after a single jump + re-freeze")
	}
}

// TestPeerLivenessDemotedSustainedRecoveryRecredits: a genuinely recovered
// standby streams continuously, so its LSN advances on consecutive polls. After
// peerRecoveryAdvances such polls the tracker re-credits it (-> flip back to
// drain-batched).
func TestPeerLivenessDemotedSustainedRecoveryRecredits(t *testing.T) {
	staleness := 1500 * time.Millisecond
	p := &peerLivenessTracker{staleness: staleness}
	base := time.Now()
	peer, self := uint64(3000), uint64(3000)
	p.observe(true, lsn(peer), lsn(self), base)

	frozenPeer := peer
	for i := 1; i <= 6; i++ {
		self += 4096
		p.observe(true, lsn(frozenPeer), lsn(self), base.Add(time.Duration(i)*500*time.Millisecond))
	}
	if !p.demoted {
		t.Fatal("precondition: peer must be demoted")
	}

	// Sustained recovery: peer advances every poll. First peerRecoveryAdvances-1
	// polls still return unusable (proving itself); the Nth re-credits.
	rp := frozenPeer
	tk := 7
	for k := 1; k <= peerRecoveryAdvances; k++ {
		self += 4096
		rp += 4096
		usable := p.observe(true, lsn(rp), lsn(self), base.Add(time.Duration(tk)*500*time.Millisecond))
		tk++
		if k < peerRecoveryAdvances && usable {
			t.Fatalf("re-credited too early at advance %d/%d", k, peerRecoveryAdvances)
		}
		if k == peerRecoveryAdvances && !usable {
			t.Fatalf("not re-credited after %d sustained advances", peerRecoveryAdvances)
		}
	}
	if p.demoted {
		t.Fatal("peer should no longer be demoted after sustained recovery")
	}
}

// TestPeerLivenessResetOnError: reset() drops the baseline so the next active
// observation re-establishes one (and credits the peer), matching the poll-error
// path.
func TestPeerLivenessResetOnError(t *testing.T) {
	p := &peerLivenessTracker{staleness: 1500 * time.Millisecond}
	base := time.Now()
	p.observe(true, lsn(100), lsn(100), base)
	p.reset()
	if p.have {
		t.Fatal("reset must clear baseline")
	}
	if !p.observe(true, lsn(50), lsn(50), base.Add(time.Second)) {
		t.Fatal("first observation after reset should credit the active peer")
	}
}

package postgres

import (
	"context"
	"crypto/tls"
	"crypto/x509"
	"encoding/json"
	"errors"
	"net/http"
	"os"
	"time"

	"github.com/jackc/pglogrepl"
	"github.com/wal-g/tracelog"
)

// Receiver-side control HTTP/mTLS API for control-plane-orchestrated failover
// (Option B, doc/walg-receiver-control-channel-design.md). The control plane
// calls these on primary loss, BEFORE promoting a standby:
//
//	GET  /v1/status      -> {lastAcceptedLsn, partialDir}
//	POST /v1/dr-catchup  {fromLsn, toLsn}
//	     -> upload the receiver's buffered tail partials to the S3 dr-tail
//	        prefix (<WALG_S3_PREFIX>/dr-tail/) so the promotion candidate can
//	        wal-fetch the gap. Returns {pushedThroughLsn, segments}. Idempotent;
//	        409 if toLsn is beyond what the receiver has durably fsync'd.
//
// The control plane decides IF to catch up after comparing lastAcceptedLsn with
// the promotion candidate's replay LSN — so when the standby is already caught up
// (the common case) no upload happens at all.
const (
	WalReceiveControlTLSCertEnv  = "WALG_WAL_RECEIVE_CONTROL_TLS_CERT"
	WalReceiveControlTLSKeyEnv   = "WALG_WAL_RECEIVE_CONTROL_TLS_KEY"
	WalReceiveControlClientCAEnv = "WALG_WAL_RECEIVE_CONTROL_CLIENT_CA"

	// When set (e.g. ":8444"), the receiver runs the control server AND the
	// autonomous push-on-primary-loss is disabled (the control plane drives
	// catch-up via /v1/dr-catchup instead).
	WalReceiveControlListenEnv = "WALG_WAL_RECEIVE_CONTROL_LISTEN"
)

type drCatchupRequest struct {
	FromLSN string `json:"fromLsn"`
	ToLSN   string `json:"toLsn"`
}

type statusResponse struct {
	LastAcceptedLSN string `json:"lastAcceptedLsn"`
	PartialDir      string `json:"partialDir"`
}

type drCatchupResponse struct {
	PushedThroughLSN string `json:"pushedThroughLsn"`
	Segments         int    `json:"segments"`
}

// HandleWALReceiveControl runs the receiver-side control HTTP server (mTLS).
// Shares the mTLS config builder with wal-receive-serve.
func HandleWALReceiveControl(ctx context.Context, addr, certFile, keyFile, clientCAFile string) error {
	tlsConfig, err := buildServeTLSConfig(certFile, keyFile, clientCAFile)
	if err != nil {
		return err
	}
	srv := &http.Server{
		Addr:              addr,
		Handler:           controlMux(),
		TLSConfig:         tlsConfig,
		ReadHeaderTimeout: 10 * time.Second,
	}
	go func() {
		<-ctx.Done()
		shutdownCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		_ = srv.Shutdown(shutdownCtx)
	}()
	tracelog.InfoLogger.Printf("wal-receive-control: listening on %s (mTLS)", addr)
	if err := srv.ListenAndServeTLS("", ""); err != nil && !errors.Is(err, http.ErrServerClosed) {
		return err
	}
	return nil
}

// controlMux is extracted so tests can exercise the handlers without a socket.
func controlMux() *http.ServeMux {
	mux := http.NewServeMux()
	mux.HandleFunc("GET /v1/status", controlStatus)
	mux.HandleFunc("POST /v1/dr-catchup", controlDRCatchup)
	mux.HandleFunc("POST /v1/failover-primary", controlFailoverPrimary)
	return mux
}

func controlStatus(w http.ResponseWriter, _ *http.Request) {
	writeControlJSON(w, http.StatusOK, statusResponse{
		LastAcceptedLSN: HighestFsyncdLSN().String(),
		PartialDir:      walReceivePartialDir(),
	})
}

func controlDRCatchup(w http.ResponseWriter, req *http.Request) {
	var body drCatchupRequest
	if err := json.NewDecoder(req.Body).Decode(&body); err != nil {
		http.Error(w, "bad request body: "+err.Error(), http.StatusBadRequest)
		return
	}
	toLSN, err := pglogrepl.ParseLSN(body.ToLSN)
	if err != nil {
		http.Error(w, "bad toLsn: "+err.Error(), http.StatusBadRequest)
		return
	}
	// The receiver cannot promise more than it has durably fsync'd. A toLsn
	// beyond that is a control-plane bug; surface the real frontier so the CP
	// can recompute rather than gate the standby on an unreachable LSN.
	if have := HighestFsyncdLSN(); toLSN > have {
		writeControlJSON(w, http.StatusConflict, statusResponse{
			LastAcceptedLSN: have.String(), PartialDir: walReceivePartialDir(),
		})
		return
	}

	// DR-tail delivery is S3-only: upload the retained tail to
	// <WALG_S3_PREFIX>/dr-tail/ and let the candidate wal-fetch it. See
	// doc/walg-receiver-s3-dr-delivery.md.
	if !drS3Enabled() {
		http.Error(w, "dr-tail S3 not configured (WALG_WAL_RECEIVE_DR_S3 unset)", http.StatusInternalServerError)
		return
	}
	n, durable, uerr := uploadDRTailToS3(req.Context(), toLSN)
	if uerr != nil {
		http.Error(w, "dr-tail s3 upload: "+uerr.Error(), http.StatusInternalServerError)
		return
	}
	// Report the DURABLE+CONTIGUOUS gate (== the raw fsync frontier, capped at
	// the longest hole-free uploaded run), NOT a record-floored value. The tail
	// is shipped untrimmed; the candidate's Postgres recovery stops at the last
	// CRC-valid record on its own and the CP asserts replay reached this gate.
	// See doc/walg-postgres-side-recovery-design.md.
	tracelog.InfoLogger.Printf("wal-receive-control: dr-catchup via S3 wrote %d tail object(s), "+
		"requested gate %s, durable gate @ %s", n, toLSN, durable)
	writeControlJSON(w, http.StatusOK, drCatchupResponse{PushedThroughLSN: durable.String(), Segments: n})
}

// buildServeTLSConfig builds the mTLS server config used by the receiver
// control API: presents certFile/keyFile and requires + verifies a client
// cert chaining to clientCAFile.
func buildServeTLSConfig(certFile, keyFile, clientCAFile string) (*tls.Config, error) {
	if certFile == "" || keyFile == "" || clientCAFile == "" {
		return nil, errors.New("wal-receive-serve: TLS cert, key, and client-CA are all required (mTLS)")
	}
	cert, err := tls.LoadX509KeyPair(certFile, keyFile)
	if err != nil {
		return nil, err
	}
	caPEM, err := os.ReadFile(clientCAFile)
	if err != nil {
		return nil, err
	}
	pool := x509.NewCertPool()
	if !pool.AppendCertsFromPEM(caPEM) {
		return nil, errors.New("wal-receive-serve: no certs parsed from client CA file")
	}
	return &tls.Config{
		Certificates: []tls.Certificate{cert},
		ClientAuth:   tls.RequireAndVerifyClientCert,
		ClientCAs:    pool,
		MinVersion:   tls.VersionTLS12,
	}, nil
}

func writeControlJSON(w http.ResponseWriter, code int, v any) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(code)
	_ = json.NewEncoder(w).Encode(v)
}

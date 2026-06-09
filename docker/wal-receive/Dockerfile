# wal-g wal-receive container.
#
# Single-tenant by design: one running container streams from one primary
# Postgres and, on primary loss, pushes partials to one standby's
# wal-receive-serve over mTLS HTTP. Multi-tenancy is solved upstairs by
# running one pod per tenant (K8s namespace per tenant is the typical
# mapping).
#
# Build context = wal-g repo root, e.g.:
#   docker build -f docker/wal-receive/Dockerfile -t wal-g-receive:dev .

# Stage 1: build wal-g from vendored source. Pinned to the Go version
# the repo's go.mod declares; GOEXPERIMENT=jsonv2 matches the existing
# Makefile expectation.
FROM golang:1.25-bookworm AS builder

ENV GOEXPERIMENT=jsonv2
ENV GOTOOLCHAIN=local
ENV CGO_ENABLED=0

WORKDIR /src
COPY . /src

# Build only the pg binary; everything else is dead weight here.
RUN cd main/pg && \
    go build -mod vendor \
        -ldflags "-s -w -X github.com/wal-g/wal-g/cmd/pg.buildDate=$(date -u +%Y.%m.%d_%H:%M:%S) \
                  -X github.com/wal-g/wal-g/cmd/pg.gitRevision=container \
                  -X github.com/wal-g/wal-g/cmd/pg.walgVersion=container" \
        -o /out/wal-g .

# Stage 2: minimal runtime. The primary-loss push path delivers WAL to the
# standby's wal-receive-serve over mTLS HTTP (no SSH). ca-certificates is for
# libpq TLS to the primary.
FROM debian:bookworm-slim AS runtime

RUN apt-get update && \
    apt-get install --yes --no-install-recommends \
        ca-certificates && \
    rm -rf /var/lib/apt/lists/*

COPY --from=builder /out/wal-g /usr/bin/wal-g
COPY docker/wal-receive/entrypoint.sh /usr/local/bin/wal-receive-entrypoint
RUN chmod 0755 /usr/bin/wal-g /usr/local/bin/wal-receive-entrypoint

# Non-root by default. The mounted partial dir must be writable by this
# user — set fsGroup: 10001 on the K8s Pod or chown the volume.
RUN useradd --uid 10001 --user-group --create-home --shell /usr/sbin/nologin walg
USER walg

# Default working dir — partials live here. Volume-mount this from an
# instance-store-backed local PV (or emptyDir on i8g/i7i nodes).
WORKDIR /var/lib/walg/partials

ENTRYPOINT ["/usr/local/bin/wal-receive-entrypoint"]

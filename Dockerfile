# ── Stage 1: build ────────────────────────────────────────────────────────────
# Toolchain pinned to match go.mod (go 1.25.0).
FROM golang:1.25-alpine AS builder

# git is needed by go mod download for VCS-stamped modules; ca-certificates
# are copied into the runtime image for TLS outbound calls.
RUN apk add --no-cache git ca-certificates

WORKDIR /src

# Copy dependency manifests first so Docker layer-caches the module download
# step independently of source changes. .dockerignore keeps .env, data/ and
# VCS metadata out of the context entirely.
COPY go.mod go.sum ./
RUN go mod download

# Copy full source and build statically-linked binaries.
# TARGETARCH (a BuildKit builtin) replaces the hardcoded amd64 so the same
# Dockerfile builds native images on arm64 (Apple Silicon) and amd64 CI.
# -buildvcs=false: git metadata is unavailable in the trimmed build context.
ARG TARGETARCH
COPY . .
RUN CGO_ENABLED=0 GOOS=linux GOARCH=${TARGETARCH} \
    go build -trimpath -buildvcs=false -ldflags="-s -w" \
    -o /out/umurinzi ./cmd/server \
 && CGO_ENABLED=0 GOOS=linux GOARCH=${TARGETARCH} \
    go build -trimpath -buildvcs=false -ldflags="-s -w" \
    -o /out/healthcheck ./cmd/healthcheck

# ── Stage 2: runtime ──────────────────────────────────────────────────────────
# FROM scratch: no shell, no package manager, no libc — the smallest possible
# attack surface and image size. The binaries are fully static.
FROM scratch

# Import CA certificates for TLS outbound calls (HTTPS APIs, managed Redis).
COPY --from=builder /etc/ssl/certs/ca-certificates.crt /etc/ssl/certs/

# Copy the compiled binaries only — no Go toolchain, no source.
COPY --from=builder /out/umurinzi /umurinzi
COPY --from=builder /out/healthcheck /healthcheck

# Run as an unprivileged user (numeric — nothing to resolve in scratch).
USER 65532:65532

# The server listens on $PORT (default 8080).
EXPOSE 8080

# Compose/CI can override with a stronger value.
ENV PORT=8080

# Probe our own tiny static binary — the image has no shell/wget/curl.
# A dependency-down (503) still counts as alive: restarting the container
# cannot fix Postgres.
HEALTHCHECK --interval=10s --timeout=4s --start-period=15s --retries=5 \
  CMD ["/healthcheck"]

ENTRYPOINT ["/umurinzi"]

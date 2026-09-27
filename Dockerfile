# ── Stage 1: build ────────────────────────────────────────────────────────────
FROM golang:1.25-alpine AS builder

# Install git (needed by go mod download for VCS metadata)
RUN apk add --no-cache git ca-certificates

WORKDIR /src

# Copy dependency manifests first so Docker layer-caches the module download
# step independently of source changes.
COPY go.mod go.sum ./
RUN go mod download -x

# Copy full source and build a statically-linked binary.
COPY . .
RUN CGO_ENABLED=0 GOOS=linux GOARCH=amd64 \
    go build -trimpath -ldflags="-s -w" \
    -o /out/umurinzi ./cmd/server

# ── Stage 2: runtime ──────────────────────────────────────────────────────────
FROM scratch

# Import CA certificates for TLS outbound calls (e.g. future OSRM integration)
COPY --from=builder /etc/ssl/certs/ca-certificates.crt /etc/ssl/certs/

# Copy the compiled binary only — no Go toolchain, no source
COPY --from=builder /out/umurinzi /umurinzi

# The server listens on $PORT (default 8080)
EXPOSE 8080

ENTRYPOINT ["/umurinzi"]

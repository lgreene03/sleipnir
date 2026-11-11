# Stage 1: Build the statically linked Go executable
# Pin by digest so a tag reassignment cannot silently change the build environment.
# Go 1.26.11, matching go.mod: the first 1.26 release with the GO-2026-6859
# net/http MaxBytesReader header race fix. The previous golang:1.27-alpine digest
# was go1.27.1, which predates the GO-2026-6809 crypto/x509 fix (go1.27.3), so the
# shipped image carried it even though the tag looked current.
FROM golang:1.26.11-alpine@sha256:ae52b5bc6ed045a9ed26ab03dc19327a802a9cc9aa8b27ed57f5c6b60579b0bb AS builder

# Install build dependencies
RUN apk add --no-cache git

WORKDIR /app

# Copy dependency structures first to leverage Docker layer caching
COPY go.mod go.sum ./
RUN go mod download

# Copy the rest of the codebase
COPY . .

# Compile static binaries (disable CGO, strip symbols with ldflags, inject version)
ARG VERSION=dev
ARG GIT_SHA=unknown
ARG BUILD_TIME=unknown
RUN CGO_ENABLED=0 GOOS=linux go build \
      -ldflags="-w -s \
        -X sleipnir/internal/version.Version=${VERSION} \
        -X sleipnir/internal/version.GitSHA=${GIT_SHA} \
        -X sleipnir/internal/version.BuildTime=${BUILD_TIME}" \
      -o sleipnir ./cmd/sleipnir
RUN CGO_ENABLED=0 GOOS=linux go build -ldflags="-w -s" -o mock_huginn ./cmd/mock_huginn
RUN CGO_ENABLED=0 GOOS=linux go build -ldflags="-w -s" -o mock_portfolio ./cmd/mock_portfolio

# Stage 2: Final runner environment
FROM alpine:3.24@sha256:be2656061b536c1883c4034790e21d0a09c5aaaf51968b89368a3c0371ea2c9a

# Install SSL certificates and timezone databases
RUN apk --no-cache add ca-certificates tzdata

# Create a non-root user to run the service (defense in depth: a compromised
# process does not run as root inside the container).
RUN addgroup -S sleipnir && adduser -S -G sleipnir -u 10001 sleipnir

WORKDIR /app

# Copy executables from compiler environment
COPY --from=builder /app/sleipnir .
COPY --from=builder /app/mock_huginn .
COPY --from=builder /app/mock_portfolio .

# The SQLite store is written under /app/data (DB_PATH default
# /app/data/sleipnir.db, volume-mounted in compose). Pre-create it and hand
# ownership to the non-root user so the store is writable without root.
RUN mkdir -p /app/data && chown -R sleipnir:sleipnir /app

# Drop privileges for all subsequent runtime.
USER sleipnir

# Expose HTTP health server port
EXPOSE 8080

# By default, run the gateway binary
ENTRYPOINT ["./sleipnir"]

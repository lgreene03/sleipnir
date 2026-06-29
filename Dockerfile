# Stage 1: Build the statically linked Go executable
# Pin by digest so a tag reassignment cannot silently change the build environment.
FROM golang:1.26-alpine@sha256:3ad57304ad93bbec8548a0437ad9e06a455660655d9af011d58b993f6f615648 AS builder

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
FROM alpine:3.24@sha256:28bd5fe8b56d1bd048e5babf5b10710ebe0bae67db86916198a6eec434943f8b

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

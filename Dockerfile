# Stage 1: Build the statically linked Go executable
# Pin by digest so a tag reassignment cannot silently change the build environment.
FROM golang:1.26-alpine@sha256:7a3e50096189ad57c9f9f865e7e4aa8585ed1585248513dc5cda498e2f41812c AS builder

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
FROM alpine:3.24@sha256:a2d49ea686c2adfe3c992e47dc3b5e7fa6e6b5055609400dc2acaeb241c829f4

# Install SSL certificates and timezone databases
RUN apk --no-cache add ca-certificates tzdata

WORKDIR /app

# Copy executables from compiler environment
COPY --from=builder /app/sleipnir .
COPY --from=builder /app/mock_huginn .
COPY --from=builder /app/mock_portfolio .

# Expose HTTP health server port
EXPOSE 8080

# By default, run the gateway binary
ENTRYPOINT ["./sleipnir"]

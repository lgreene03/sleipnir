# Stage 1: Build the statically linked Go executable
FROM golang:1.25-alpine AS builder

# Install build dependencies
RUN apk add --no-cache git

WORKDIR /app

# Copy dependency structures first to leverage Docker layer caching
COPY go.mod go.sum ./
RUN go mod download

# Copy the rest of the codebase
COPY . .

# Compile static binaries (disable CGO, strip symbols with ldflags)
RUN CGO_ENABLED=0 GOOS=linux go build -ldflags="-w -s" -o sleipnir ./cmd/sleipnir
RUN CGO_ENABLED=0 GOOS=linux go build -ldflags="-w -s" -o mock_huginn ./cmd/mock_huginn
RUN CGO_ENABLED=0 GOOS=linux go build -ldflags="-w -s" -o mock_muninn ./cmd/mock_muninn

# Stage 2: Final runner environment
FROM alpine:3.23

# Install SSL certificates and timezone databases
RUN apk --no-cache add ca-certificates tzdata

WORKDIR /app

# Copy executables from compiler environment
COPY --from=builder /app/sleipnir .
COPY --from=builder /app/mock_huginn .
COPY --from=builder /app/mock_muninn .

# Expose HTTP health server port
EXPOSE 8080

# By default, run the gateway binary
ENTRYPOINT ["./sleipnir"]

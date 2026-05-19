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

# Compile a static binary (disable CGO, strip symbols with ldflags)
RUN CGO_ENABLED=0 GOOS=linux go build -ldflags="-w -s" -o sleipnir ./cmd/sleipnir

# Stage 2: Final runner environment
FROM alpine:3.20

# Install SSL certificates and timezone databases
RUN apk --no-cache add ca-certificates tzdata

WORKDIR /app

# Copy executable from compiler environment
COPY --from=builder /app/sleipnir .

# Expose HTTP health server port
EXPOSE 8080

# Run the gateway binary
ENTRYPOINT ["./sleipnir"]

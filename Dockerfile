# -------- BACKEND BUILD STAGE --------
FROM golang:1.26.3-bookworm AS builder

WORKDIR /build

# Install minimum build dependencies
RUN apt-get update && apt-get install -y \
    git \
    ca-certificates \
 && rm -rf /var/lib/apt/lists/*

# Cache dependency
COPY go.mod go.sum ./
RUN go mod download

# Copy source
COPY . .

# Build binary
RUN git config --global --add safe.directory /build && \
    COMMIT_ID="$(git rev-parse --short HEAD 2>/dev/null || echo unknown)" && \
    BUILD_TIME="$(date -u +"%Y-%m-%dT%H:%M:%SZ")" && \
    CGO_ENABLED=0 GOOS=linux GOARCH=amd64 \
    go build \
      -trimpath \
      -buildvcs=false \
      -ldflags="-w -s \
      -X github.com/KAnggara75/Rest2Kafka/internal/service.CommitID=${COMMIT_ID} \
      -X github.com/KAnggara75/Rest2Kafka/internal/service.BuildTime=${BUILD_TIME}" \
      -o rest2kafka \
      ./cmd/rest2kafka/main.go


# -------- RUNTIME STAGE --------
FROM debian:bookworm-slim

# Install runtime dependencies
RUN apt-get update && apt-get install -y \
    ca-certificates \
    tzdata \
 && rm -rf /var/lib/apt/lists/*

WORKDIR /app

# Copy binary
COPY --from=builder /build/rest2kafka .

# Security hardening
RUN useradd -u 10001 appuser
USER appuser

# Environment variables
ENV PORT=8888
ENV TZ=Asia/Jakarta

EXPOSE 8888

CMD ["./rest2kafka"]

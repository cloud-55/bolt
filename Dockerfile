# Build stage
FROM rust:1.75-slim-bookworm AS builder

WORKDIR /app

# Install build dependencies (including make/gcc for jemalloc)
RUN apt-get update && apt-get install -y \
    pkg-config \
    make \
    gcc \
    libc6-dev \
    && rm -rf /var/lib/apt/lists/*

# Copy source code
COPY Cargo.toml Cargo.lock* ./
COPY src/ ./src/

# Build release binaries
RUN cargo build --release

# Runtime stage
FROM debian:bookworm-slim

WORKDIR /app

# Install runtime dependencies
RUN apt-get update && apt-get install -y \
    ca-certificates \
    curl \
    && rm -rf /var/lib/apt/lists/* \
    && useradd -r -s /bin/false bolt

# Copy binaries from builder
COPY --from=builder /app/target/release/bolt-server /usr/local/bin/
COPY --from=builder /app/target/release/bolt-ctl /usr/local/bin/

# Create data directory
RUN mkdir -p /data && chown bolt:bolt /data

# Switch to non-root user
USER bolt

# Default environment variables
ENV BOLT_HOST=0.0.0.0
ENV BOLT_PORT=2012
ENV BOLT_DATA_DIR=/data
ENV BOLT_LOG_LEVEL=info
ENV RUST_LOG=info
ENV BOLT_METRICS_PORT=9091

# Expose ports
# 2012 - Client port
# 2013 - Cluster port
# 9091 - Metrics port (Prometheus)
EXPOSE 2012 2013 9091

# Health check using HTTP endpoint
HEALTHCHECK --interval=30s --timeout=5s --start-period=5s --retries=3 \
    CMD curl -f http://localhost:9091/health || exit 1

# Default command
CMD ["bolt-server"]

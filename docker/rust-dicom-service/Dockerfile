# Multi-stage build for Rust DICOM service
FROM rust:1.82-slim as builder

# Install build dependencies
RUN apt-get update && apt-get install -y \
    pkg-config \
    libssl-dev \
    && rm -rf /var/lib/apt/lists/*

WORKDIR /app

# Copy Rust project files specifically
COPY docker/rust-dicom-service/Cargo.toml docker/rust-dicom-service/Cargo.lock ./
COPY docker/rust-dicom-service/src ./src

# Build the application
RUN cargo build --release

# Runtime stage
FROM debian:bookworm-slim

# Install runtime dependencies
RUN apt-get update && apt-get install -y \
    ca-certificates \
    libssl3 \
    && rm -rf /var/lib/apt/lists/*

# Create app user
RUN useradd -r -s /bin/false axiom

# Copy the binary
COPY --from=builder /app/target/release/axiom-dicom-service /usr/local/bin/

# Create directories for DICOM data
RUN mkdir -p /dicom_data/incoming /dicom_data/dustbin && \
    chown -R axiom:axiom /dicom_data

# Set user
USER axiom

# Default port for DICOM listener
EXPOSE 11112

ENTRYPOINT ["axiom-dicom-service"]

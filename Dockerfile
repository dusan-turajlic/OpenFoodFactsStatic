# Multi-stage build for optimal image size
FROM rust:1.89-slim-bookworm

# Install system dependencies
RUN apt-get update && apt-get install -y \
    pkg-config \
    libssl-dev \
    ca-certificates \
    && \
    rm -rf /var/lib/apt/lists/*

# Set working directory
WORKDIR /app

COPY src/ src/
COPY Cargo.toml ./

# Build the actual application
RUN cargo build --release

# Create app user for security
RUN useradd -r -s /bin/false appuser

# Create directories for data and output
RUN mkdir -p /app/food_facts_raw_data /app/static && \
    chown -R appuser:appuser /app

# Switch to non-root user
USER appuser

# Set environment variables
ENV RUST_LOG=info

# Default command
CMD ["target/release/process_data"]

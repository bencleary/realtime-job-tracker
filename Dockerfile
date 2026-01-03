# Build stage
FROM rust:1.75 as builder

WORKDIR /app

# Copy manifests
COPY Cargo.toml Cargo.lock ./

# Copy source code
COPY src ./src
COPY templates ./templates

# Build the application
RUN cargo build --release

# Runtime stage
FROM debian:bookworm-slim

WORKDIR /app

# Install required runtime dependencies
RUN apt-get update && apt-get install -y \
    ca-certificates \
    && rm -rf /var/lib/apt/lists/*

# Bind to all interfaces inside containers.
ENV BIND_ADDR=0.0.0.0:3000

# Copy the binary from builder
COPY --from=builder /app/target/release/poem-oai /app/poem-oai

# Copy templates
COPY templates ./templates

# Expose the port the app runs on
EXPOSE 3000

# Set the startup command
CMD ["/app/poem-oai"]

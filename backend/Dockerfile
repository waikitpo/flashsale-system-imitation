FROM golang:1.24 AS builder

# Install build essentials
RUN apt-get update && apt-get install -y build-essential cmake

# Set working directory
WORKDIR /app

# Copy dependency files
COPY backend/go.mod backend/go.sum ./backend/

# Download dependencies
WORKDIR /app/backend
RUN go mod download

# Copy source code
WORKDIR /app
COPY engine ./engine
COPY backend ./backend

# Build the application
WORKDIR /app/backend
RUN go mod tidy
# Enable CGO for C++ bridge
RUN CGO_ENABLED=1 GOOS=linux go build -o backend_server .

# Runtime Stage
FROM debian:trixie-slim

WORKDIR /app

# Install runtime dependencies (certificates, etc.)
RUN apt-get update && apt-get install -y ca-certificates && rm -rf /var/lib/apt/lists/*

# Copy binary and config
COPY --from=builder /app/backend/backend_server .
COPY --from=builder /app/backend/config ./config

# Create data directory for WAL
RUN mkdir -p data

# Expose port (default 3000)
EXPOSE 3000

# Run
CMD ["./backend_server"]

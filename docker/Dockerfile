# --- Stage 1: Builder Stage ---
FROM rust:1.96-alpine AS builder

WORKDIR /app

# Install required build dependencies for Rust and SQLx
RUN apk add --no-cache \
  pkgconfig \
  openssl-dev \
  openssl-libs-static \
  sqlite-dev \
  build-base \
  cmake \
  curl \
  ninja-build \
  clang \
  && rm -rf /var/cache/apk/*

# Copy the locked dependency graph before the source code.
COPY Cargo.toml Cargo.lock ./

# Copy the source code for the application
COPY src src/

# Set SQLX_OFFLINE to true for offline SQLx compilation
ENV SQLX_OFFLINE=true

# Copy the pre-generated SQLx metadata for offline mode
COPY .sqlx .sqlx/

# Copy the migrations folder
COPY migrations migrations/

# Build the application in release mode. Cache mounts avoid baking Cargo's
# registry and the large target directory into an intermediate image layer.
RUN --mount=type=cache,target=/usr/local/cargo/registry \
    --mount=type=cache,target=/app/target \
    cargo build --release --locked --no-default-features \
    && cp /app/target/release/nezumo /app/nezumo \
    && strip /app/nezumo

# --- Stage 2: Runtime Stage ---
FROM alpine:latest

# Install runtime dependencies (ca-certificates, openssl) and mutool (MuPDF)
# for PDF -> SVG conversion in the /boards/{id}/pdf endpoint.
RUN apk add --no-cache \
  ca-certificates \
  curl \
  openssl \
  mupdf-tools \
  && rm -rf /var/cache/apk/*

# Create non-root user for security purposes
RUN adduser -D -u 1001 appuser

WORKDIR /app

# Copy the built binary from the builder stage
COPY --from=builder /app/nezumo .

# Bundle the board fonts (real OTF/TTF + fonts.json) next to the binary so PDF
# export (svg2pdf/usvg) can shape text with no operator config. usvg can't decode
# the SVG's embedded subset-woff2 @font-face, so it needs real font files here.
# Resolved at runtime as ./fonts (WORKDIR /app) unless RENDER_FONTS_DIR is set. For
# non-Docker deploys, copy this `fonts/` dir next to the binary (or point
# RENDER_FONTS_DIR at a dir holding the full OTF/TTF set).
COPY fonts fonts/

# Runtime mail templates used by registration and password-reset messages.
COPY src/mail/footer.html templates/mail/footer.html

# Ensure the .env file and other app files have the correct ownership and permissions
RUN chown -R appuser:appuser /app

# Switch to the non-root user
USER appuser

# Expose the application port (default 3000)
EXPOSE 3000

# Run the application when the container starts
CMD ["./nezumo"]

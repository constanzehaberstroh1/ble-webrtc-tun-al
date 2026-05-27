# ============================================
# Stage 1 — Build Go binary
# ============================================
FROM golang:1.25-alpine AS builder

RUN apk add --no-cache git gcc musl-dev

WORKDIR /app

COPY go.mod go.sum ./
RUN go mod download

COPY . .

# Copy the pre-built static assets from the local repository
# directly into the embed path. Since internal/webui/dist is
# already checked in, this ensures the web UI is bundled successfully
# without requiring a memory-intensive Node build stage on Clever Cloud.
RUN mkdir -p /app/internal/webui/dist && \
    cp -r /app/web/dist/* /app/internal/webui/dist/ 2>/dev/null || true

RUN CGO_ENABLED=0 GOOS=linux go build -ldflags="-s -w" \
    -o /app/bin/server ./cmd/server

# ============================================
# Stage 2 — Runtime (lightweight Alpine)
# ============================================
FROM alpine:3.21

RUN apk add --no-cache \
    bash \
    ca-certificates \
    curl \
    ncurses-terminfo-base \
    && update-ca-certificates

WORKDIR /app

COPY --from=builder /app/bin/server /app/server
COPY GUIDE.md /app/GUIDE.md

EXPOSE 8080

ENTRYPOINT ["/app/server"]

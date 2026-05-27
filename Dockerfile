# ============================================
# Stage 1 — Build frontend (Node.js)
# ============================================
FROM node:20-alpine AS frontend

WORKDIR /app/web

COPY web/package.json ./
RUN npm ci

COPY web/ ./
RUN npm run build

# ============================================
# Stage 2 — Build Go binary
# ============================================
FROM golang:1.25-alpine AS builder

RUN apk add --no-cache git gcc musl-dev

WORKDIR /app

COPY go.mod go.sum ./
RUN go mod download

COPY . .

# Copy built frontend into the embedded webui directory
COPY --from=frontend /app/web/dist /app/internal/webui/dist

RUN CGO_ENABLED=0 GOOS=linux go build -ldflags="-s -w" \
    -o /app/bin/server ./cmd/server

# ============================================
# Stage 3 — Runtime (lightweight Alpine)
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

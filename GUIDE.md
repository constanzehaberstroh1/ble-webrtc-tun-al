# BLE WebRTC Tunnel — Complete Guide

> A WebRTC-based tunneling system that routes client traffic through Bale's unblocked SFU infrastructure to bypass ISP restrictions. Traffic is encoded as video frames and transmitted over WebRTC data channels through the Bale chat platform.

---

## Table of Contents

1. [How It Works](#how-it-works)
2. [Architecture](#architecture)
3. [Prerequisites](#prerequisites)
4. [Quick Start](#quick-start)
5. [Configuration](#configuration)
6. [Building](#building)
7. [Running](#running)
8. [Admin Panel](#admin-panel)
9. [Account Management](#account-management)
10. [Migration from Legacy Config](#migration-from-legacy-config)
11. [REST API Reference](#rest-api-reference)
12. [Logging System](#logging-system)
13. [Deployment](#deployment)
14. [Troubleshooting](#troubleshooting)
15. [Security](#security)
16. [Project Structure](#project-structure)

---

## How It Works

```
┌──────────────────────────────────────────────────────────────────────┐
│                        RESTRICTED NETWORK                            │
│                                                                      │
│  ┌──────────┐    SOCKS5/HTTP    ┌──────────┐    WebRTC (Bale SFU)   │
│  │  Browser  │ ──────────────→  │  CLIENT   │ ════════════════════╗  │
│  │  or App   │   localhost:1080 │  Binary   │                     ║  │
│  └──────────┘                   └──────────┘                     ║  │
└──────────────────────────────────────────────────────────────────║──┘
                                                                   ║
                          Bale's SFU (WebRTC)                     ║
                          ════════════════════                    ║
                          (IP is not blocked)                     ║
                                                                   ║
┌──────────────────────────────────────────────────────────────────║──┐
│                        UNRESTRICTED NETWORK                      ║  │
│                                                                  ║  │
│  ┌──────────┐    yamux tunnel   ┌──────────┐   WebRTC (Bale SFU) ║  │
│  │ Internet  │ ←──────────────  │  SERVER   │ ═══════════════════╝  │
│  │           │                  │  Binary   │                       │
│  └──────────┘                   └──────────┘                       │
└──────────────────────────────────────────────────────────────────────┘
```

### Flow

1. **Client** starts a SOCKS5/HTTP proxy on `localhost:1080`
2. Browser/apps connect to the local proxy
3. Client uses its **Bale account token** to "call" the paired server account via Bale's WebSocket API
4. Both sides join a Bale SFU room and establish a WebRTC PeerConnection
5. Traffic flows over **WebRTC DataChannels**, multiplexed with **yamux**
6. Server receives the traffic and forwards it to the open internet
7. Responses travel back through the same WebRTC tunnel

### Why It Works

- Bale's SFU servers (`*.ble.ir`) are **not blocked** by the ISP
- WebRTC TURN servers on port 443 look like regular HTTPS traffic
- Data is encoded as video frames, making it harder to detect

---

## Architecture

```
ble-webrtc-tun/
├── cmd/
│   ├── client/       # Client binary — local proxy + Bale caller
│   ├── server/       # Server binary — Bale receiver + internet forwarder
│   └── migrate/      # Migration tool — .env.tokens → SQLite
├── internal/
│   ├── accounts/     # Account lifecycle, health checks, pairing
│   ├── admin/        # Legacy HTML admin panel (deprecated)
│   ├── api/          # REST API server (replaces admin)
│   ├── bale/         # Bale WebSocket protocol client
│   ├── config/       # Environment configuration loader
│   ├── db/           # SQLite database (GORM, CGO-free)
│   ├── dcconn/       # DataChannel → io.ReadWriteCloser adapter
│   ├── logger/       # Centralized logging system (per-component files)
│   ├── livekit/      # LiveKit SFU transport (PeerConnections)
│   ├── pool/         # Round-robin yamux session pool
│   ├── proxy/        # TCP proxy for forwarding
│   ├── router/       # Connection routing + state machine
│   ├── sync/         # Event-sourced client↔server sync
│   ├── transport/    # WebRTC transport layer
│   └── webui/        # Embedded React admin panel (go:embed)
├── web/              # React admin panel source (Vite + TailwindCSS)
├── data/             # SQLite databases (auto-created, gitignored)
├── .env              # Infrastructure config
├── .env.tokens       # Legacy token pairs (deprecated → use DB)
├── Dockerfile        # Multi-stage Docker build
├── docker-compose.yml
├── Makefile
└── GUIDE.md          # ← You are here
```

### Key Components

| Component | Description |
|-----------|-------------|
| **Database** | SQLite via GORM (pure Go, no CGO). Stores accounts, pairings, connections, events |
| **Account Manager** | Manages Bale token lifecycle, health checks, auto-pairing |
| **Router** | State machine for server accounts (IDLE → RESERVED → IN_CALL → IDLE) |
| **Sync Engine** | Event-sourced bidirectional sync between client and server DBs |
| **REST API** | Full CRUD for accounts, pairings, connections, stats |
| **Admin Panel** | React SPA embedded in the Go binary via `go:embed` |
| **Logger** | Per-component log files under `logs/`, combined log, 5 severity levels |

---

## Prerequisites

- **Go 1.21+** (tested with Go 1.25)
- **Node.js 18+** (for building the admin panel)
- **Bale accounts** with valid access tokens (JWT)
- **Linux** (TUN mode requires `cap_add: NET_ADMIN`)

### Optional

- Docker & Docker Compose (for containerized deployment)
- `sudo` access (only needed for TUN interface mode)

---

## Quick Start

### 1. Clone and Build

```bash
git clone https://github.com/salman/ble-webrtc-tun.git
cd ble-webrtc-tun

# Build everything (frontend + all binaries)
make build
```

This produces three binaries in `bin/`:
- `bin/server` — server-side tunnel endpoint
- `bin/client` — client-side proxy + tunnel
- `bin/migrate` — legacy token migration tool

### 2. Configure

```bash
cp .env.example .env
# Edit .env with your TURN/STUN servers and admin credentials
```

### 3. Add Accounts via Admin Panel

```bash
# Start the server
./bin/server

# Open admin panel in browser
# http://localhost:8081 (default API/admin port)
# Login with ADMIN_USERNAME / ADMIN_PASSWORD from .env
```

Then use the admin panel to:
1. Add Bale account tokens (SERVER role)
2. Add Bale account tokens (CLIENT role)
3. Create pairings (CLIENT ↔ SERVER)

### 4. Start the Client

```bash
# On the restricted network machine
./bin/client
```

Configure your browser to use `SOCKS5 proxy: localhost:1080`

---

## Configuration

### Environment Variables (`.env`)

| Variable | Default | Description |
|----------|---------|-------------|
| `ROLE` | `client` | Binary role: `client` or `server` |
| `LIVEKIT_WS_URL` | `wss://meet-gwbm6.ble.ir/rtc` | LiveKit/Bale SFU WebSocket URL |
| `TURN_SERVER_PRIMARY` | `turns:meet-turn.ble.ir:443?transport=tcp` | Primary TURN server (port 443 = HTTPS camouflage) |
| `TURN_SERVER_SECONDARY` | `turn:2.189.68.97:3478?transport=tcp` | Secondary TURN server |
| `STUN_SERVER_PRIMARY` | `stun:2.189.68.115:443` | STUN server for NAT traversal |
| `STUN_SERVER_SECONDARY` | `stun:stun.l.google.com:19302` | Google STUN fallback |
| `TURN_USERNAME` | — | TURN credentials (from LiveKit signaling) |
| `TURN_CREDENTIAL` | — | TURN password |
| `TUN_IP` | `10.0.0.2` | TUN interface IP (client=.2, server=.1) |
| `TUN_MASK` | `255.255.255.0` | TUN subnet mask |
| `TUN_MTU` | `1400` | TUN MTU size |
| `TUN_NAME` | `tun-ble` | TUN interface name |
| `ADMIN_LISTEN_ADDR` | `:8080` | Legacy admin panel listen address |
| `ADMIN_USERNAME` | `admin` | Admin panel + API username |
| `ADMIN_PASSWORD` | `changeme` | Admin panel + API password |
| `API_LISTEN_ADDR` | `:8081` (server) `:8082` (client) | REST API / admin panel port |
| `LOG_LEVEL` | `info` | Logging verbosity |

### Token Configuration (Legacy: `.env.tokens`)

> **Note:** This format is deprecated. Use the admin panel or REST API to manage tokens instead. The system auto-migrates from `.env.tokens` on first run.

```bash
# Each pair N defines one client↔server channel
BALE_TOKEN_CLIENT_1=eyJhbGciOiJSUzI1NiIs...
BALE_TOKEN_SERVER_1=eyJhbGciOiJSUzI1NiIs...
BALE_TARGET_SERVER_1=507575034

BALE_TOKEN_CLIENT_2=eyJhbGciOiJSUzI1NiIs...
BALE_TOKEN_SERVER_2=eyJhbGciOiJSUzI1NiIs...
BALE_TARGET_SERVER_2=423228813
# ... up to 8 pairs
```

### How to Get Bale Tokens

1. Open [web.bale.ai](https://web.bale.ai) in a browser
2. Login with the phone number you want to use
3. Open **Developer Tools** → **Application** → **Local Storage**
4. Copy the `access_token` value (it's a JWT starting with `eyJ...`)
5. Each tunnel channel needs **two separate Bale accounts** (one for client, one for server)

---

## Building

### Full Build (Recommended)

```bash
make build
```

This runs:
1. `npm install && npm run build` in `web/` (React admin panel)
2. Copies `web/dist/` → `internal/webui/dist/` (for `go:embed`)
3. `go build` for server, client, and migrate binaries

### Individual Targets

```bash
make build-web      # Build React admin panel only
make build-server   # Build server binary only
make build-client   # Build client binary only
make build-migrate  # Build migration tool only
```

### Docker Build

```bash
make docker         # Build Docker image
# or
docker-compose build
```

---

## Running

### Server Mode

```bash
# With TUN interface (requires root/NET_ADMIN)
sudo ./bin/server

# Without TUN (proxy mode — no root needed)
./bin/server
```

The server:
- Opens `data/server.db` (auto-created)
- Auto-migrates from `.env.tokens` if DB is empty
- Starts the legacy admin panel on `ADMIN_LISTEN_ADDR` (`:8080`)
- Starts the REST API + modern admin panel on `API_LISTEN_ADDR` (`:8081`)
- Connects to Bale WS for each enabled SERVER account
- Waits for incoming calls from paired CLIENT accounts

### Client Mode

```bash
# With TUN interface (routes all traffic through tunnel)
sudo ./bin/client

# Without TUN (SOCKS5/HTTP proxy on localhost:1080)
./bin/client
```

The client:
- Opens `data/client.db` (auto-created)
- Starts SOCKS5 proxy on `localhost:1080`
- Starts HTTP proxy on `localhost:8118`
- Loads paired accounts from DB
- Calls each paired SERVER account via Bale
- Establishes WebRTC DataChannel tunnels
- Routes proxy traffic through the tunnels via yamux

### Docker

```bash
# Start server container
docker-compose up -d

# View logs
docker-compose logs -f

# Stop
docker-compose down
```

---

## Admin Panel

The admin panel is a React SPA embedded in the Go binary. It's accessible at the API server port (default `:8081`).

### Login

- Navigate to `http://<server-ip>:8081`
- Enter the credentials from `ADMIN_USERNAME` / `ADMIN_PASSWORD`
- Credentials are transmitted via HTTP Basic Auth

### Dashboard

Real-time system overview:
- **Active Sessions** — currently connected tunnels
- **Server Accounts** — total, idle, in-call counts
- **Client Accounts** — total and offline counts
- **Uptime** — time since server start
- **Data Transfer** — total bytes sent/received
- **Active Connections** table with call ID, room, and start time

Data refreshes automatically (configurable in Settings).

### Accounts

Manage Bale token accounts:
- **Add Account** — paste a Bale JWT token and select role (CLIENT/SERVER)
- **Enable/Disable** — toggle accounts without deleting
- **Delete** — permanently remove an account
- **Status indicators**: IDLE (blue), IN_CALL (green), RESERVED (amber), OFFLINE (gray), ERROR (red)

### Pairings

Link CLIENT and SERVER accounts:
- **Create Pairing** — manually select one client + one server
- **Auto-Pair** — automatically match unpaired clients with unpaired servers (1:1)
- **Delete Pairing** — unlink accounts

> **Important:** Each server account must be paired with exactly one client account. The client can only call a server that is paired with it.

### Connection History

Audit log of all past tunnel sessions:
- Call ID, Room ID, Duration
- Bytes sent/received per session
- Termination reason (USER_DISCONNECT, NETWORK_DROP, TIMEOUT, ERROR)

### Settings

Customize the admin panel:
- **Theme Mode** — Dark / Light
- **Accent Color** — 6 color schemes (Indigo, Violet, Emerald, Rose, Amber, Cyan)
- **Auto-refresh Interval** — 3s, 5s, 10s, 30s, 60s
- **History Limit** — 25, 50, or 100 sessions

Settings are persisted in the browser's localStorage.

---

## Account Management

### Account Lifecycle

```
Token Added → JWT Parsed → BaleUserID Extracted → DB Record Created → Health Check
                                                                         │
                                              ┌──────────┐              │
                                              │  OFFLINE  │◄──── WS Connect Failed
                                              └─────┬─────┘
                                                    │ WS Connected
                                              ┌─────▼─────┐
                                     ┌───────►│    IDLE    │◄──── Call Ended
                                     │        └─────┬─────┘
                                     │              │ Client Reserves
                                     │        ┌─────▼─────┐
                                     │        │  RESERVED  │──── Timeout (30s) ──┐
                                     │        └─────┬─────┘                     │
                                     │              │ Call Accepted              │
                                     │        ┌─────▼─────┐                     │
                                     └────────│  IN_CALL   │                    │
                                              └───────────┘                     │
                                                                                │
                                              ┌───────────┐                     │
                                              │   ERROR    │◄──────────────────┘
                                              └───────────┘
```

### Health Checks

- Run every **60 seconds** for all enabled accounts
- Attempt a brief WebSocket connection to Bale
- Mark accounts as `OFFLINE` or `ERROR` if connection fails
- Emit `STATUS_CHANGED` events for the sync engine

### Auto-Pairing Logic

When you click **Auto-Pair** in the admin panel or call `POST /api/pairings/auto`:

1. Find all CLIENT accounts without an existing pairing
2. Find all SERVER accounts without an existing pairing
3. Pair them 1:1 in order of creation
4. Return the number of pairs created

---

## Migration from Legacy Config

### Automatic (Recommended)

The system auto-migrates on first run:
1. Checks if `data/{role}.db` has any accounts
2. If empty AND `.env.tokens` exists → imports tokens and creates pairings
3. Continues with DB-driven mode

No action needed — just start the binary.

### Manual Migration

```bash
# Preview what will be imported (no changes)
./bin/migrate --role=server --dry-run

# Import .env.tokens → database
./bin/migrate --role=server --tokens=.env.tokens

# Validate database integrity
./bin/migrate --role=server --validate

# Export database back to .env.tokens format (rollback)
./bin/migrate --role=server --export --output=.env.tokens.bak
```

### Migration CLI Flags

| Flag | Default | Description |
|------|---------|-------------|
| `--role` | `server` | Database role: `client` or `server` |
| `--tokens` | `.env.tokens` | Path to the legacy token file |
| `--dry-run` | `false` | Preview without writing to DB |
| `--export` | `false` | Export DB → `.env.tokens` format |
| `--output` | stdout | Output file for `--export` |
| `--validate` | `false` | Check DB integrity |

---

## REST API Reference

All API endpoints require **HTTP Basic Authentication** (same credentials as admin panel).

Base URL: `http://<host>:<api_port>/api`

### Authentication

```bash
# All requests need Basic Auth
curl -u admin:changeme http://localhost:8081/api/stats
```

### Health

| Method | Endpoint | Auth | Description |
|--------|----------|------|-------------|
| `GET` | `/api/health` | No | Health check, uptime, DB status |

```bash
curl http://localhost:8081/api/health
# {"status":"ok","role":"server","uptime":"2h15m30s","db_path":"data/server.db","sessions":3}
```

### Stats

| Method | Endpoint | Description |
|--------|----------|-------------|
| `GET` | `/api/stats` | Dashboard statistics |

```json
{
  "uptime": "2h15m30s",
  "active_sessions": 3,
  "total_clients": 4,
  "total_servers": 4,
  "idle_servers": 1,
  "in_call_servers": 3,
  "offline_accounts": 0,
  "connections": {
    "total_bytes_sent": 104857600,
    "total_bytes_received": 52428800,
    "total_sessions": 42
  }
}
```

### Accounts

| Method | Endpoint | Description |
|--------|----------|-------------|
| `GET` | `/api/accounts` | List all accounts |
| `GET` | `/api/accounts?role=CLIENT` | Filter by role |
| `POST` | `/api/accounts` | Add new account |
| `GET` | `/api/accounts/:id` | Get account details |
| `PATCH` | `/api/accounts/:id` | Enable/disable account |
| `DELETE` | `/api/accounts/:id` | Delete account |
| `POST` | `/api/accounts/:id/info` | Refresh Bale info |

#### Add Account

```bash
curl -u admin:changeme -X POST http://localhost:8081/api/accounts \
  -H 'Content-Type: application/json' \
  -d '{"token": "eyJhbGciOiJSUzI1NiIs...", "role": "SERVER"}'
```

Response:
```json
{
  "id": 5,
  "bale_user_id": 507575034,
  "role": "SERVER",
  "status": "IDLE",
  "enabled": true,
  "created_at": "2026-05-02T14:30:00Z"
}
```

#### Disable Account

```bash
curl -u admin:changeme -X PATCH http://localhost:8081/api/accounts/5 \
  -H 'Content-Type: application/json' \
  -d '{"enabled": false}'
```

### Pairings

| Method | Endpoint | Description |
|--------|----------|-------------|
| `GET` | `/api/pairings` | List all pairings |
| `POST` | `/api/pairings` | Create pairing |
| `DELETE` | `/api/pairings/:id` | Delete pairing |
| `PATCH` | `/api/pairings/:id` | Activate/deactivate |
| `POST` | `/api/pairings/auto` | Auto-pair unmatched accounts |

#### Create Pairing

```bash
curl -u admin:changeme -X POST http://localhost:8081/api/pairings \
  -H 'Content-Type: application/json' \
  -d '{"client_account_id": 1, "server_account_id": 2}'
```

#### Auto-Pair

```bash
curl -u admin:changeme -X POST http://localhost:8081/api/pairings/auto
# {"paired": 3}
```

### Connections

| Method | Endpoint | Description |
|--------|----------|-------------|
| `GET` | `/api/connections/active` | Currently active tunnels |
| `GET` | `/api/connections/history?limit=50` | Historical sessions |

### Events (Sync)

| Method | Endpoint | Description |
|--------|----------|-------------|
| `GET` | `/api/events?since=0` | Get events since sequence ID |

### Migration & Validation

| Method | Endpoint | Description |
|--------|----------|-------------|
| `POST` | `/api/migrate` | Trigger .env.tokens import |
| `GET` | `/api/validate` | Check database integrity |

### Settings

| Method | Endpoint | Description |
|--------|----------|-------------|
| `GET` | `/api/settings` | Get all settings |
| `POST` | `/api/settings` | Set a key-value pair |

```bash
curl -u admin:changeme -X POST http://localhost:8081/api/settings \
  -H 'Content-Type: application/json' \
  -d '{"key": "refresh_interval", "value": "10"}'
```

---

## Deployment

### Single Binary (Recommended)

The entire system — server logic, REST API, and admin panel UI — is compiled into a **single portable binary**. No external dependencies.

```bash
# Build
make build

# Deploy
scp bin/server user@server-host:/opt/ble-tunnel/
scp .env user@server-host:/opt/ble-tunnel/

# Run
ssh user@server-host
cd /opt/ble-tunnel
./server
```

### Systemd Service

```ini
# /etc/systemd/system/ble-tunnel.service
[Unit]
Description=BLE WebRTC Tunnel Server
After=network.target

[Service]
Type=simple
WorkingDirectory=/opt/ble-tunnel
ExecStart=/opt/ble-tunnel/server
Restart=always
RestartSec=5
Environment=ROLE=server
Environment=ADMIN_USERNAME=admin
Environment=ADMIN_PASSWORD=your-secure-password
Environment=API_LISTEN_ADDR=:8081

# Required for TUN mode
AmbientCapabilities=CAP_NET_ADMIN CAP_NET_RAW

[Install]
WantedBy=multi-user.target
```

```bash
sudo systemctl daemon-reload
sudo systemctl enable ble-tunnel
sudo systemctl start ble-tunnel
sudo systemctl status ble-tunnel
```

### Docker

```bash
# Build and run
docker-compose up -d

# View logs
docker-compose logs -f ble-tunnel-server

# Access admin panel
open http://localhost:8080
```

### Persistent Data

The database is stored in `data/server.db` (or `data/client.db`). Make sure this directory is persistent:

```yaml
# docker-compose.yml
volumes:
  - ./data:/app/data    # ← Persist database
  - ./logs:/app/logs    # ← Persist logs
```

---

## Troubleshooting

### Common Issues

#### "Session expired — please login again"

- Your admin password was changed or the server restarted
- Solution: re-login with the correct credentials

#### "all server accounts busy"

- All server accounts are in `IN_CALL` or `RESERVED` status
- Check the Dashboard → Active Connections for stuck sessions
- Add more SERVER accounts, or wait for current calls to end

#### "Bale connect failed"

- Token may be expired (Bale tokens expire after ~1 year)
- Solution: get a fresh token from [web.bale.ai](https://web.bale.ai) and update the account

#### "could not extract user ID from JWT"

- Token format is invalid or corrupted
- Make sure you copied the complete token (starts with `eyJ`, three parts separated by dots)

#### Server accounts stuck in "IN_CALL"

- Network interruption may have left stale state
- Restart the server binary — accounts will reset to IDLE on reconnect

#### Client can't connect through proxy

1. Check that the client binary is running
2. Verify SOCKS5 proxy is listening: `ss -tlnp | grep 1080`
3. Check client logs for WebRTC connection errors
4. Ensure TURN servers are accessible from the restricted network

### Viewing Logs

The application uses a **per-component logging system**. Each component writes to its own file under `logs/<role>/`, making it easy to isolate issues.

```bash
# Watch all logs in real-time
tail -f logs/server/combined.log

# Watch a specific component
tail -f logs/server/bale.log      # Bale WebSocket issues
tail -f logs/server/sfu.log       # LiveKit/WebRTC issues
tail -f logs/server/router.log    # Call routing problems
tail -f logs/server/api.log       # REST API request errors
tail -f logs/server/accounts.log  # Account health check failures
tail -f logs/server/db.log        # Database errors

# Search for errors across all logs
grep '\[ERROR\]' logs/server/combined.log
grep '\[WARN\]' logs/server/combined.log

# Search a specific component for errors
grep '\[ERROR\]' logs/server/bale.log
```

See [Logging System](#logging-system) for full details.

### Validating the Database

```bash
./bin/migrate --role=server --validate
```

Checks for:
- Accounts with invalid roles or empty tokens
- Pairings referencing non-existent accounts
- Role mismatches (e.g., a CLIENT in a server pairing slot)
- Unpaired enabled accounts

---

## Logging System

The application includes a centralized, structured logging system (`internal/logger/`) that writes to both **stdout** and **per-component log files**.

### Log Directory Structure

```
logs/
├── server/                  # Server binary logs
│   ├── combined.log         # All components combined
│   ├── main.log             # Server startup, shutdown, signals
│   ├── bale.log             # Bale WebSocket protocol (calls, messages)
│   ├── sfu.log              # LiveKit SFU transport (WebRTC, PeerConnection)
│   ├── webrtc.log           # Low-level WebRTC DataChannel / video tunnel
│   ├── api.log              # REST API server (requests, auth)
│   ├── router.log           # Connection routing, state machine transitions
│   ├── accounts.log         # Account lifecycle, health checks
│   ├── db.log               # SQLite database operations
│   ├── sync.log             # Event-sourced sync engine
│   ├── admin.log            # Legacy admin panel
│   └── webui.log            # Embedded React panel
└── client/                  # Client binary logs
    ├── combined.log
    ├── main.log
    ├── bale.log
    ├── sfu.log
    └── ...                  # Same structure as server
```

### Log Levels

| Level | Description | When to Use |
|-------|-------------|-------------|
| `DEBUG` | Verbose diagnostic info | Hex dumps, protocol details, trace data |
| `INFO` | Normal operations | Startup, connections, state changes |
| `WARN` | Potential issues | Timeouts, fallbacks, deprecated usage |
| `ERROR` | Failures that are recoverable | Connection drops, auth failures, bad input |
| `FATAL` | Unrecoverable failures | Exits the process immediately |

Set the level via environment variable:

```bash
# Show only warnings and above
LOG_LEVEL=warn ./bin/server

# Show everything including debug
LOG_LEVEL=debug ./bin/server
```

### Log Format

**Stdout** (colored, human-readable):
```
2026/05/02 18:19:41.635 INFO  [MAIN] === BLE WebRTC Tunnel — Server ===
2026/05/02 18:19:41.645 ERROR [MAIN] Admin panel error: listen tcp :8080: address in use
```

**Log files** (machine-parseable):
```
2026/05/02 18:19:41.635567 [INFO ] [MAIN] === BLE WebRTC Tunnel — Server ===
2026/05/02 18:19:41.645692 [ERROR] [MAIN] Admin panel error: listen tcp :8080: address in use
```

### Debugging Guide by Component

| Problem | Log File | What to Look For |
|---------|----------|-------------------|
| Bale WS drops | `bale.log` | `[ERROR]` connect/read failures, `[WARN]` token issues |
| Calls not connecting | `sfu.log` | ICE failures, SDP errors, LiveKit token issues |
| WebRTC data not flowing | `webrtc.log` | DataChannel errors, ICE state changes |
| API returning errors | `api.log` | HTTP status codes, auth failures |
| Accounts going OFFLINE | `accounts.log` | Health check failures, token expiry |
| Call collisions | `router.log` | State machine transitions, duplicate call warnings |
| DB errors | `db.log` | GORM slow query warnings, migration errors |
| Sync failures | `sync.log` | Event replay errors, connection drops |

---

## Security

### Authentication

- Admin panel and all `/api/*` endpoints require **HTTP Basic Auth**
- Set strong credentials via `ADMIN_USERNAME` and `ADMIN_PASSWORD`
- The `/api/health` endpoint is unauthenticated (for monitoring probes)
- Frontend static files are served without auth (the SPA loads before login)

### Token Security

- Bale JWT tokens are **never exposed** in API responses (tagged with `json:"-"`)
- Tokens are stored in the local SQLite database
- The database file (`data/*.db`) should have restricted file permissions:

```bash
chmod 600 data/server.db
```

### Network

- The REST API binds to all interfaces by default — restrict with `API_LISTEN_ADDR=127.0.0.1:8081` for local-only access
- In production, put the admin panel behind a reverse proxy (nginx/caddy) with TLS
- TURN credentials rotate — the system extracts them from LiveKit signaling automatically

### Recommendations

1. **Change default credentials** immediately after first setup
2. **Use HTTPS** for the admin panel in production (reverse proxy with TLS)
3. **Restrict network access** to the API port using firewall rules
4. **Rotate Bale tokens** periodically (they have a 1-year expiry)
5. **Back up the database** regularly: `cp data/server.db data/server.db.bak`

---

## Project Structure

### Binaries

| Binary | Description | Default Port |
|--------|-------------|--------------|
| `bin/server` | Server-side tunnel endpoint | `:8080` (admin), `:8081` (API) |
| `bin/client` | Client-side proxy + tunnel | `:8082` (API), `:1080` (SOCKS5) |
| `bin/migrate` | Database migration tool | — |

### Database Schema

| Table | Purpose |
|-------|---------|
| `accounts` | Bale token accounts (role, status, token, bale_user_id) |
| `pairings` | Client ↔ Server account mappings |
| `connection_logs` | Historical WebRTC session records |
| `events` | Append-only event log for sync engine |
| `settings` | Key-value configuration store |

### Account Statuses

| Status | Meaning |
|--------|---------|
| `IDLE` | Connected to Bale, ready for calls |
| `RESERVED` | Reserved by a client, call pending |
| `IN_CALL` | Active WebRTC tunnel session |
| `OFFLINE` | Not connected to Bale WS |
| `ERROR` | Connection or health check failed |

### Event Types

| Event | Trigger |
|-------|---------|
| `ACCOUNT_ADDED` | New account created |
| `ACCOUNT_REMOVED` | Account deleted |
| `ACCOUNT_UPDATED` | Account enabled/disabled |
| `STATUS_CHANGED` | Account status transition |
| `PAIRING_CREATED` | New client↔server pairing |
| `PAIRING_REMOVED` | Pairing deleted |
| `CALL_STARTED` | WebRTC tunnel established |
| `CALL_ENDED` | Tunnel session ended |

---

## Make Targets

```bash
make build          # Build everything (web + server + client + migrate)
make build-web      # Build React admin panel
make build-server   # Build server binary
make build-client   # Build client binary
make build-migrate  # Build migration tool
make docker         # Build Docker image
make docker-up      # Start Docker containers
make docker-down    # Stop Docker containers
make docker-logs    # Tail Docker logs
make run-server     # Run server (sudo)
make run-client     # Run client (sudo)
make migrate        # Run migration
make tidy           # go mod tidy
make clean          # Remove bin/ directory
```

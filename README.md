# BLE WebRTC Tunnel

تونل VPN بر بستر WebRTC پلتفرم بله — تمام ترافیک شبکه از طریق DataChannel رمزنگاری‌شده (DTLS/SCTP) منتقل می‌شود.

## معماری

```
┌─────────────────┐          ┌──────────────┐          ┌─────────────────┐
│  Client (Iran)  │          │  Bale TURN   │          │ Server (Abroad) │
│                 │          │   Servers    │          │                 │
│  TUN 10.0.0.2   │◄────────►│  meet-turn   │◄────────►│  TUN 10.0.0.1   │
│  ↕ DataChannel  │  WebRTC  │  .ble.ir     │  WebRTC  │  ↕ NAT/Forward  │
│  All Traffic    │  (DTLS)  │  Port 443    │  (DTLS)  │  → Internet     │
└─────────────────┘          └──────────────┘          └─────────────────┘
```

## نحوه کار

1. **سیگنالینگ**: کلاینت و سرور به WebSocket لایوکیت بله وصل می‌شوند و اطلاعات TURN را استخراج می‌کنند
2. **اتصال WebRTC**: از طریق سرورهای TURN بله، اتصال WebRTC برقرار می‌شود
3. **DataChannel**: یک کانال داده رمزنگاری‌شده ایجاد می‌شود
4. **تونل TUN**: ترافیک شبکه از TUN خوانده و روی DataChannel ارسال می‌شود
5. **NAT**: سرور ترافیک را به اینترنت فوروارد می‌کند

## نصب و راه‌اندازی

### پیش‌نیازها
- Go 1.22+
- Linux (برای TUN interface)
- دسترسی root (برای ساخت TUN و تنظیم routing)

### بیلد

```bash
# بیلد هر دو
make build

# یا جداگانه
make build-client
make build-server
```

### تنظیمات

فایل `.env.example` را به `.env` کپی کنید و مقادیر را تنظیم کنید:

```bash
cp .env.example .env
```

مقادیر مهم:
- `LIVEKIT_WS_URL` — آدرس WebSocket لایوکیت بله
- `LIVEKIT_ACCESS_TOKEN` — توکن دسترسی
- `SIGNAL_SERVER_ADDR` — آدرس عمومی سرور (فقط برای کلاینت)
- `ADMIN_USERNAME` / `ADMIN_PASSWORD` — اطلاعات ورود پنل مدیریت

### اجرای سرور

```bash
# مستقیم
sudo ROLE=server ./bin/server

# با داکر
docker-compose up -d
```

### اجرای کلاینت

```bash
sudo ROLE=client SIGNAL_SERVER_ADDR=YOUR_SERVER_IP:8080 ./bin/client
```

## پنل مدیریت

پنل وب روی پورت `8080` در دسترس است:
- **داشبورد**: وضعیت اتصال و آمار ترافیک
- **لاگ زنده**: مشاهده لاگ‌ها به صورت بلادرنگ
- **ترمینال**: اجرای دستورات لینوکسی روی سرور

## داکر

```bash
# ساخت ایمیج
docker-compose build

# اجرا
docker-compose up -d

# مشاهده لاگ
docker-compose logs -f
```

## امنیت

- تمام ترافیک DataChannel با DTLS/SCTP رمزنگاری می‌شود
- ترافیک از نظر فایروال شبیه ترافیک HTTPS است (پورت 443/TCP)
- سرور TURN بله ترافیک را relay می‌کند بدون امکان خواندن محتوا

## ساختار پروژه

```
├── cmd/
│   ├── client/main.go      # نقطه ورود کلاینت
│   └── server/main.go      # نقطه ورود سرور
├── internal/
│   ├── admin/               # پنل مدیریت وب
│   ├── config/              # تنظیمات از .env
│   ├── livekit/             # اتصال به WebSocket بله
│   ├── transport/           # WebRTC و DataChannel
│   └── tunnel/              # TUN interface
├── .env.example
├── Dockerfile
├── docker-compose.yml
└── Makefile
```

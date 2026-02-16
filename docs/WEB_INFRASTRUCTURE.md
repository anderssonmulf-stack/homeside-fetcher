# Web Infrastructure

The system is exposed publicly via **bvpro.hem.se** with multiple security layers.

## Security Stack

```
Internet
    ↓
┌─────────────────────────────────────────┐
│  Router Port Forwarding                 │
│  └── 80, 443 → 192.168.86.9            │
└─────────────────────────────────────────┘
    ↓
┌─────────────────────────────────────────┐
│  UFW Firewall                           │
│  └── Allow 80/tcp, 443/tcp             │
└─────────────────────────────────────────┘
    ↓
┌─────────────────────────────────────────┐
│  nginx                                  │
│  ├── GeoIP (Sweden only)               │  ← Blocks non-SE countries
│  ├── Rate limiting                      │  ← 1 req/s login, 10 req/s general
│  ├── Connection limit (10/IP)          │
│  ├── HTTPS (Let's Encrypt)             │
│  └── Basic auth                         │
└─────────────────────────────────────────┘
    ↓
┌─────────────────────────────────────────┐
│  fail2ban                               │
│  └── Ban IP after 3 failed logins      │  ← 1 hour ban
└─────────────────────────────────────────┘
    ↓
┌─────────────────────────────────────────┐
│  Flask/Gunicorn (localhost:5000)       │
└─────────────────────────────────────────┘
```

## nginx Reverse Proxy

Configuration files in `nginx/`:

| File | Purpose |
|------|---------|
| `bvpro.hem.se.conf` | Site config for the web GUI |
| `rate-limiting.conf` | Rate limit zones (login: 1 req/s, general: 10 req/s) |
| `geoip-sweden-only.conf` | GeoIP config to allow only Swedish IPs |
| `fail2ban-nginx.conf` | Auto-ban after 3 failed logins |
| `setup-nginx.sh` | Deployment script |

## Setup Instructions

**Initial setup (run once):**
```bash
# Install packages
sudo apt install nginx certbot python3-certbot-nginx apache2-utils fail2ban libnginx-mod-http-geoip geoip-database

# Deploy configs
sudo bash /opt/dev/homeside-fetcher/nginx/setup-nginx.sh

# Create user
sudo htpasswd -c /etc/nginx/.htpasswd USERNAME

# Open firewall
sudo ufw allow 80/tcp
sudo ufw allow 443/tcp

# Get SSL certificate
sudo certbot --nginx -d bvpro.hem.se

# Reload services
sudo systemctl reload nginx
sudo systemctl restart fail2ban
```

**Add more users:**
```bash
sudo htpasswd /etc/nginx/.htpasswd newuser
```

**Check fail2ban status:**
```bash
sudo fail2ban-client status nginx-http-auth
```

## Geo-Blocking

Only Swedish IPs (country code `SE`) are allowed. All other countries get connection dropped (HTTP 444).

Uses nginx GeoIP module with legacy GeoIP database (`/usr/share/GeoIP/GeoIP.dat`).

To allow additional countries, edit `/etc/nginx/conf.d/geoip-sweden-only.conf`:
```nginx
map $geoip_country_code $allowed_country {
    default 0;      # Block all by default
    SE      1;      # Sweden
    NO      1;      # Norway (example)
    ""      1;      # Local requests
}
```

## SSL Certificate Renewal

Let's Encrypt certificates auto-renew via certbot's systemd timer:
```bash
sudo certbot certificates
sudo systemctl status certbot.timer
```

## GUI Deployment

### systemd Service

The Flask app runs via Gunicorn, managed by systemd (`bvpro-gui.service`):

```ini
[Unit]
Description=Svenskeb Settings GUI
After=network.target

[Service]
Type=simple
User=ulf
WorkingDirectory=/opt/dev/homeside-fetcher/webgui
EnvironmentFile=/opt/dev/homeside-fetcher/webgui/.env
Environment="PATH=/opt/dev/homeside-fetcher/webgui/venv/bin"
ExecStart=/opt/dev/homeside-fetcher/webgui/venv/bin/gunicorn -b 127.0.0.1:5000 -w 2 app:app
Restart=always

[Install]
WantedBy=multi-user.target
```

### Commands

```bash
# Install/update service
sudo cp webgui/bvpro-gui.service /etc/systemd/system/
sudo systemctl daemon-reload
sudo systemctl enable bvpro-gui
sudo systemctl restart bvpro-gui

# View logs
sudo journalctl -u bvpro-gui -f

# Create admin user
cd /opt/dev/homeside-fetcher/webgui
source venv/bin/activate
python create_admin.py
```

### Environment Variables (webgui/.env)

```bash
SECRET_KEY=your-secret-key
SMTP_HOST=send.one.com
SMTP_PORT=587
SMTP_USER=info@bvpro.hem.se
SMTP_PASSWORD=your-password
ADMIN_EMAIL=admin@bvpro.hem.se
INFLUXDB_URL=http://localhost:8086
INFLUXDB_TOKEN=your-token
INFLUXDB_ORG=homeside
INFLUXDB_BUCKET=heating
```

### nginx Site Config

Site config at `/etc/nginx/sites-available/bvpro.hem.se`:
```nginx
server {
    listen 443 ssl;
    server_name bvpro.hem.se www.bvpro.hem.se;

    # SSL via Let's Encrypt
    ssl_certificate /etc/letsencrypt/live/bvpro.hem.se/fullchain.pem;
    ssl_certificate_key /etc/letsencrypt/live/bvpro.hem.se/privkey.pem;

    # GeoIP Sweden-only
    if ($allowed_country = 0) { return 444; }

    location / {
        proxy_pass http://127.0.0.1:5000;
        proxy_set_header Host $host;
        proxy_set_header X-Real-IP $remote_addr;
    }
}
```

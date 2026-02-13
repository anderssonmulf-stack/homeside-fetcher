#!/bin/bash
# Setup script for nginx reverse proxy with geo-blocking
# Run with: sudo bash setup-nginx.sh

set -e

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"

echo "=== Setting up nginx for svenskeb.se ==="

# Install GeoIP module and database
echo "1. Installing GeoIP module..."
apt-get install -y libnginx-mod-http-geoip geoip-database

# Copy rate limiting config
echo "2. Installing rate limiting config..."
cp "$SCRIPT_DIR/rate-limiting.conf" /etc/nginx/conf.d/rate-limiting.conf

# Copy GeoIP config
echo "3. Installing GeoIP config (Sweden only)..."
cp "$SCRIPT_DIR/geoip-sweden-only.conf" /etc/nginx/conf.d/geoip-sweden-only.conf

# Copy site configs
echo "4. Installing site configs..."
cp "$SCRIPT_DIR/svenskeb.se.conf" /etc/nginx/sites-available/svenskeb.se

# Enable sites
echo "5. Enabling sites..."
ln -sf /etc/nginx/sites-available/svenskeb.se /etc/nginx/sites-enabled/

# Copy fail2ban config
echo "6. Installing fail2ban config..."
cp "$SCRIPT_DIR/fail2ban-nginx.conf" /etc/fail2ban/jail.d/nginx.conf

# Test nginx config
echo "7. Testing nginx configuration..."
nginx -t

echo ""
echo "=== Setup complete! ==="
echo ""
echo "Next steps:"
echo "1. Create password file: sudo htpasswd -c /etc/nginx/.htpasswd USERNAME"
echo "2. Reload nginx: sudo systemctl reload nginx"
echo "3. Restart fail2ban: sudo systemctl restart fail2ban"
echo "4. Once DNS is ready, get SSL: sudo certbot --nginx -d svenskeb.se"
echo ""
echo "Geo-blocking: Only Swedish IPs (SE) are allowed."
echo "All other countries will get connection dropped (444)."
echo ""

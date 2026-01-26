#!/bin/bash
# Setup script for nginx reverse proxy
# Run with: sudo bash setup-nginx.sh

set -e

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"

echo "=== Setting up nginx for svenskeb.se ==="

# Copy rate limiting config
echo "1. Installing rate limiting config..."
cp "$SCRIPT_DIR/rate-limiting.conf" /etc/nginx/conf.d/rate-limiting.conf

# Copy site config
echo "2. Installing site config..."
cp "$SCRIPT_DIR/grafana.svenskeb.se.conf" /etc/nginx/sites-available/grafana.svenskeb.se

# Enable site
echo "3. Enabling site..."
ln -sf /etc/nginx/sites-available/grafana.svenskeb.se /etc/nginx/sites-enabled/

# Copy fail2ban config
echo "4. Installing fail2ban config..."
cp "$SCRIPT_DIR/fail2ban-nginx.conf" /etc/fail2ban/jail.d/nginx.conf

# Test nginx config
echo "5. Testing nginx configuration..."
nginx -t

echo ""
echo "=== Setup complete! ==="
echo ""
echo "Next steps:"
echo "1. Create password file: sudo htpasswd -c /etc/nginx/.htpasswd USERNAME"
echo "2. Reload nginx: sudo systemctl reload nginx"
echo "3. Restart fail2ban: sudo systemctl restart fail2ban"
echo "4. Once DNS is ready, get SSL: sudo certbot --nginx -d grafana.svenskeb.se"
echo ""

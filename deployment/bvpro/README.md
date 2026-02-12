# BVPro Deployment

Deploy the BVPro (BalansVärme Pro) instance alongside the existing SvenskEB deployment.
Both share the same codebase — only branding and config differ.

## First-time setup

1. **Copy and edit .env:**
   ```bash
   cp deployment/bvpro/.env.example deployment/bvpro/.env
   # Edit deployment/bvpro/.env with real credentials
   ```

2. **Replace placeholder logo:**
   ```bash
   # Replace with actual BVPro logo (PNG, ~200x60px recommended)
   cp /path/to/bvpro-logo.png webgui/static/images/themes/bvpro/logo.png
   ```

3. **Install systemd service:**
   ```bash
   sudo cp deployment/bvpro/bvpro-gui.service /etc/systemd/system/
   sudo systemctl daemon-reload
   sudo systemctl enable bvpro-gui
   sudo systemctl start bvpro-gui
   ```

4. **Install nginx config:**
   ```bash
   sudo cp deployment/bvpro/bvpro.hem.se.conf /etc/nginx/sites-available/
   sudo ln -s /etc/nginx/sites-available/bvpro.hem.se.conf /etc/nginx/sites-enabled/
   sudo certbot --nginx -d bvpro.hem.se
   sudo systemctl reload nginx
   ```

5. **Create admin user:**
   ```bash
   cd /opt/dev/homeside-fetcher/webgui
   source venv/bin/activate
   SITE_THEME=bvpro python create_admin.py
   ```

## Verify

```bash
sudo systemctl status bvpro-gui
curl -s https://bvpro.hem.se/login | grep "BVPro"
```

## Logs

```bash
sudo journalctl -u bvpro-gui -f
```

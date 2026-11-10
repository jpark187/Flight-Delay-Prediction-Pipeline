#!/usr/bin/env bash
# poller_startup.sh - VM startup script: install faa_poller.sh (passed in as metadata) as a
# systemd service, so it starts on boot and restarts if it ever crashes.
MD="http://metadata.google.internal/computeMetadata/v1/instance/attributes"
curl -fsS -H "Metadata-Flavor: Google" "$MD/poller-script" > /usr/local/bin/faa_poller.sh
chmod +x /usr/local/bin/faa_poller.sh
cat > /etc/systemd/system/faa-poller.service << 'UNIT'
[Unit]
Description=FAA NAS status poller
After=network-online.target
Wants=network-online.target

[Service]
ExecStart=/usr/local/bin/faa_poller.sh
Restart=always
RestartSec=30

[Install]
WantedBy=multi-user.target
UNIT
systemctl daemon-reload
systemctl enable --now faa-poller.service

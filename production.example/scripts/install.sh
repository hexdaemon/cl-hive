#!/bin/bash
#
# Install Hive AI Advisor systemd user service
#
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROD_DIR="$(dirname "$SCRIPT_DIR")"
HIVE_DIR="$(dirname "$PROD_DIR")"
SYSTEMD_USER_DIR="${HOME}/.config/systemd/user"

echo "Installing Hive AI Advisor systemd units..."

# Create systemd user directory if needed
mkdir -p "$SYSTEMD_USER_DIR"

# Update service file with actual paths
sed "s|%h/cl-hive|${HIVE_DIR}|g" "${PROD_DIR}/systemd/hive-advisor.service" > "${SYSTEMD_USER_DIR}/hive-advisor.service"

# Copy timer file
cp "${PROD_DIR}/systemd/hive-advisor.timer" "$SYSTEMD_USER_DIR/"

# Make scripts executable
chmod +x "${PROD_DIR}/scripts/run-advisor.sh"
chmod +x "${PROD_DIR}/scripts/health-check.sh" 2>/dev/null || true

# Reload systemd to pick up new units
systemctl --user daemon-reload

# Enable the timer (starts on boot)
systemctl --user enable hive-advisor.timer

# Start the timer now
systemctl --user start hive-advisor.timer

echo ""
echo "Installation complete!"
echo ""
echo "Commands:"
echo "  Check timer status:    systemctl --user status hive-advisor.timer"
echo "  View upcoming runs:    systemctl --user list-timers"
echo "  Manual trigger:        systemctl --user start hive-advisor.service"
echo "  View logs:             journalctl --user -u hive-advisor.service -f"
echo "  Stop timer:            systemctl --user stop hive-advisor.timer"
echo "  Disable timer:         systemctl --user disable hive-advisor.timer"
echo ""

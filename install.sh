#!/bin/bash
set -euo pipefail

echo "Installing bugcam..."

if [ ! -f /etc/rpi-issue ]; then
    echo "Warning: This doesn't appear to be Raspberry Pi OS"
fi

sudo apt update
sudo apt install -y hailo-all pipx i2c-tools libcjson-dev build-essential python3-picamera2

# picamera2 needs the system apt package (its libcamera bindings are compiled
# against this Pi's specific camera stack -- it isn't a normal pip wheel), so
# the pipx venv needs to see system site-packages or `import picamera2` fails
# even though the apt package is installed. --force so re-running this script
# on a device already set up under the old (non-system-site-packages) install
# actually recreates the venv instead of a no-op "already installed".
pipx install --system-site-packages --force bugcam
pipx ensurepath

echo ""
echo "Installation complete!"
echo "Close and reopen your terminal, then run:"
echo "  bugcam setup"

#!/usr/bin/env bash
#
# Update the Proxmox JIT Runners LXC: pull latest code, sync deps, restart.
# Must be run as root inside the container:
#
#   bash /opt/runner-orchestrator/deploy/update.sh
#
set -euo pipefail

APP_DIR=/opt/runner-orchestrator
SERVICE=runner-orchestrator
UV="${UV_BIN:-$(command -v uv 2>/dev/null || echo /root/.local/bin/uv)}"

log() { printf '\n\033[1;36m==> %s\033[0m\n' "$*"; }

if [ "$(id -u)" -ne 0 ]; then
  echo "This script must be run as root." >&2
  exit 1
fi

log "Fetching latest code (main)"
git config --global --add safe.directory "$APP_DIR" || true
git -C "$APP_DIR" fetch origin main --tags --prune

# A fast-forward is the normal case. It stops being possible if upstream history
# was rewritten (e.g. to purge something that should never have been committed),
# and a deploy target is a pure checkout with nothing local worth keeping — so
# match the remote exactly rather than failing and requiring a manual re-clone.
if git -C "$APP_DIR" merge-base --is-ancestor HEAD origin/main 2>/dev/null; then
  log "Fast-forwarding to origin/main"
  git -C "$APP_DIR" merge --ff-only origin/main
else
  log "History diverged from origin/main; resetting this checkout to match"
  git -C "$APP_DIR" reset --hard origin/main
  git -C "$APP_DIR" clean -fd
fi

log "Syncing dependencies (uv sync --no-dev)"
cd "$APP_DIR"
"$UV" sync --no-dev

log "Reinstalling systemd units (if changed)"
if [ -f "$APP_DIR/deploy/systemd/$SERVICE.service" ]; then
  install -m 644 "$APP_DIR/deploy/systemd/$SERVICE.service" \
    /etc/systemd/system/
  systemctl daemon-reload
fi

log "Restarting service"
systemctl restart "$SERVICE"
echo "Check status: systemctl status $SERVICE"

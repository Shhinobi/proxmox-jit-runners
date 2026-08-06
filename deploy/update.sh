#!/usr/bin/env bash
#
# Update do Proxmox JIT Runners no LXC: pull, reinstala deps, reinicia serviço.
# Execute como root dentro do container:
#
#   bash /opt/runner-orchestrator/deploy/update.sh
#
set -euo pipefail

APP_DIR=/opt/runner-orchestrator
SERVICE=runner-orchestrator
VENV="$APP_DIR/.venv"
REQUIREMENTS="$APP_DIR/requirements.lock"

log() { printf '\n\033[1;36m==> %s\033[0m\n' "$*"; }

if [ "$(id -u)" -ne 0 ]; then
  echo "Este script deve ser executado como root." >&2
  exit 1
fi

log "Pulling latest code (main)"
git config --global --add safe.directory "$APP_DIR" || true
git -C "$APP_DIR" pull --ff-only

log "Reinstalando dependências"
"$VENV/bin/pip" install --quiet -r "$REQUIREMENTS"

log "Reinstalando systemd units (se mudaram)"
if [ -f "$APP_DIR/deploy/systemd/$SERVICE.service" ]; then
  install -m 644 "$APP_DIR/deploy/systemd/$SERVICE.service" \
    /etc/systemd/system/
  systemctl daemon-reload
fi

log "Reiniciando serviço"
systemctl restart "$SERVICE"
echo "Verifique: systemctl status $SERVICE"

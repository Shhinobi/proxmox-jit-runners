# ─── proxmox-jit-runners deploy helpers ───────────────────────────────────────
# Usage: make <target>
#
# Overridable environment variables:
#   DEPLOY_HOST   SSH host of the container (default: root@10.0.1.60)
#
DEPLOY_HOST   ?= root@10.0.1.60
DEPLOY_SCRIPT  = /opt/runner-orchestrator/deploy/update.sh
REMOTE_APP     = /opt/runner-orchestrator
SERVICE        = runner-orchestrator

# Current local branch (evaluated once)
CURRENT_BRANCH := $(shell git rev-parse --abbrev-ref HEAD)

.PHONY: build build-remote deploy ssh logs status restart rollback help

## ── build ────────────────────────────────────────────────────────────────────
## Local check: sync venv and run ruff + mypy (lint/typecheck).
## Use this to validate the code before committing or deploying.
build:
	@echo "\033[1;36m==> Syncing local venv...\033[0m"
	uv sync
	@echo "\033[1;36m==> Lint (ruff)...\033[0m"
	uv run ruff check orchestrator/
	@echo "\033[1;36m==> Typecheck (mypy)...\033[0m"
	uv run mypy orchestrator/
	@echo "\033[1;32m✔  Local build done!\033[0m"

## ── build-remote ─────────────────────────────────────────────────────────────
## Sync dependencies on the remote host WITHOUT restarting the service.
build-remote:
	@echo "\033[1;36m==> Syncing deps on $(DEPLOY_HOST)...\033[0m"
	ssh $(DEPLOY_HOST) "cd $(REMOTE_APP) && /root/.local/bin/uv sync --no-dev"
	@echo "\033[1;32m✔  Remote deps updated!\033[0m"

## ── deploy ────────────────────────────────────────────────────────────────────
## Push current branch + pull + sync deps + restart on the remote host.
deploy:
	@echo "\033[1;36m==> Checking working tree...\033[0m"
	@if ! git diff --quiet || ! git diff --cached --quiet; then \
	  echo "\033[1;31mERROR: uncommitted changes present. Commit before deploying.\033[0m" >&2; \
	  exit 1; \
	fi
	@echo "\033[1;36m==> Pushing branch '$(CURRENT_BRANCH)'...\033[0m"
	git push origin HEAD
	@echo "\033[1;36m==> Running deploy on $(DEPLOY_HOST)...\033[0m"
	ssh $(DEPLOY_HOST) "bash $(DEPLOY_SCRIPT)"
	@echo "\033[1;32m✔  Deploy complete!\033[0m"

## ── ssh ───────────────────────────────────────────────────────────────────────
## Open an interactive SSH shell on the remote host.
ssh:
	ssh $(DEPLOY_HOST)

## ── logs ──────────────────────────────────────────────────────────────────────
## Follow live logs from the service.
logs:
	ssh -t $(DEPLOY_HOST) "journalctl -u $(SERVICE) -f --no-pager"

## ── status ────────────────────────────────────────────────────────────────────
## Show the service status on the remote host.
status:
	ssh $(DEPLOY_HOST) "systemctl status $(SERVICE) --no-pager"

## ── restart ───────────────────────────────────────────────────────────────────
## Restart the service WITHOUT syncing deps (e.g. after editing config).
restart:
	@echo "\033[1;36m==> Restarting $(SERVICE) on $(DEPLOY_HOST)...\033[0m"
	ssh $(DEPLOY_HOST) "systemctl restart $(SERVICE)"
	@echo "\033[1;32m✔  Service restarted.\033[0m"

## ── rollback ──────────────────────────────────────────────────────────────────
## Revert the last commit on the remote host and restart.
rollback:
	@echo "\033[1;33m==> Reverting last commit on $(DEPLOY_HOST)...\033[0m"
	ssh $(DEPLOY_HOST) "git -C $(REMOTE_APP) revert --no-edit HEAD && \
	  bash $(DEPLOY_SCRIPT)"
	@echo "\033[1;32m✔  Rollback complete.\033[0m"

## ── help ──────────────────────────────────────────────────────────────────────
help:
	@echo ""
	@echo "  \033[1mproxmox-jit-runners — available targets\033[0m"
	@echo ""
	@echo "  make build          Lint + typecheck locally (ruff + mypy via uv)"
	@echo "  make build-remote   Sync deps on host WITHOUT restarting"
	@echo "  make deploy         Push + pull + sync deps + restart on host"
	@echo "  make ssh            Open SSH shell on host"
	@echo "  make logs           Follow live logs (journalctl -f)"
	@echo "  make status         Show systemd service status"
	@echo "  make restart        Restart without syncing deps"
	@echo "  make rollback       Revert last commit on host and restart"
	@echo ""
	@echo "  Default host: $(DEPLOY_HOST)"
	@echo "  Current branch: $(CURRENT_BRANCH)"
	@echo ""

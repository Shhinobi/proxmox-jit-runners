# ─── proxmox-jit-runners deploy helpers ───────────────────────────────────────
# Uso: make <target>
#
# Variáveis de ambiente sobrescrevíveis:
#   DEPLOY_HOST   host SSH do container (padrão: root@10.0.1.60)
#
DEPLOY_HOST   ?= root@10.0.1.60
DEPLOY_SCRIPT  = /opt/runner-orchestrator/deploy/update.sh
REMOTE_APP     = /opt/runner-orchestrator
SERVICE        = runner-orchestrator
VENV           = .venv

# Branch local corrente (calculada uma única vez)
CURRENT_BRANCH := $(shell git rev-parse --abbrev-ref HEAD)

.PHONY: build build-remote deploy ssh logs status restart rollback help

## ── build ────────────────────────────────────────────────────────────────────
## "Build" local: sincroniza venv e roda mypy + ruff (lint/typecheck).
## Útil para validar o código antes de commitar/fazer deploy.
build:
	@echo "\033[1;36m==> Sincronizando venv local...\033[0m"
	uv sync
	@echo "\033[1;36m==> Lint (ruff)...\033[0m"
	uv run ruff check orchestrator/
	@echo "\033[1;36m==> Typecheck (mypy)...\033[0m"
	uv run mypy orchestrator/
	@echo "\033[1;32m✔  Build local concluído!\033[0m"

## ── build-remote ─────────────────────────────────────────────────────────────
## Reinstala deps no host SEM reiniciar o serviço.
build-remote:
	@echo "\033[1;36m==> Reinstalando deps em $(DEPLOY_HOST)...\033[0m"
	ssh $(DEPLOY_HOST) "$(REMOTE_APP)/$(VENV)/bin/pip install --quiet \
	  -r $(REMOTE_APP)/requirements.lock"
	@echo "\033[1;32m✔  Deps remotas atualizadas!\033[0m"

## ── deploy ────────────────────────────────────────────────────────────────────
## Push da branch atual + pull + reinstala deps + restart no host.
deploy:
	@echo "\033[1;36m==> Verificando working tree...\033[0m"
	@if ! git diff --quiet || ! git diff --cached --quiet; then \
	  echo "\033[1;31mERRO: há mudanças não commitadas. Faça commit antes de deploiar.\033[0m" >&2; \
	  exit 1; \
	fi
	@echo "\033[1;36m==> Push da branch '$(CURRENT_BRANCH)'...\033[0m"
	git push origin HEAD
	@echo "\033[1;36m==> Executando deploy em $(DEPLOY_HOST)...\033[0m"
	ssh $(DEPLOY_HOST) "bash $(DEPLOY_SCRIPT)"
	@echo "\033[1;32m✔  Deploy concluído!\033[0m"

## ── ssh ───────────────────────────────────────────────────────────────────────
## Abre um shell interativo no host.
ssh:
	ssh $(DEPLOY_HOST)

## ── logs ──────────────────────────────────────────────────────────────────────
## Acompanha os logs ao vivo do serviço.
logs:
	ssh -t $(DEPLOY_HOST) "journalctl -u $(SERVICE) -f --no-pager"

## ── status ────────────────────────────────────────────────────────────────────
## Exibe o status do serviço no host.
status:
	ssh $(DEPLOY_HOST) "systemctl status $(SERVICE) --no-pager"

## ── restart ───────────────────────────────────────────────────────────────────
## Reinicia o serviço no host SEM reinstalar deps (útil para recarregar config).
restart:
	@echo "\033[1;36m==> Reiniciando $(SERVICE) em $(DEPLOY_HOST)...\033[0m"
	ssh $(DEPLOY_HOST) "systemctl restart $(SERVICE)"
	@echo "\033[1;32m✔  Serviço reiniciado.\033[0m"

## ── rollback ──────────────────────────────────────────────────────────────────
## Reverte o último commit no host e reinicia.
rollback:
	@echo "\033[1;33m==> Revertendo o último commit em $(DEPLOY_HOST)...\033[0m"
	ssh $(DEPLOY_HOST) "git -C $(REMOTE_APP) revert --no-edit HEAD && \
	  bash $(DEPLOY_SCRIPT)"
	@echo "\033[1;32m✔  Rollback concluído.\033[0m"

## ── help ──────────────────────────────────────────────────────────────────────
help:
	@echo ""
	@echo "  \033[1mproxmox-jit-runners — targets disponíveis\033[0m"
	@echo ""
	@echo "  make build          Lint + typecheck local (ruff + mypy via uv)"
	@echo "  make build-remote   Reinstala deps no host SEM reiniciar"
	@echo "  make deploy         Push + pull + deps + restart no host"
	@echo "  make ssh            Abre shell SSH no host"
	@echo "  make logs           Logs ao vivo (journalctl -f)"
	@echo "  make status         Status do serviço systemd"
	@echo "  make restart        Restart sem reinstalar deps"
	@echo "  make rollback       Reverte último commit no host e reinicia"
	@echo ""
	@echo "  Host padrão: $(DEPLOY_HOST)"
	@echo "  Branch atual: $(CURRENT_BRANCH)"
	@echo ""

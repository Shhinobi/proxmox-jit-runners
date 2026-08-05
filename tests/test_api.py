from __future__ import annotations

import re
from pathlib import Path
from unittest import mock

import pytest

from orchestrator.api import (
    build_alerts,
    build_overview,
    metrics_summary,
    public_policies,
    set_drain,
    ui_directory,
)
from orchestrator.config import ConfigError, load_settings, save_repository_policies
from orchestrator.eventlog import EventLog
from tests.helpers import settings


def test_default_config_starts_in_shadow_mode() -> None:
    loaded = load_settings(Path("orchestrator/default-config.yaml"))
    assert loaded.shadow_mode
    assert "acme/example-app" in loaded.repositories
    assert "acme/example-service" in loaded.repositories
    privacy = loaded.repositories["acme/example-service"]
    assert privacy.default_mode == "ephemeral"
    assert privacy.warm_capacity == 2
    assert loaded.capacity.max_active >= 3  # ninja 1 + privacy 2
    assert loaded.github.installations["acme-app"].account == "acme"


def test_public_policies_exclude_secrets() -> None:
    data = public_policies(settings())
    assert "OldOwner/example-app" in data["repositories"]
    assert "OldOwner/example-app-Temp" not in data["repositories"]
    serialized = str(data)
    assert "private_key" not in serialized
    assert "webhook_secret" not in serialized
    assert "api_token" not in serialized
    prod = data["repositories"]["OldOwner/example-app"]
    assert prod["default_mode"] == "ephemeral"
    assert prod["warm_capacity"] == 0  # helper fixture default
    assert "capacity" in data
    assert "max_active" in data["capacity"]
    assert "pool_size" in data["capacity"]


def test_public_policies_includes_profiles_for_dashboard_selector() -> None:
    data = public_policies(settings())
    assert "profiles" in data
    assert "crossbuild-small" in data["profiles"]
    spec = data["profiles"]["crossbuild-small"]
    assert set(spec.keys()) == {"cores", "memory_mb", "swap_mb", "rootfs_gb"}


def test_ui_directory_contains_dashboard_assets() -> None:
    ui_dir = ui_directory()
    assert (ui_dir / "index.html").is_file()
    assert (ui_dir / "app.js").is_file()
    assert (ui_dir / "styles.css").is_file()
    html = (ui_dir / "index.html").read_text(encoding="utf-8")
    assert "Runner Orchestrator" in html
    js = Path(ui_dir / "app.js").read_text(encoding="utf-8")
    assert "/api/v1/events" in js
    assert "github_job_url" in js
    assert "github_run_url" in js
    assert "admin/drain" in js
    assert "confirmAction" in js
    assert "confirm-modal" in html
    assert re.search(r"(?<![.\w])confirm\s*\(", js) is None
    assert 'data-tab="logs"' in html
    assert "renderLogs" in js
    assert "snapshot.logs" in js
    assert "operationalBadge" in js
    assert "draining" in js
    assert "install-editor" in html
    assert "capacity-editor" in html
    assert "pullPoliciesFromDom" in js
    assert "loadPoliciesFromApi" in js
    assert "renderCapacityEditor" in js
    assert "max_active" in js
    assert "repo-card" in js
    assert "installation_id" in js
    assert 'data-tab="cache"' in html
    assert "loadCacheInventory" in js
    assert "/api/v1/admin/cache/purge" in js


def test_build_alerts_for_drain_and_delete_failed() -> None:
    overview = {
        "ready": True,
        "checks": {},
        "drain": True,
        "shadow_mode": False,
        "capacity": {
            "queued": 2,
            "oldest_queued_job_age_seconds": 120,
            "warm_ready": 0,
            "warm_target": 2,
            "warm_provisioning": 0,
        },
    }
    instances = [
        {
            "vmid": 5001,
            "status": "delete_failed",
            "error": "GitHub API 409: Already exists",
        }
    ]
    alerts = build_alerts(overview, instances, {"cleanup_failures_total": {}})
    codes = {a["code"] for a in alerts}
    assert "drain" in codes
    assert "queue_stale" in codes
    assert "warm_missing" in codes
    assert "delete_failed" in codes
    delete_alert = next(a for a in alerts if a["code"] == "delete_failed")
    assert "409" in delete_alert["message"]


async def test_operational_status_prefers_draining_over_ready() -> None:
    class _FakeDb:
        async def status_summary(self):
            return {"jobs": [], "instances": []}

        async def count_active(self):
            return 0

        async def count_ready_pool(self):
            return 0

        async def oldest_queued_job_age(self):
            return 0

    class _FakeOrch:
        draining = True
        db = _FakeDb()

        async def readiness(self):
            return True, {"database": True, "secrets": True, "cache": True}

    class _Settings:
        shadow_mode = False
        repositories = {}
        capacity = type("C", (), {"max_active": 2, "pool_size": 1})()
        proxmox = type(
            "P",
            (),
            {
                "node": "pve",
                "template_vmid": 9101,
                "vmid_start": 5000,
                "vmid_end": 5099,
            },
        )()

    overview = await build_overview(_FakeOrch(), _Settings())  # type: ignore[arg-type]
    assert overview["ready"]
    assert overview["drain"]
    assert overview["status"] == "draining"


def test_warm_missing_suppressed_while_provisioning() -> None:
    overview = {
        "ready": True,
        "checks": {},
        "drain": False,
        "shadow_mode": False,
        "capacity": {
            "queued": 0,
            "oldest_queued_job_age_seconds": 0,
            "warm_ready": 0,
            "warm_target": 2,
            "warm_provisioning": 1,
        },
    }
    alerts = build_alerts(overview, [], {"cleanup_failures_total": {}})
    codes = {a["code"] for a in alerts}
    assert "warm_missing" not in codes
    assert "warm_replenishing" in codes


def test_event_log_ring_buffer() -> None:
    log = EventLog(maxlen=3)
    log.emit("info", "system", "one")
    log.emit("warn", "warm", "two", vmid=5000)
    log.emit("error", "destroy", "three", detail="traceback here")
    log.emit("info", "job", "four", job_id=1)
    items = log.list(limit=10)
    assert len(items) == 3
    assert items[0]["message"] == "four"
    errors = log.list(level="error")
    assert len(errors) == 1
    assert "traceback" in errors[0]["detail"]
    warm = log.list(category="warm")
    assert len(warm) == 1
    assert warm[0]["context"]["vmid"] == 5000


def test_metrics_summary_is_dict() -> None:
    summary = metrics_summary()
    assert "provision_seconds_avg" in summary
    assert "webhooks_total" in summary


def test_set_drain_toggles_marker(tmp_path: Path) -> None:
    marker = tmp_path / "drain"
    with mock.patch("orchestrator.api.DRAIN_MARKER", marker):
        assert set_drain(True)["drain"]
        assert marker.exists()
        assert not set_drain(False)["drain"]
        assert not marker.exists()


def test_save_repository_policies_round_trip(tmp_path: Path) -> None:
    source = Path("orchestrator/default-config.yaml")
    cfg = tmp_path / "config.yaml"
    db = tmp_path / "state.db"
    text = source.read_text(encoding="utf-8")
    text = text.replace(
        "database_path: /var/lib/runner-orchestrator/state.db",
        f"database_path: {db}",
    )
    cfg.write_text(text, encoding="utf-8")
    repos = {
        "acme/example-app": {
            "github_installation": "acme-app",
            "default_mode": "ephemeral",
            "warm_capacity": 2,
            "allowed_modes": ["ephemeral", "pool"],
            "allowed_events": ["push", "workflow_dispatch"],
            "required_labels": ["self-hosted", "proxmox-lxc", "crossbuild"],
        },
        "acme/example-service": {
            "github_installation": "acme-app",
            "default_mode": "pool",
            "warm_capacity": 0,
            "allowed_modes": ["ephemeral", "pool", "persistent"],
            "allowed_events": ["push", "workflow_dispatch", "schedule"],
            "required_labels": ["self-hosted", "proxmox-lxc", "crossbuild"],
        },
    }
    loaded = save_repository_policies(repos, cfg, capacity={"max_active": 4, "pool_size": 1})
    assert loaded.repositories["acme/example-app"].warm_capacity == 2
    assert loaded.repositories["acme/example-service"].default_mode == "pool"
    assert loaded.capacity.max_active == 4
    assert loaded.capacity.pool_size == 1
    override = tmp_path / "repositories-override.yaml"
    assert override.is_file()
    override_text = override.read_text(encoding="utf-8")
    assert "max_active" in override_text
    again = load_settings(cfg)
    assert again.repositories["acme/example-app"].default_mode == "ephemeral"
    assert again.capacity.max_active == 4
    with pytest.raises(ConfigError):
        save_repository_policies(
            {
                "WrongOwner/Repo": {
                    "github_installation": "acme-app",
                    "default_mode": "ephemeral",
                    "warm_capacity": 0,
                    "allowed_modes": ["ephemeral"],
                    "allowed_events": ["push"],
                    "required_labels": ["self-hosted"],
                }
            },
            cfg,
        )
    with pytest.raises(ConfigError):
        save_repository_policies(
            {
                "acme/example-app": {
                    "github_installation": "acme-app",
                    "default_mode": "ephemeral",
                    "warm_capacity": 5,
                    "allowed_modes": ["ephemeral"],
                    "allowed_events": ["push"],
                    "required_labels": ["self-hosted", "proxmox-lxc", "crossbuild"],
                }
            },
            cfg,
            capacity={"max_active": 2, "pool_size": 1},
        )


def test_save_policies_can_update_installation_account(tmp_path: Path) -> None:
    source = Path("orchestrator/default-config.yaml")
    cfg = tmp_path / "config.yaml"
    db = tmp_path / "state.db"
    text = source.read_text(encoding="utf-8")
    text = text.replace(
        "database_path: /var/lib/runner-orchestrator/state.db",
        f"database_path: {db}",
    )
    cfg.write_text(text, encoding="utf-8")
    installations = {
        "acme-personal": {
            "account": "acme",
            "installation_id": 1234567,
            "scope": "repository",
            "runner_group_id": 1,
        }
    }
    repos = {
        "acme/example-app": {
            "github_installation": "acme-personal",
            "default_mode": "ephemeral",
            "warm_capacity": 2,
            "allowed_modes": ["ephemeral", "pool", "persistent"],
            "allowed_events": ["push", "workflow_dispatch", "schedule"],
            "required_labels": ["self-hosted", "proxmox-lxc", "crossbuild"],
        }
    }
    loaded = save_repository_policies(repos, cfg, installations=installations)
    assert "acme-personal" in loaded.github.installations
    assert loaded.github.installations["acme-personal"].account == "acme"
    assert "acme/example-app" in loaded.repositories
    again = load_settings(cfg)
    assert again.github.installations["acme-personal"].account == "acme"
    assert (
        again.repositories["acme/example-app"].github_installation == "acme-personal"
    )

from __future__ import annotations

import unittest
from pathlib import Path

import tempfile
from pathlib import Path
from unittest import mock

from orchestrator.api import build_alerts, metrics_summary, public_policies, set_drain, ui_directory
from orchestrator.config import ConfigError, load_settings, save_repository_policies
from tests.helpers import settings


class ApiHelperTests(unittest.TestCase):
    def test_default_config_starts_in_shadow_mode(self) -> None:
        loaded = load_settings(Path("orchestrator/default-config.yaml"))
        self.assertTrue(loaded.shadow_mode)
        self.assertIn("JustShinobi/NinjaCrawler", loaded.repositories)
        self.assertIn("JustShinobi/privacy-leak-go", loaded.repositories)
        privacy = loaded.repositories["JustShinobi/privacy-leak-go"]
        self.assertEqual(privacy.default_mode, "ephemeral")
        self.assertEqual(privacy.warm_capacity, 2)
        self.assertGreaterEqual(loaded.capacity.max_active, 3)  # ninja 1 + privacy 2
        self.assertEqual(
            loaded.github.installations["metaldevops-personal"].account, "JustShinobi"
        )

    def test_public_policies_exclude_secrets(self) -> None:
        data = public_policies(settings())
        self.assertIn("MetalDevops/NinjaCrawler", data["repositories"])
        self.assertNotIn("MetalDevops/NinjaCrawler-Temp", data["repositories"])
        serialized = str(data)
        self.assertNotIn("private_key", serialized)
        self.assertNotIn("webhook_secret", serialized)
        self.assertNotIn("api_token", serialized)
        prod = data["repositories"]["MetalDevops/NinjaCrawler"]
        self.assertEqual(prod["default_mode"], "ephemeral")
        self.assertEqual(prod["warm_capacity"], 0)  # helper fixture default
        self.assertIn("capacity", data)
        self.assertIn("max_active", data["capacity"])
        self.assertIn("pool_size", data["capacity"])

    def test_public_policies_includes_profiles_for_dashboard_selector(self) -> None:
        data = public_policies(settings())
        self.assertIn("profiles", data)
        self.assertIn("crossbuild-small", data["profiles"])
        spec = data["profiles"]["crossbuild-small"]
        self.assertEqual(set(spec.keys()), {"cores", "memory_mb", "swap_mb", "rootfs_gb"})

    def test_ui_directory_contains_dashboard_assets(self) -> None:
        ui_dir = ui_directory()
        self.assertTrue((ui_dir / "index.html").is_file())
        self.assertTrue((ui_dir / "app.js").is_file())
        self.assertTrue((ui_dir / "styles.css").is_file())
        html = (ui_dir / "index.html").read_text(encoding="utf-8")
        self.assertIn("Runner Orchestrator", html)
        js = Path(ui_dir / "app.js").read_text(encoding="utf-8")
        self.assertIn("/api/v1/events", js)
        self.assertIn("github_job_url", js)
        self.assertIn("github_run_url", js)
        self.assertIn("admin/drain", js)
        self.assertIn("confirmAction", js)
        self.assertIn("confirm-modal", html)
        self.assertNotRegex(js, r"(?<![.\w])confirm\s*\(")
        self.assertIn('data-tab="logs"', html)
        self.assertIn("renderLogs", js)
        self.assertIn("snapshot.logs", js)
        self.assertIn("operationalBadge", js)
        self.assertIn("draining", js)
        self.assertIn("install-editor", html)
        self.assertIn("capacity-editor", html)
        self.assertIn("pullPoliciesFromDom", js)
        self.assertIn("loadPoliciesFromApi", js)
        self.assertIn("renderCapacityEditor", js)
        self.assertIn("max_active", js)
        self.assertIn("repo-card", js)
        self.assertIn("installation_id", js)
        self.assertIn('data-tab="cache"', html)
        self.assertIn("loadCacheInventory", js)
        self.assertIn("/api/v1/admin/cache/purge", js)

    def test_build_alerts_for_drain_and_delete_failed(self) -> None:
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
        self.assertIn("drain", codes)
        self.assertIn("queue_stale", codes)
        self.assertIn("warm_missing", codes)
        self.assertIn("delete_failed", codes)
        delete_alert = next(a for a in alerts if a["code"] == "delete_failed")
        self.assertIn("409", delete_alert["message"])

    def test_operational_status_prefers_draining_over_ready(self) -> None:
        import asyncio

        from orchestrator.api import build_overview

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
                    "node": "pve02",
                    "template_vmid": 9101,
                    "vmid_start": 5000,
                    "vmid_end": 5099,
                },
            )()

        overview = asyncio.run(build_overview(_FakeOrch(), _Settings()))  # type: ignore[arg-type]
        self.assertTrue(overview["ready"])
        self.assertTrue(overview["drain"])
        self.assertEqual(overview["status"], "draining")

    def test_warm_missing_suppressed_while_provisioning(self) -> None:
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
        self.assertNotIn("warm_missing", codes)
        self.assertIn("warm_replenishing", codes)

    def test_event_log_ring_buffer(self) -> None:
        from orchestrator.eventlog import EventLog

        log = EventLog(maxlen=3)
        log.emit("info", "system", "one")
        log.emit("warn", "warm", "two", vmid=5000)
        log.emit("error", "destroy", "three", detail="traceback here")
        log.emit("info", "job", "four", job_id=1)
        items = log.list(limit=10)
        self.assertEqual(len(items), 3)
        self.assertEqual(items[0]["message"], "four")
        errors = log.list(level="error")
        self.assertEqual(len(errors), 1)
        self.assertIn("traceback", errors[0]["detail"])
        warm = log.list(category="warm")
        self.assertEqual(len(warm), 1)
        self.assertEqual(warm[0]["context"]["vmid"], 5000)

    def test_metrics_summary_is_dict(self) -> None:
        summary = metrics_summary()
        self.assertIn("provision_seconds_avg", summary)
        self.assertIn("webhooks_total", summary)

    def test_set_drain_toggles_marker(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            marker = Path(tmp) / "drain"
            with mock.patch("orchestrator.api.DRAIN_MARKER", marker):
                self.assertTrue(set_drain(True)["drain"])
                self.assertTrue(marker.exists())
                self.assertFalse(set_drain(False)["drain"])
                self.assertFalse(marker.exists())

    def test_save_repository_policies_round_trip(self) -> None:
        source = Path("orchestrator/default-config.yaml")
        with tempfile.TemporaryDirectory() as tmp:
            cfg = Path(tmp) / "config.yaml"
            db = Path(tmp) / "state.db"
            text = source.read_text(encoding="utf-8")
            text = text.replace(
                "database_path: /var/lib/runner-orchestrator/state.db",
                f"database_path: {db}",
            )
            cfg.write_text(text, encoding="utf-8")
            repos = {
                "JustShinobi/NinjaCrawler": {
                    "github_installation": "metaldevops-personal",
                    "default_mode": "ephemeral",
                    "warm_capacity": 2,
                    "allowed_modes": ["ephemeral", "pool"],
                    "allowed_events": ["push", "workflow_dispatch"],
                    "required_labels": ["self-hosted", "proxmox-lxc", "crossbuild"],
                },
                "JustShinobi/privacy-leak-go": {
                    "github_installation": "metaldevops-personal",
                    "default_mode": "pool",
                    "warm_capacity": 0,
                    "allowed_modes": ["ephemeral", "pool", "persistent"],
                    "allowed_events": ["push", "workflow_dispatch", "schedule"],
                    "required_labels": ["self-hosted", "proxmox-lxc", "crossbuild"],
                },
            }
            loaded = save_repository_policies(
                repos, cfg, capacity={"max_active": 4, "pool_size": 1}
            )
            self.assertEqual(loaded.repositories["JustShinobi/NinjaCrawler"].warm_capacity, 2)
            self.assertEqual(loaded.repositories["JustShinobi/privacy-leak-go"].default_mode, "pool")
            self.assertEqual(loaded.capacity.max_active, 4)
            self.assertEqual(loaded.capacity.pool_size, 1)
            override = Path(tmp) / "repositories-override.yaml"
            self.assertTrue(override.is_file())
            override_text = override.read_text(encoding="utf-8")
            self.assertIn("max_active", override_text)
            again = load_settings(cfg)
            self.assertEqual(again.repositories["JustShinobi/NinjaCrawler"].default_mode, "ephemeral")
            self.assertEqual(again.capacity.max_active, 4)
            with self.assertRaises(ConfigError):
                save_repository_policies(
                    {
                        "WrongOwner/Repo": {
                            "github_installation": "metaldevops-personal",
                            "default_mode": "ephemeral",
                            "warm_capacity": 0,
                            "allowed_modes": ["ephemeral"],
                            "allowed_events": ["push"],
                            "required_labels": ["self-hosted"],
                        }
                    },
                    cfg,
                )
            with self.assertRaises(ConfigError):
                save_repository_policies(
                    {
                        "JustShinobi/NinjaCrawler": {
                            "github_installation": "metaldevops-personal",
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

    def test_save_policies_can_update_installation_account(self) -> None:
        source = Path("orchestrator/default-config.yaml")
        with tempfile.TemporaryDirectory() as tmp:
            cfg = Path(tmp) / "config.yaml"
            db = Path(tmp) / "state.db"
            text = source.read_text(encoding="utf-8")
            text = text.replace(
                "database_path: /var/lib/runner-orchestrator/state.db",
                f"database_path: {db}",
            )
            cfg.write_text(text, encoding="utf-8")
            installations = {
                "justshinobi-personal": {
                    "account": "JustShinobi",
                    "installation_id": 146433491,
                    "scope": "repository",
                    "runner_group_id": 1,
                }
            }
            repos = {
                "JustShinobi/NinjaCrawler": {
                    "github_installation": "justshinobi-personal",
                    "default_mode": "ephemeral",
                    "warm_capacity": 2,
                    "allowed_modes": ["ephemeral", "pool", "persistent"],
                    "allowed_events": ["push", "workflow_dispatch", "schedule"],
                    "required_labels": ["self-hosted", "proxmox-lxc", "crossbuild"],
                }
            }
            loaded = save_repository_policies(repos, cfg, installations=installations)
            self.assertIn("justshinobi-personal", loaded.github.installations)
            self.assertEqual(
                loaded.github.installations["justshinobi-personal"].account, "JustShinobi"
            )
            self.assertIn("JustShinobi/NinjaCrawler", loaded.repositories)
            again = load_settings(cfg)
            self.assertEqual(
                again.github.installations["justshinobi-personal"].account, "JustShinobi"
            )
            self.assertEqual(
                again.repositories["JustShinobi/NinjaCrawler"].github_installation,
                "justshinobi-personal",
            )

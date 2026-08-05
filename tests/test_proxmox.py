from __future__ import annotations

import unittest
from typing import Any
from unittest import mock
from urllib.parse import quote

from orchestrator.config import ProxmoxConfig, RunnerProfile
from orchestrator.proxmox import ProxmoxClient, ProxmoxError


class RecordingProxmoxClient(ProxmoxClient):
    def __init__(self) -> None:
        super().__init__(
            ProxmoxConfig(
                endpoint="https://pve.invalid",
                api_token_file="/unused/token",
            )
        )
        self.calls: list[tuple[str, str, dict[str, Any] | None]] = []

    async def request(
        self,
        method: str,
        path: str,
        data: dict[str, Any] | None = None,
    ) -> Any:
        self.calls.append((method, path, data))
        return None


class ProxmoxFirewallTests(unittest.IsolatedAsyncioTestCase):
    async def test_otlp_exceptions_precede_rfc1918_drops(self) -> None:
        client = RecordingProxmoxClient()

        await client.configure_firewall(5000)

        created = [
            data
            for method, path, data in client.calls
            if method == "POST" and path.endswith("/firewall/rules")
        ]
        # Proxmox prepends each POST. Reverse the API call sequence to model
        # the effective order returned and evaluated by the firewall.
        effective = list(reversed(created))
        self.assertEqual(
            [(rule["action"], rule["dest"], rule.get("dport")) for rule in effective],
            [
                ("ACCEPT", "10.0.1.10/32", "4317"),
                ("ACCEPT", "10.0.1.10/32", "4318"),
                ("ACCEPT", "10.0.1.20/32", "22"),
                ("DROP", "10.0.0.0/8", None),
                ("DROP", "172.16.0.0/12", None),
                ("DROP", "192.168.0.0/16", None),
            ],
        )
        self.assertTrue(all("pos" not in rule for rule in created))


class ScriptedProxmoxClient(ProxmoxClient):
    """Returns canned responses in call order; raises if the queue runs dry."""

    def __init__(self, responses: list[Any]) -> None:
        super().__init__(
            ProxmoxConfig(endpoint="https://pve.invalid", api_token_file="/unused/token")
        )
        self._responses = list(responses)
        self.calls: list[tuple[str, str, dict[str, Any] | None]] = []

    async def request(self, method: str, path: str, data: dict[str, Any] | None = None) -> Any:
        self.calls.append((method, path, data))
        if not self._responses:
            raise AssertionError(f"no scripted response left for {method} {path}")
        result = self._responses.pop(0)
        if isinstance(result, Exception):
            raise result
        return result


class CountingErrorProxmoxClient(ProxmoxClient):
    """Always fails with the same ProxmoxError message; counts attempts."""

    def __init__(self, message: str) -> None:
        super().__init__(
            ProxmoxConfig(endpoint="https://pve.invalid", api_token_file="/unused/token")
        )
        self.message = message
        self.attempts = 0

    async def request(self, method: str, path: str, data: dict[str, Any] | None = None) -> Any:
        self.attempts += 1
        raise ProxmoxError(self.message)


class LockRetryTests(unittest.IsolatedAsyncioTestCase):
    async def test_retries_transient_lock_error_then_succeeds(self) -> None:
        client = ScriptedProxmoxClient(
            [ProxmoxError("CT is locked (disk)"), ProxmoxError("CT is locked (disk)"), "UPID:done"]
        )
        with mock.patch("orchestrator.proxmox.asyncio.sleep", mock.AsyncMock()):
            result = await client.request_with_lock_retry("POST", "/x", attempts=5, base_delay=0)
        self.assertEqual(result, "UPID:done")
        self.assertEqual(len(client.calls), 3)

    async def test_does_not_retry_non_lock_errors(self) -> None:
        client = CountingErrorProxmoxClient("Proxmox API 403: permission denied")
        with self.assertRaises(ProxmoxError):
            await client.request_with_lock_retry("POST", "/x", attempts=5, base_delay=0)
        self.assertEqual(client.attempts, 1)

    async def test_raises_after_exhausting_attempts_on_persistent_lock(self) -> None:
        client = CountingErrorProxmoxClient("still locked (disk)")
        with (
            mock.patch("orchestrator.proxmox.asyncio.sleep", mock.AsyncMock()),
            self.assertRaises(ProxmoxError),
        ):
            await client.request_with_lock_retry("POST", "/x", attempts=3, base_delay=0)
        self.assertEqual(client.attempts, 3)


class CloneTests(unittest.IsolatedAsyncioTestCase):
    async def test_clone_full_sequence(self) -> None:
        upid = "UPID:pve:00001234:00005678:00000000:qmclone:9101:root@pam:"
        responses: list[Any] = [
            upid,  # POST clone
            {"status": "stopped", "exitstatus": "OK"},  # GET task status (wait_task)
            None,  # PUT config
            None,  # PUT firewall/options
            None,
            None,
            None,
            None,
            None,
            None,  # POST firewall/rules x6
        ]
        client = ScriptedProxmoxClient(responses)
        profile = RunnerProfile()

        await client.clone(5000, "pve-lxc-ephemeral-5000", "10.0.0.100", profile, "ephemeral", 42)

        methods_paths = [(m, p) for m, p, _ in client.calls]
        self.assertEqual(methods_paths[0], ("POST", "/nodes/pve/lxc/9101/clone"))
        self.assertEqual(
            methods_paths[1], ("GET", f"/nodes/pve/tasks/{quote(upid, safe='')}/status")
        )
        self.assertEqual(methods_paths[2], ("PUT", "/nodes/pve/lxc/5000/config"))
        self.assertEqual(methods_paths[3], ("PUT", "/nodes/pve/lxc/5000/firewall/options"))
        self.assertEqual([m for m, _ in methods_paths[4:]], ["POST"] * 6)
        self.assertEqual(len(client.calls), 10)
        # Clone config carries the profile sizing and job tag through.
        self.assertEqual(client.calls[0][2]["newid"], 5000)
        config_call = client.calls[2][2]
        self.assertEqual(config_call["cores"], profile.cores)
        self.assertIn("job-42", config_call["tags"])

    async def test_clone_raises_on_task_failure(self) -> None:
        upid = "UPID:pve:00001234:00005678:00000000:qmclone:9101:root@pam:"
        client = ScriptedProxmoxClient(
            [upid, {"status": "stopped", "exitstatus": "clone failed: disk full"}]
        )
        with self.assertRaises(ProxmoxError):
            await client.clone(
                5000, "pve-lxc-5000", "10.0.0.100", RunnerProfile(), "ephemeral", None
            )
        # Never reached config/firewall calls after the clone task itself failed.
        self.assertEqual(len(client.calls), 2)


class DestroyTests(unittest.IsolatedAsyncioTestCase):
    async def test_destroy_is_a_noop_when_ct_already_gone(self) -> None:
        client = ScriptedProxmoxClient(
            [
                None,  # POST unlock (best-effort, swallows its own errors)
                ProxmoxError("Configuration file does not exist"),  # GET status/current
            ]
        )
        await client.destroy(5000)
        self.assertEqual(len(client.calls), 2)

    async def test_destroy_skips_stop_when_already_stopped(self) -> None:
        client = ScriptedProxmoxClient(
            [
                None,  # unlock
                {"status": "stopped"},  # status/current
                {},  # GET config (no mp0)
                None,  # unlock (again, before delete)
                "UPID:pve:destroy",  # DELETE
                {"status": "stopped", "exitstatus": "OK"},  # wait_task for delete
            ]
        )
        await client.destroy(5000)
        methods = [m for m, _, _ in client.calls]
        self.assertNotIn(
            ("POST", "/nodes/pve/lxc/5000/status/stop"), [(m, p) for m, p, _ in client.calls]
        )
        self.assertEqual(methods[-2], "DELETE")

    async def test_destroy_stops_running_ct_and_clears_mp0_before_delete(self) -> None:
        client = ScriptedProxmoxClient(
            [
                None,  # unlock
                {"status": "running"},  # status/current
                "UPID:pve:stop",  # POST stop
                {"status": "stopped", "exitstatus": "OK"},  # wait_task for stop
                {"mp0": "local-lvm:vm-5000-disk-1,mp=/cache"},  # GET config
                None,  # PUT config delete mp0
                None,  # unlock (again)
                "UPID:pve:destroy",  # DELETE
                {"status": "stopped", "exitstatus": "OK"},  # wait_task for delete
            ]
        )
        await client.destroy(5000)

        methods_paths = [(m, p) for m, p, _ in client.calls]
        self.assertIn(("POST", "/nodes/pve/lxc/5000/status/stop"), methods_paths)
        delete_call = next(d for m, p, d in client.calls if m == "PUT" and p.endswith("/config"))
        self.assertEqual(delete_call, {"delete": "mp0"})
        self.assertEqual(methods_paths[-2][0], "DELETE")


if __name__ == "__main__":
    unittest.main()

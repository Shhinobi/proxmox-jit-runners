from __future__ import annotations

import asyncio
from pathlib import Path
from typing import Any
from urllib.parse import quote

import httpx

from .config import ProxmoxConfig, RunnerProfile


class ProxmoxError(RuntimeError):
    pass


class ProxmoxClient:
    def __init__(self, config: ProxmoxConfig):
        self.config = config

    def _headers(self) -> dict[str, str]:
        token = Path(self.config.api_token_file).read_text(encoding="utf-8").strip()
        if not token:
            raise ProxmoxError("empty Proxmox API token")
        return {"Authorization": f"PVEAPIToken={token}"}

    async def request(self, method: str, path: str, data: dict[str, Any] | None = None) -> Any:
        query_method = method.upper() in {"GET", "DELETE"}
        async with httpx.AsyncClient(verify=self.config.verify_tls, timeout=45) as client:
            response = await client.request(
                method,
                f"{self.config.endpoint}/api2/json{path}",
                headers=self._headers(),
                params=data if query_method else None,
                data=data if not query_method else None,
            )
        if response.is_error:
            raise ProxmoxError(f"Proxmox API {response.status_code}: {response.text[:500]}")
        payload = response.json()
        return payload.get("data")

    async def cluster_vmids(self) -> set[int]:
        resources = await self.request("GET", "/cluster/resources", {"type": "vm"})
        return {int(item["vmid"]) for item in resources or [] if item.get("vmid") is not None}

    async def clone(
        self,
        vmid: int,
        hostname: str,
        ip_address: str,
        profile: RunnerProfile,
        mode: str,
        job_id: int | None,
    ) -> None:
        upid = await self.request(
            "POST",
            f"/nodes/{self.config.node}/lxc/{self.config.template_vmid}/clone",
            {
                "newid": vmid,
                "hostname": hostname,
                "full": 1,
                "storage": self.config.storage,
            },
        )
        await self.wait_task(str(upid))
        tags = ["jit-runner", f"mode-{mode}", "managed-orchestrator"]
        if job_id is not None:
            tags.append(f"job-{job_id}")
        await self.request(
            "PUT",
            f"/nodes/{self.config.node}/lxc/{vmid}/config",
            {
                "cores": profile.cores,
                "memory": profile.memory_mb,
                "swap": profile.swap_mb,
                "nameserver": "1.1.1.1 1.0.0.1",
                "net0": (
                    f"name=eth0,bridge={self.config.bridge},ip={ip_address}/24,"
                    f"gw={self.config.gateway},firewall=1,mtu=1450"
                ),
                "tags": ";".join(tags),
                "onboot": 0,
                "description": f"Managed by runner-orchestrator; mode={mode}; job={job_id or 'pool'}",
            },
        )
        await self.configure_firewall(vmid)

    async def configure_firewall(self, vmid: int) -> None:
        base = f"/nodes/{self.config.node}/lxc/{vmid}/firewall"
        await self.request(
            "PUT",
            f"{base}/options",
            {
                "enable": 1,
                "policy_in": "DROP",
                "policy_out": "ACCEPT",
                "ipfilter": 1,
                "macfilter": 1,
            },
        )
        # Public resolvers are configured on the CT; prevent lateral RFC1918 access.
        rules = [
            {
                "type": "out",
                "action": "ACCEPT",
                "dest": "10.0.1.10/32",
                "proto": "tcp",
                "dport": "4317",
                "comment": "Allow OTLP gRPC to SigNoz",
            },
            {
                "type": "out",
                "action": "ACCEPT",
                "dest": "10.0.1.10/32",
                "proto": "tcp",
                "dport": "4318",
                "comment": "Allow OTLP HTTP to SigNoz",
            },
            {"type": "out", "action": "DROP", "dest": "10.0.0.0/8", "comment": "Deny RFC1918 lateral"},
            {"type": "out", "action": "DROP", "dest": "172.16.0.0/12", "comment": "Deny RFC1918 lateral"},
            {"type": "out", "action": "DROP", "dest": "192.168.0.0/16", "comment": "Deny RFC1918 lateral"},
        ]
        # The Proxmox firewall API prepends new rules.  Create them bottom-up so
        # the effective top-down order keeps the OTLP exceptions ahead of the
        # broad RFC1918 denies.
        for rule in reversed(rules):
            await self.request("POST", f"{base}/rules", {**rule, "enable": 1})

    async def start(self, vmid: int) -> None:
        upid = await self.request("POST", f"/nodes/{self.config.node}/lxc/{vmid}/status/start")
        await self.wait_task(str(upid))

    async def stop(self, vmid: int) -> None:
        try:
            upid = await self.request("POST", f"/nodes/{self.config.node}/lxc/{vmid}/status/stop")
            await self.wait_task(str(upid), timeout=90)
        except ProxmoxError as exc:
            msg = str(exc).lower()
            if any(
                token in msg
                for token in (
                    "not running",
                    "does not exist",
                    "no such container",
                    "configuration file does not exist",
                )
            ):
                return
            raise

    async def destroy(self, vmid: int) -> None:
        try:
            current = await self.status(vmid)
        except ProxmoxError as exc:
            msg = str(exc).lower()
            if any(
                token in msg
                for token in ("does not exist", "no such container", "configuration file does not exist")
            ):
                return
            raise
        if str(current.get("status", "")).lower() == "running":
            await self.stop(vmid)
        try:
            config = await self.request("GET", f"/nodes/{self.config.node}/lxc/{vmid}/config")
        except ProxmoxError as exc:
            msg = str(exc).lower()
            if any(
                token in msg
                for token in ("does not exist", "no such container", "configuration file does not exist")
            ):
                return
            raise
        if config and config.get("mp0"):
            await self.request(
                "PUT",
                f"/nodes/{self.config.node}/lxc/{vmid}/config",
                {"delete": "mp0"},
            )
        try:
            upid = await self.request(
                "DELETE",
                f"/nodes/{self.config.node}/lxc/{vmid}",
            )
            await self.wait_task(str(upid))
        except ProxmoxError as exc:
            msg = str(exc).lower()
            if any(
                token in msg
                for token in ("does not exist", "no such container", "configuration file does not exist")
            ):
                return
            raise

    async def status(self, vmid: int) -> dict[str, Any]:
        data = await self.request("GET", f"/nodes/{self.config.node}/lxc/{vmid}/status/current")
        return dict(data or {})

    async def wait_task(self, upid: str, timeout: int = 300) -> None:
        if not upid.startswith("UPID:"):
            raise ProxmoxError(f"invalid task id: {upid!r}")
        node = upid.split(":", 2)[1]
        task_path = quote(upid, safe="")
        deadline = asyncio.get_running_loop().time() + timeout
        while asyncio.get_running_loop().time() < deadline:
            status = await self.request("GET", f"/nodes/{node}/tasks/{task_path}/status")
            if status.get("status") == "stopped":
                if status.get("exitstatus") != "OK":
                    raise ProxmoxError(f"task failed: {status.get('exitstatus')}")
                return
            await asyncio.sleep(1)
        raise ProxmoxError(f"task timed out after {timeout}s: {upid}")

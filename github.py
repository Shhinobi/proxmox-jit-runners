from __future__ import annotations

import time
from pathlib import Path
from typing import Any

import httpx
import jwt

from .config import GitHubConfig, GitHubInstallation


class GitHubError(RuntimeError):
    pass


class GitHubClient:
    def __init__(self, config: GitHubConfig):
        self.config = config
        self._installation_tokens: dict[int, tuple[str, int]] = {}

    def _app_jwt(self) -> str:
        now = int(time.time())
        private_key = Path(self.config.private_key_file).read_text(encoding="utf-8")
        return jwt.encode({"iat": now - 30, "exp": now + 540, "iss": self.config.app_id}, private_key, algorithm="RS256")

    async def _token(self, installation: GitHubInstallation) -> str:
        cached = self._installation_tokens.get(installation.installation_id)
        if cached and cached[1] > int(time.time()) + 60:
            return cached[0]
        headers = self._headers(self._app_jwt())
        async with httpx.AsyncClient(timeout=20) as client:
            response = await client.post(
                f"{self.config.api_url}/app/installations/{installation.installation_id}/access_tokens",
                headers=headers,
            )
        self._raise(response)
        data = response.json()
        # Installation tokens currently live for an hour. Refresh after 50 minutes.
        token = str(data["token"])
        self._installation_tokens[installation.installation_id] = (token, int(time.time()) + 3000)
        return token

    def _headers(self, token: str) -> dict[str, str]:
        return {
            "Accept": "application/vnd.github+json",
            "Authorization": f"Bearer {token}",
            "X-GitHub-Api-Version": self.config.api_version,
            "User-Agent": "proxmox-jit-runner-orchestrator/0.1",
        }

    @staticmethod
    def _raise(response: httpx.Response) -> None:
        if response.is_error:
            raise GitHubError(f"GitHub API {response.status_code}: {response.text[:500]}")

    async def request(
        self,
        installation: GitHubInstallation,
        method: str,
        path: str,
        json: dict[str, Any] | None = None,
    ) -> dict[str, Any]:
        token = await self._token(installation)
        async with httpx.AsyncClient(timeout=30) as client:
            response = await client.request(
                method, f"{self.config.api_url}{path}", headers=self._headers(token), json=json
            )
        self._raise(response)
        return response.json() if response.content else {}

    async def get_workflow_run(
        self, installation: GitHubInstallation, repository: str, run_id: int
    ) -> dict[str, Any]:
        return await self.request(installation, "GET", f"/repos/{repository}/actions/runs/{run_id}")

    async def generate_jit(
        self,
        installation: GitHubInstallation,
        repository: str,
        name: str,
        labels: list[str],
    ) -> dict[str, Any]:
        base = self._runner_base(installation, repository)
        return await self.request(
            installation,
            "POST",
            f"{base}/actions/runners/generate-jitconfig",
            {
                "name": name,
                "runner_group_id": installation.runner_group_id,
                "labels": labels,
                "work_folder": "_work",
            },
        )

    async def registration_token(
        self, installation: GitHubInstallation, repository: str
    ) -> str:
        base = self._runner_base(installation, repository)
        data = await self.request(
            installation, "POST", f"{base}/actions/runners/registration-token"
        )
        return str(data["token"])

    async def find_runner(
        self, installation: GitHubInstallation, repository: str, name: str
    ) -> dict[str, Any] | None:
        base = self._runner_base(installation, repository)
        data = await self.request(
            installation, "GET", f"{base}/actions/runners?per_page=100"
        )
        return next(
            (runner for runner in data.get("runners", []) if runner.get("name") == name),
            None,
        )

    async def delete_runner(
        self, installation: GitHubInstallation, repository: str, runner_id: int
    ) -> None:
        base = self._runner_base(installation, repository)
        await self.request(
            installation, "DELETE", f"{base}/actions/runners/{runner_id}"
        )

    @staticmethod
    def runner_url(installation: GitHubInstallation, repository: str) -> str:
        target = installation.account if installation.scope == "organization" else repository
        return f"https://github.com/{target}"

    @staticmethod
    def _runner_base(installation: GitHubInstallation, repository: str) -> str:
        if installation.scope == "organization":
            return f"/orgs/{installation.account}"
        if installation.scope == "repository":
            return f"/repos/{repository}"
        raise GitHubError(f"unsupported GitHub installation scope: {installation.scope}")

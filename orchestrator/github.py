from __future__ import annotations

import asyncio
import time
from typing import Any

import httpx
import jwt

from .config import GitHubConfig, GitHubInstallation
from .security import CachedSecret


class GitHubError(RuntimeError):
    def __init__(self, message: str, *, status_code: int | None = None) -> None:
        super().__init__(message)
        self.status_code = status_code


class GitHubClient:
    def __init__(self, config: GitHubConfig):
        self.config = config
        self._installation_tokens: dict[int, tuple[str, int]] = {}
        self._private_key = CachedSecret(config.private_key_file)
        # Shared across requests instead of one-per-call, for connection reuse.
        self._client = httpx.AsyncClient(follow_redirects=True)

    async def aclose(self) -> None:
        await self._client.aclose()

    def _app_jwt(self) -> str:
        now = int(time.time())
        return jwt.encode(
            {"iat": now - 30, "exp": now + 540, "iss": self.config.app_id},
            self._private_key.get(),
            algorithm="RS256",
        )

    async def _token(self, installation: GitHubInstallation) -> str:
        cached = self._installation_tokens.get(installation.installation_id)
        if cached and cached[1] > int(time.time()) + 60:
            return cached[0]
        headers = self._headers(self._app_jwt())
        response = await self._client.post(
            f"{self.config.api_url}/app/installations/{installation.installation_id}/access_tokens",
            headers=headers,
            timeout=20,
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
            raise GitHubError(
                f"GitHub API {response.status_code}: {response.text[:500]}",
                status_code=response.status_code,
            )
        # Unfollowed redirects (or unexpected 3xx bodies) produce empty payloads and
        # later KeyErrors like missing encoded_jit_config. Fail clearly instead.
        if 300 <= response.status_code < 400:
            location = response.headers.get("location", "")
            raise GitHubError(
                f"GitHub API {response.status_code} redirect without follow "
                f"(location={location!r}). Repository may have been renamed; "
                f"update Policies installation account + repository owner.",
                status_code=response.status_code,
            )

    @staticmethod
    def _retryable(exc: Exception) -> bool:
        """Network blips and GitHub 5xx are worth a retry; 4xx are policy/auth
        errors that won't fix themselves."""
        if isinstance(exc, httpx.TransportError):
            return True
        if isinstance(exc, GitHubError):
            return exc.status_code is not None and exc.status_code >= 500
        return False

    async def request(
        self,
        installation: GitHubInstallation,
        method: str,
        path: str,
        json: dict[str, Any] | None = None,
        *,
        attempts: int = 4,
        base_delay: float = 1.0,
    ) -> dict[str, Any]:
        token = await self._token(installation)
        last: Exception | None = None
        for attempt in range(attempts):
            try:
                # follow_redirects=True is set on the shared client: GitHub returns
                # 307 when a user/org/repo was renamed (e.g. OldOwner →
                # acme). Without following, generate-jitconfig returns an
                # empty body and callers crash on missing encoded_jit_config.
                response = await self._client.request(
                    method,
                    f"{self.config.api_url}{path}",
                    headers=self._headers(token),
                    json=json,
                    timeout=30,
                )
                self._raise(response)
                if not response.content:
                    return {}
                try:
                    data = response.json()
                except ValueError as exc:
                    raise GitHubError(
                        f"GitHub API {response.status_code}: non-JSON body: {response.text[:300]}",
                        status_code=response.status_code,
                    ) from exc
                return data if isinstance(data, dict) else {"_items": data}
            except Exception as exc:  # noqa: BLE001 - retry gate below decides fate
                last = exc
                if not self._retryable(exc) or attempt + 1 >= attempts:
                    raise
                await asyncio.sleep(base_delay * (attempt + 1))
        assert last is not None
        raise last

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
        data = await self.request(
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
        if not data.get("encoded_jit_config"):
            raise GitHubError(
                "GitHub generate-jitconfig response missing encoded_jit_config "
                f"for {repository!r} (keys={sorted(data.keys())}). "
                "If the GitHub handle/repo was renamed, update Policies to the "
                "new owner/name and installation account."
            )
        return data

    async def registration_token(self, installation: GitHubInstallation, repository: str) -> str:
        base = self._runner_base(installation, repository)
        data = await self.request(
            installation, "POST", f"{base}/actions/runners/registration-token"
        )
        return str(data["token"])

    async def list_runners(
        self, installation: GitHubInstallation, repository: str
    ) -> list[dict[str, Any]]:
        base = self._runner_base(installation, repository)
        data = await self.request(installation, "GET", f"{base}/actions/runners?per_page=100")
        runners = data.get("runners", [])
        return [dict(r) for r in runners if isinstance(r, dict)]

    async def find_runner(
        self, installation: GitHubInstallation, repository: str, name: str
    ) -> dict[str, Any] | None:
        return next(
            (
                runner
                for runner in await self.list_runners(installation, repository)
                if runner.get("name") == name
            ),
            None,
        )

    async def delete_runner(
        self, installation: GitHubInstallation, repository: str, runner_id: int
    ) -> None:
        base = self._runner_base(installation, repository)
        await self.request(installation, "DELETE", f"{base}/actions/runners/{runner_id}")

    async def try_delete_runner(
        self, installation: GitHubInstallation, repository: str, runner_id: int
    ) -> bool:
        """Best-effort delete. Returns False if GitHub refuses (e.g. still busy)."""
        try:
            await self.delete_runner(installation, repository, runner_id)
            return True
        except GitHubError as exc:
            msg = str(exc).lower()
            if "422" in msg or "busy" in msg or "running a job" in msg:
                return False
            raise

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

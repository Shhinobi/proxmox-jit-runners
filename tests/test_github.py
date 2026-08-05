from __future__ import annotations

import unittest
from typing import Any
from unittest import mock

import httpx

from orchestrator.config import GitHubConfig, GitHubInstallation
from orchestrator.github import GitHubClient, GitHubError


class RecordingGitHubClient(GitHubClient):
    def __init__(self, *, jit_payload: dict[str, Any] | None = None) -> None:
        super().__init__(
            GitHubConfig(
                app_id="1",
                private_key_file="/unused",
                webhook_secret_file="/unused",
                installations={},
            )
        )
        self.calls: list[tuple[int, str, str, dict[str, Any] | None]] = []
        self.jit_payload = jit_payload if jit_payload is not None else {"encoded_jit_config": "jit"}

    async def request(
        self,
        installation: GitHubInstallation,
        method: str,
        path: str,
        json: dict[str, Any] | None = None,
    ) -> dict[str, Any]:
        self.calls.append((installation.installation_id, method, path, json))
        if path.endswith("generate-jitconfig"):
            return dict(self.jit_payload)
        if path.endswith("actions/runners?per_page=100"):
            return {"runners": [{"id": 10, "name": "runner-1", "status": "online"}]}
        return {"token": "registration"}


class GitHubScopeTests(unittest.IsolatedAsyncioTestCase):
    async def test_personal_repository_uses_repository_endpoint(self) -> None:
        client = RecordingGitHubClient()
        installation = GitHubInstallation("OldOwner", 1234567, "repository")
        await client.generate_jit(
            installation, "OldOwner/example-app", "runner-1", ["crossbuild"]
        )
        self.assertEqual(
            client.calls[0][2],
            "/repos/OldOwner/example-app/actions/runners/generate-jitconfig",
        )
        self.assertEqual(
            client.runner_url(installation, "OldOwner/example-app"),
            "https://github.com/OldOwner/example-app",
        )

    async def test_organization_uses_org_endpoint_and_group(self) -> None:
        client = RecordingGitHubClient()
        installation = GitHubInstallation("AcmeOrg", 200, "organization", runner_group_id=7)
        await client.generate_jit(
            installation, "AcmeOrg/Project", "runner-2", ["crossbuild"]
        )
        self.assertEqual(
            client.calls[0][2], "/orgs/AcmeOrg/actions/runners/generate-jitconfig"
        )
        self.assertEqual(client.calls[0][3]["runner_group_id"], 7)
        self.assertEqual(
            client.runner_url(installation, "AcmeOrg/Project"),
            "https://github.com/AcmeOrg",
        )

    async def test_runner_lookup_and_delete_use_repository_scope(self) -> None:
        client = RecordingGitHubClient()
        installation = GitHubInstallation("OldOwner", 1234567, "repository")
        runner = await client.find_runner(
            installation, "OldOwner/example-app", "runner-1"
        )
        self.assertEqual(runner["id"], 10)
        await client.delete_runner(
            installation, "OldOwner/example-app", int(runner["id"])
        )
        self.assertEqual(
            client.calls[-1][2],
            "/repos/OldOwner/example-app/actions/runners/10",
        )

    async def test_generate_jit_rejects_missing_encoded_config(self) -> None:
        client = RecordingGitHubClient(jit_payload={})
        installation = GitHubInstallation("acme", 1234567, "repository")
        with self.assertRaises(GitHubError) as ctx:
            await client.generate_jit(
                installation, "acme/example-app", "runner-1", ["crossbuild"]
            )
        self.assertIn("encoded_jit_config", str(ctx.exception))


def _plain_client() -> GitHubClient:
    return GitHubClient(
        GitHubConfig(
            app_id="1",
            private_key_file="/unused",
            webhook_secret_file="/unused",
            installations={},
        )
    )


class GitHubRetryTests(unittest.IsolatedAsyncioTestCase):
    """Exercises the real request() retry/backoff, not the RecordingGitHubClient override."""

    async def test_retries_transient_network_error_then_succeeds(self) -> None:
        client = _plain_client()
        installation = GitHubInstallation("Acme", 1, "organization")
        responses: list[Any] = [httpx.ConnectError("boom"), httpx.Response(200, json={"ok": True})]

        async def fake_request(*_args: Any, **_kwargs: Any) -> httpx.Response:
            result = responses.pop(0)
            if isinstance(result, Exception):
                raise result
            return result

        with mock.patch.object(GitHubClient, "_token", mock.AsyncMock(return_value="tok")), \
             mock.patch.object(httpx.AsyncClient, "request", side_effect=fake_request), \
             mock.patch("orchestrator.github.asyncio.sleep", mock.AsyncMock()):
            data = await client.request(installation, "GET", "/foo", base_delay=0)

        self.assertEqual(data, {"ok": True})
        self.assertEqual(responses, [])

    async def test_does_not_retry_client_error(self) -> None:
        client = _plain_client()
        installation = GitHubInstallation("Acme", 1, "organization")
        calls = 0

        async def fake_request(*_args: Any, **_kwargs: Any) -> httpx.Response:
            nonlocal calls
            calls += 1
            return httpx.Response(404, json={"message": "not found"})

        with mock.patch.object(GitHubClient, "_token", mock.AsyncMock(return_value="tok")), \
             mock.patch.object(httpx.AsyncClient, "request", side_effect=fake_request):
            with self.assertRaises(GitHubError) as ctx:
                await client.request(installation, "GET", "/foo", attempts=3, base_delay=0)

        self.assertEqual(calls, 1)
        self.assertEqual(ctx.exception.status_code, 404)

    async def test_exhausts_retries_on_persistent_5xx(self) -> None:
        client = _plain_client()
        installation = GitHubInstallation("Acme", 1, "organization")
        calls = 0

        async def fake_request(*_args: Any, **_kwargs: Any) -> httpx.Response:
            nonlocal calls
            calls += 1
            return httpx.Response(503, json={"message": "unavailable"})

        with mock.patch.object(GitHubClient, "_token", mock.AsyncMock(return_value="tok")), \
             mock.patch.object(httpx.AsyncClient, "request", side_effect=fake_request), \
             mock.patch("orchestrator.github.asyncio.sleep", mock.AsyncMock()):
            with self.assertRaises(GitHubError):
                await client.request(installation, "GET", "/foo", attempts=3, base_delay=0)

        self.assertEqual(calls, 3)

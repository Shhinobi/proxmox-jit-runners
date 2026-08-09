from __future__ import annotations

import time
from datetime import UTC, datetime
from typing import Any
from unittest import mock

import httpx
import pytest

from orchestrator.config import GitHubConfig, GitHubInstallation
from orchestrator.github import (
    GitHubClient,
    GitHubError,
    GitHubUnavailable,
    _parse_retry_after,
)


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
        # Matched on the path rather than the full query string so pagination
        # parameters can change without silently falling through to the
        # registration-token branch.
        if "actions/runners?" in path:
            return {"runners": [{"id": 10, "name": "runner-1", "status": "online"}]}
        return {"token": "registration"}


async def test_personal_repository_uses_repository_endpoint() -> None:
    client = RecordingGitHubClient()
    installation = GitHubInstallation("OldOwner", 1234567, "repository")
    await client.generate_jit(installation, "OldOwner/example-app", "runner-1", ["crossbuild"])
    assert client.calls[0][2] == "/repos/OldOwner/example-app/actions/runners/generate-jitconfig"
    assert (
        client.runner_url(installation, "OldOwner/example-app")
        == "https://github.com/OldOwner/example-app"
    )


async def test_organization_uses_org_endpoint_and_group() -> None:
    client = RecordingGitHubClient()
    installation = GitHubInstallation("AcmeOrg", 200, "organization", runner_group_id=7)
    await client.generate_jit(installation, "AcmeOrg/Project", "runner-2", ["crossbuild"])
    assert client.calls[0][2] == "/orgs/AcmeOrg/actions/runners/generate-jitconfig"
    assert client.calls[0][3]["runner_group_id"] == 7
    assert client.runner_url(installation, "AcmeOrg/Project") == "https://github.com/AcmeOrg"


async def test_runner_lookup_and_delete_use_repository_scope() -> None:
    client = RecordingGitHubClient()
    installation = GitHubInstallation("OldOwner", 1234567, "repository")
    runner = await client.find_runner(installation, "OldOwner/example-app", "runner-1")
    assert runner["id"] == 10
    await client.delete_runner(installation, "OldOwner/example-app", int(runner["id"]))
    assert client.calls[-1][2] == "/repos/OldOwner/example-app/actions/runners/10"


async def test_generate_jit_rejects_missing_encoded_config() -> None:
    client = RecordingGitHubClient(jit_payload={})
    installation = GitHubInstallation("acme", 1234567, "repository")
    with pytest.raises(GitHubError) as ctx:
        await client.generate_jit(installation, "acme/example-app", "runner-1", ["crossbuild"])
    assert "encoded_jit_config" in str(ctx.value)


def _plain_client() -> GitHubClient:
    return GitHubClient(
        GitHubConfig(
            app_id="1",
            private_key_file="/unused",
            webhook_secret_file="/unused",
            installations={},
        )
    )


# The following exercise the real request() retry/backoff, not RecordingGitHubClient's override.


async def test_retries_transient_network_error_then_succeeds() -> None:
    client = _plain_client()
    installation = GitHubInstallation("Acme", 1, "organization")
    responses: list[Any] = [httpx.ConnectError("boom"), httpx.Response(200, json={"ok": True})]

    async def fake_request(*_args: Any, **_kwargs: Any) -> httpx.Response:
        result = responses.pop(0)
        if isinstance(result, Exception):
            raise result
        return result

    with (
        mock.patch.object(GitHubClient, "_token", mock.AsyncMock(return_value="tok")),
        mock.patch.object(httpx.AsyncClient, "request", side_effect=fake_request),
        mock.patch("orchestrator.github.asyncio.sleep", mock.AsyncMock()),
    ):
        data = await client.request(installation, "GET", "/foo", base_delay=0)

    assert data == {"ok": True}
    assert responses == []


async def test_does_not_retry_client_error() -> None:
    client = _plain_client()
    installation = GitHubInstallation("Acme", 1, "organization")
    calls = 0

    async def fake_request(*_args: Any, **_kwargs: Any) -> httpx.Response:
        nonlocal calls
        calls += 1
        return httpx.Response(404, json={"message": "not found"})

    with (
        mock.patch.object(GitHubClient, "_token", mock.AsyncMock(return_value="tok")),
        mock.patch.object(httpx.AsyncClient, "request", side_effect=fake_request),
        pytest.raises(GitHubError) as ctx,
    ):
        await client.request(installation, "GET", "/foo", attempts=3, base_delay=0)

    assert calls == 1
    assert ctx.value.status_code == 404


async def test_exhausts_retries_on_persistent_5xx() -> None:
    client = _plain_client()
    installation = GitHubInstallation("Acme", 1, "organization")
    calls = 0

    async def fake_request(*_args: Any, **_kwargs: Any) -> httpx.Response:
        nonlocal calls
        calls += 1
        return httpx.Response(503, json={"message": "unavailable"})

    with (
        mock.patch.object(GitHubClient, "_token", mock.AsyncMock(return_value="tok")),
        mock.patch.object(httpx.AsyncClient, "request", side_effect=fake_request),
        mock.patch("orchestrator.github.asyncio.sleep", mock.AsyncMock()),
        pytest.raises(GitHubError),
    ):
        await client.request(installation, "GET", "/foo", attempts=3, base_delay=0)

    assert calls == 3


# --- Timeouts, classification, and pagination (2026-08 resilience rework) -----


async def test_client_uses_layered_timeouts() -> None:
    """A single scalar timeout would not have bounded the incident's failure.

    The TLS handshake stalled *after* ClientHello, which is a read timeout;
    connect alone never fires for that shape.
    """
    client = _plain_client()
    timeout = client._client.timeout
    assert timeout.connect == 5.0
    assert timeout.read == 15.0
    assert timeout.write == 10.0
    assert timeout.pool == 5.0


async def test_transport_error_surfaces_as_github_unavailable() -> None:
    """Teardown keys off this type to tell "no answer" from "GitHub said no"."""
    client = _plain_client()
    installation = GitHubInstallation("Acme", 1, "organization")

    async def fake_request(*_args: Any, **_kwargs: Any) -> httpx.Response:
        raise httpx.ConnectError("boom")

    with (
        mock.patch.object(GitHubClient, "_token", mock.AsyncMock(return_value="tok")),
        mock.patch.object(httpx.AsyncClient, "request", side_effect=fake_request),
        mock.patch("orchestrator.github.asyncio.sleep", mock.AsyncMock()),
        pytest.raises(GitHubUnavailable),
    ):
        await client.request(installation, "GET", "/foo", attempts=2, base_delay=0)


async def test_rate_limit_is_retried_and_honours_retry_after() -> None:
    client = _plain_client()
    installation = GitHubInstallation("Acme", 1, "organization")
    responses = [
        httpx.Response(429, headers={"retry-after": "7"}, json={"message": "slow down"}),
        httpx.Response(200, json={"ok": True}),
    ]
    slept: list[float] = []

    async def fake_request(*_args: Any, **_kwargs: Any) -> httpx.Response:
        return responses.pop(0)

    async def fake_sleep(seconds: float) -> None:
        slept.append(seconds)

    with (
        mock.patch.object(GitHubClient, "_token", mock.AsyncMock(return_value="tok")),
        mock.patch.object(httpx.AsyncClient, "request", side_effect=fake_request),
        mock.patch("orchestrator.github.asyncio.sleep", fake_sleep),
    ):
        data = await client.request(installation, "GET", "/foo", attempts=3, base_delay=1.0)

    assert data == {"ok": True}
    assert slept == [7.0], "Retry-After must win over the exponential schedule"


def test_rate_limit_reset_header_is_used_when_retry_after_absent() -> None:
    reset = time.time() + 12
    response = httpx.Response(
        429,
        headers={"x-ratelimit-remaining": "0", "x-ratelimit-reset": str(int(reset))},
        json={"message": "limit"},
    )
    delay = GitHubClient._retry_delay(
        GitHubError("x", status_code=429, retry_after=_parse_retry_after(response.headers)),
        0,
        1.0,
    )
    assert 10 <= delay <= 13


async def test_request_gives_up_when_retry_would_exceed_the_deadline() -> None:
    """Sleeping past the deadline only to be abandoned wastes the wait."""
    client = _plain_client()
    installation = GitHubInstallation("Acme", 1, "organization")

    async def fake_request(*_args: Any, **_kwargs: Any) -> httpx.Response:
        return httpx.Response(429, headers={"retry-after": "600"}, json={"message": "limit"})

    with (
        mock.patch.object(GitHubClient, "_token", mock.AsyncMock(return_value="tok")),
        mock.patch.object(httpx.AsyncClient, "request", side_effect=fake_request),
        pytest.raises(GitHubUnavailable) as ctx,
    ):
        await client.request(installation, "GET", "/foo", attempts=3, total_deadline=5.0)

    assert "deadline exhausted" in str(ctx.value)


async def test_401_is_retried_once_with_a_fresh_token() -> None:
    client = _plain_client()
    installation = GitHubInstallation("Acme", 1, "organization")
    responses = [
        httpx.Response(401, json={"message": "bad credentials"}),
        httpx.Response(200, json={"ok": True}),
    ]
    tokens = ["stale", "fresh"]
    handed_out: list[str] = []

    async def fake_request(*_args: Any, **_kwargs: Any) -> httpx.Response:
        return responses.pop(0)

    async def fake_token(_self: Any, _installation: Any) -> str:
        handed_out.append(tokens.pop(0))
        return handed_out[-1]

    with (
        mock.patch.object(GitHubClient, "_token", fake_token),
        mock.patch.object(httpx.AsyncClient, "request", side_effect=fake_request),
    ):
        data = await client.request(installation, "GET", "/foo", attempts=3, base_delay=0)

    assert data == {"ok": True}
    assert handed_out == ["stale", "fresh"], "the second attempt must not reuse the stale token"


async def test_list_runners_paginates_past_one_hundred() -> None:
    """Truncating this list makes the warm health check destroy healthy runners."""
    client = _plain_client()
    installation = GitHubInstallation("Acme", 1, "organization")
    pages = {
        1: [{"id": i, "name": f"r{i}"} for i in range(100)],
        2: [{"id": 100 + i, "name": f"r{100 + i}"} for i in range(37)],
    }
    seen: list[str] = []

    async def fake_request(
        _self: Any, _installation: Any, _method: str, path: str, *_a: Any, **_k: Any
    ) -> dict[str, Any]:
        seen.append(path)
        page = int(path.split("&page=")[1])
        return {"runners": pages.get(page, [])}

    with mock.patch.object(GitHubClient, "request", fake_request):
        runners = await client.list_runners(installation, "Acme/Project")

    assert len(runners) == 137
    assert len(seen) == 2, "must stop as soon as a short page arrives"


async def test_try_delete_runner_classifies_on_status_not_substring() -> None:
    """A runner id containing '422' must not read as 'busy'.

    The old substring check inspected the formatted message, so any digits in
    the id or body could abort a legitimate delete.
    """
    client = _plain_client()
    installation = GitHubInstallation("Acme", 1, "organization")

    async def not_found(*_a: Any, **_k: Any) -> None:
        raise GitHubError("GitHub API 404: not found", status_code=404)

    async def busy(*_a: Any, **_k: Any) -> None:
        raise GitHubError("GitHub API 422: still running a job", status_code=422)

    async def server_error(*_a: Any, **_k: Any) -> None:
        raise GitHubError("GitHub API 500: boom, runner 4221", status_code=500)

    with mock.patch.object(GitHubClient, "delete_runner", not_found):
        assert await client.try_delete_runner(installation, "Acme/P", 4221) is True

    with mock.patch.object(GitHubClient, "delete_runner", busy):
        assert await client.try_delete_runner(installation, "Acme/P", 1) is False

    with (
        mock.patch.object(GitHubClient, "delete_runner", server_error),
        pytest.raises(GitHubError),
    ):
        await client.try_delete_runner(installation, "Acme/P", 4221)


def test_token_expiry_uses_expires_at_from_the_response() -> None:
    now = int(time.time())
    expires = datetime.fromtimestamp(now + 3600, tz=UTC).isoformat().replace("+00:00", "Z")
    refresh_at = GitHubClient._token_expiry({"expires_at": expires})
    # 80% of a one-hour lifetime.
    assert now + 2820 <= refresh_at <= now + 2940


def test_token_expiry_falls_back_when_expires_at_is_missing() -> None:
    now = int(time.time())
    assert GitHubClient._token_expiry({}) == pytest.approx(now + 3000, abs=2)


# --- Runner-list caching ------------------------------------------------------


def _installation() -> GitHubInstallation:
    return GitHubInstallation("Acme", 1, "organization")


async def test_list_runners_is_cached_within_the_ttl() -> None:
    """One reconcile pass must not re-list the same scope per instance."""
    client = _plain_client()
    calls = 0

    async def fake_request(_self: Any, _inst: Any, _method: str, _path: str, *_a: Any, **_k: Any):
        nonlocal calls
        calls += 1
        return {"runners": [{"id": 1, "name": "r1"}]}

    with mock.patch.object(GitHubClient, "request", fake_request):
        for _ in range(5):
            await client.list_runners(_installation(), "Acme/Project")

    assert calls == 1


async def test_find_runner_can_bypass_the_cache() -> None:
    """The loops that poll for a state change must read through."""
    client = _plain_client()
    calls = 0

    async def fake_request(_self: Any, _inst: Any, _method: str, _path: str, *_a: Any, **_k: Any):
        nonlocal calls
        calls += 1
        return {"runners": [{"id": 1, "name": "r1"}]}

    with mock.patch.object(GitHubClient, "request", fake_request):
        await client.find_runner(_installation(), "Acme/Project", "r1")
        await client.find_runner(_installation(), "Acme/Project", "r1", fresh=True)

    assert calls == 2


async def test_organization_scope_shares_one_cached_list() -> None:
    """Every repository in an org installation reads the same runner list."""
    client = _plain_client()
    calls = 0

    async def fake_request(_self: Any, _inst: Any, _method: str, _path: str, *_a: Any, **_k: Any):
        nonlocal calls
        calls += 1
        return {"runners": []}

    with mock.patch.object(GitHubClient, "request", fake_request):
        await client.list_runners(_installation(), "Acme/One")
        await client.list_runners(_installation(), "Acme/Two")
        await client.list_runners(_installation(), "Acme/Three")

    assert calls == 1


async def test_deleting_a_runner_invalidates_the_cache() -> None:
    """A stale list must not hide a registration we just removed."""
    client = _plain_client()
    calls = 0

    async def fake_request(_self: Any, _inst: Any, method: str, _path: str, *_a: Any, **_k: Any):
        nonlocal calls
        if method == "GET":
            calls += 1
            return {"runners": [{"id": 1, "name": "r1"}]}
        return {}

    with mock.patch.object(GitHubClient, "request", fake_request):
        await client.list_runners(_installation(), "Acme/Project")
        await client.delete_runner(_installation(), "Acme/Project", 1)
        await client.list_runners(_installation(), "Acme/Project")

    assert calls == 2


async def test_truncated_list_is_never_cached() -> None:
    """Reusing a truncated list would make the warm check destroy live runners."""
    client = _plain_client()
    calls = 0

    async def fake_request(_self: Any, _inst: Any, _method: str, _path: str, *_a: Any, **_k: Any):
        nonlocal calls
        calls += 1
        return {"runners": [{"id": i, "name": f"r{i}"} for i in range(100)]}

    with mock.patch.object(GitHubClient, "request", fake_request):
        await client.list_runners(_installation(), "Acme/Project")
        first = calls
        await client.list_runners(_installation(), "Acme/Project")

    assert calls > first, "a page-capped result must be re-fetched, not reused"

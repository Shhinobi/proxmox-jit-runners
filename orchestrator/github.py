from __future__ import annotations

import asyncio
import contextlib
import logging
import random
import time
from datetime import datetime
from email.utils import parsedate_to_datetime
from typing import Any
from urllib.parse import urlsplit

import httpx
import jwt

from .config import GitHubConfig, GitHubInstallation
from .resilience import CircuitBreaker, CircuitOpen, CircuitState
from .security import CachedSecret

LOGGER = logging.getLogger("runner_orchestrator")

# Layered rather than one scalar. The the incident outage was a TLS handshake
# that stalled *after* ClientHello — a read timeout, not a connect timeout — so
# a single `timeout=30` was doing nothing useful for 30 seconds a call. Both
# layers are load-bearing.
_TIMEOUT = httpx.Timeout(connect=5.0, read=15.0, write=10.0, pool=5.0)

# Hard ceiling on one logical call including every retry and backoff sleep.
# Bounds how long a single reconcile step can be stuck on a dead dependency.
_TOTAL_DEADLINE_SECONDS = 45.0

# GitHub returns at most 100 runners per page. Without pagination a fleet
# larger than that silently truncates, `find_runner` returns None for healthy
# runners, and `_reconcile_warm_instance` destroys them as orphans.
_RUNNERS_PER_PAGE = 100
_MAX_RUNNER_PAGES = 10

# `find_runner` is a full `list_runners`, and a single reconcile pass calls it
# once per warm instance, once per running instance, once per teardown probe,
# and once per configured repository inside every registration purge. On a
# fleet of a dozen containers that was dozens of identical round trips a
# minute. A short TTL collapses them to one per runner scope per pass without
# meaningfully changing what any caller observes; the two loops that poll for a
# state change opt out with `fresh=True`.
_RUNNER_LIST_TTL_SECONDS = 15.0


class GitHubError(RuntimeError):
    def __init__(
        self,
        message: str,
        *,
        status_code: int | None = None,
        retry_after: float | None = None,
    ) -> None:
        super().__init__(message)
        self.status_code = status_code
        self.retry_after = retry_after


class GitHubUnavailable(GitHubError):
    """GitHub did not answer: transport failure, timeout, or an exhausted deadline.

    Distinct from a `GitHubError` carrying a status code, which *is* an answer.
    Callers that must decide "may I proceed without GitHub?" — above all the
    teardown path — key off this difference: an answer can defer a destroy, the
    absence of one cannot.
    """


def _parse_retry_after(headers: httpx.Headers) -> float | None:
    """Seconds to wait, from `Retry-After` or the primary rate-limit reset."""
    raw = headers.get("retry-after")
    if raw:
        try:
            return max(0.0, float(raw))
        except ValueError:
            try:
                delta = parsedate_to_datetime(raw) - datetime.now(
                    tz=parsedate_to_datetime(raw).tzinfo
                )
                return max(0.0, delta.total_seconds())
            except (TypeError, ValueError):
                return None
    if headers.get("x-ratelimit-remaining") == "0":
        reset = headers.get("x-ratelimit-reset")
        if reset:
            try:
                return max(0.0, float(reset) - time.time())
            except ValueError:
                return None
    return None


class GitHubClient:
    def __init__(self, config: GitHubConfig):
        self.config = config
        self._installation_tokens: dict[int, tuple[str, int]] = {}
        self._token_locks: dict[int, asyncio.Lock] = {}
        # Keyed by runner scope, not repository: an organization-scoped
        # installation serves every repository from one runner list.
        self._runner_lists: dict[tuple[int, str], tuple[float, list[dict[str, Any]]]] = {}
        self._private_key = CachedSecret(config.private_key_file)
        # One breaker for the whole client: the failure domain is the network
        # path to api.github.com, not any single installation.
        self.breaker = CircuitBreaker("github_api")
        # Shared across requests instead of one-per-call, for connection reuse.
        self._client = httpx.AsyncClient(
            follow_redirects=True,
            timeout=_TIMEOUT,
            limits=httpx.Limits(max_connections=10, max_keepalive_connections=5),
        )

    @property
    def available(self) -> bool:
        """Whether a call would be attempted at all right now.

        Read by the reconcile loop so purely-GitHub phases can be skipped
        wholesale instead of failing one instance at a time.
        """
        return self.breaker.allows()

    async def aclose(self) -> None:
        await self._client.aclose()

    def _app_jwt(self) -> str:
        now = int(time.time())
        return jwt.encode(
            {"iat": now - 30, "exp": now + 540, "iss": self.config.app_id},
            self._private_key.get(),
            algorithm="RS256",
        )

    def _cached_token(self, installation_id: int) -> str | None:
        cached = self._installation_tokens.get(installation_id)
        if cached and cached[1] > int(time.time()) + 60:
            return cached[0]
        return None

    def invalidate_token(self, installation: GitHubInstallation) -> None:
        self._installation_tokens.pop(installation.installation_id, None)

    @staticmethod
    def _token_expiry(data: dict[str, Any]) -> int:
        """Refresh at 80% of the token's real lifetime.

        GitHub tells us `expires_at`; the previous hardcoded `now + 3000`
        silently assumed a one-hour lifetime and would have gone stale the day
        that changed.
        """
        now = int(time.time())
        raw = data.get("expires_at")
        if isinstance(raw, str):
            try:
                expires = int(datetime.fromisoformat(raw.replace("Z", "+00:00")).timestamp())
            except ValueError:
                expires = now + 3600
            lifetime = max(60, expires - now)
            return now + int(lifetime * 0.8)
        return now + 3000

    async def _token(self, installation: GitHubInstallation) -> str:
        installation_id = installation.installation_id
        cached = self._cached_token(installation_id)
        if cached:
            return cached
        # Single-flight per installation: three concurrent warm creations used
        # to mint three separate JWT-signed token requests against a cold cache.
        lock = self._token_locks.setdefault(installation_id, asyncio.Lock())
        async with lock:
            cached = self._cached_token(installation_id)
            if cached:
                return cached
            headers = self._headers(self._app_jwt())
            try:
                response = await self._client.post(
                    f"{self.config.api_url}/app/installations/{installation_id}/access_tokens",
                    headers=headers,
                )
            except httpx.TransportError as exc:
                # Must be reported, or a broken token endpoint bypasses the
                # breaker entirely — and a half-open trial started by the
                # caller would never be released.
                self.breaker.record_failure(f"token endpoint: {exc}")
                raise GitHubUnavailable(f"GitHub token request failed: {exc}") from exc
            self._raise(response)
            data = response.json()
            token = str(data["token"])
            self._installation_tokens[installation_id] = (token, self._token_expiry(data))
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
                retry_after=_parse_retry_after(response.headers),
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
        """Availability failures are worth a retry; a policy answer is not.

        429 counts: being rate-limited is a temporary condition with a known
        wait, and treating it as fatal (which the previous `>= 500` gate did)
        turned a throttle into a hard failure of whatever was in flight.
        """
        if isinstance(exc, httpx.TransportError):
            return True
        if isinstance(exc, GitHubError):
            if exc.status_code is None:
                return isinstance(exc, GitHubUnavailable)
            return exc.status_code == 429 or exc.status_code >= 500
        return False

    @staticmethod
    def _retry_delay(exc: Exception, attempt: int, base_delay: float) -> float:
        """Exponential with jitter, unless GitHub told us exactly how long to wait.

        The jitter matters when a whole reconcile pass fails at once: without
        it every instance retries in lockstep and re-creates the same burst
        against a dependency that is still down.
        """
        retry_after = getattr(exc, "retry_after", None)
        if retry_after is not None:
            return float(retry_after)
        if base_delay <= 0:
            return 0.0
        return base_delay * (2**attempt) * random.uniform(0.5, 1.5)

    async def request(
        self,
        installation: GitHubInstallation,
        method: str,
        path: str,
        json: dict[str, Any] | None = None,
        *,
        attempts: int = 3,
        base_delay: float = 1.0,
        total_deadline: float | None = None,
    ) -> dict[str, Any]:
        deadline = time.monotonic() + (
            _TOTAL_DEADLINE_SECONDS if total_deadline is None else total_deadline
        )
        last: Exception | None = None
        refreshed_token = False
        for attempt in range(attempts):
            # Checked per attempt, not once: a retry ladder must not keep
            # hammering a dependency the breaker gave up on mid-ladder.
            try:
                self.breaker.before_call()
            except CircuitOpen as exc:
                raise GitHubUnavailable(str(exc), retry_after=exc.retry_after) from exc
            # Fetched inside the loop so a 401 can be retried against a fresh
            # token; hoisted outside, a stale or revoked token hard-failed every
            # call until the process restarted.
            token = await self._token(installation)
            try:
                # follow_redirects=True is set on the shared client: GitHub returns
                # 307 when a user/org/repo was renamed. Without following,
                # generate-jitconfig returns an
                # empty body and callers crash on missing encoded_jit_config.
                response = await self._client.request(
                    method,
                    f"{self.config.api_url}{path}",
                    headers=self._headers(token),
                    json=json,
                )
                if response.status_code == 401 and not refreshed_token:
                    # An answer, so the dependency is up — only the credential
                    # was stale.
                    self.breaker.record_success()
                    refreshed_token = True
                    self.invalidate_token(installation)
                    last = GitHubError("GitHub API 401: refreshing token", status_code=401)
                    continue
                if response.status_code == 429 or response.status_code >= 500:
                    self.breaker.record_failure(f"HTTP {response.status_code}")
                else:
                    # Any other status is GitHub answering us, including 4xx.
                    # Counting those would let one bad repository policy open
                    # the circuit for the whole fleet.
                    self.breaker.record_success()
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
            except httpx.TimeoutException as exc:
                self.breaker.record_failure(f"timeout: {exc}")
                last = GitHubUnavailable(f"GitHub API timeout on {method} {path}: {exc}")
            except httpx.TransportError as exc:
                self.breaker.record_failure(f"transport: {exc}")
                last = GitHubUnavailable(f"GitHub API transport error on {method} {path}: {exc}")
            except Exception as exc:  # noqa: BLE001 - retry gate below decides fate
                last = exc
                if not self._retryable(exc):
                    raise
            if attempt + 1 >= attempts:
                break
            delay = self._retry_delay(last, attempt, base_delay)
            remaining = deadline - time.monotonic()
            if delay >= remaining:
                # Sleeping past the deadline only to be abandoned wastes the
                # wait; give up now and say why.
                raise GitHubUnavailable(
                    f"GitHub API deadline exhausted on {method} {path} "
                    f"(next retry needed {delay:.1f}s, {remaining:.1f}s left)"
                ) from last
            await asyncio.sleep(delay)
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
        # A new registration exists now; a stale cached list would hide it.
        self.invalidate_runner_list(installation, repository)
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

    def invalidate_runner_list(self, installation: GitHubInstallation, repository: str) -> None:
        """Drop the cached list so the next read reflects a change we just made."""
        with contextlib.suppress(GitHubError):
            self._runner_lists.pop(
                (installation.installation_id, self._runner_base(installation, repository)), None
            )

    async def list_runners(
        self, installation: GitHubInstallation, repository: str, *, fresh: bool = False
    ) -> list[dict[str, Any]]:
        base = self._runner_base(installation, repository)
        key = (installation.installation_id, base)
        if not fresh:
            cached = self._runner_lists.get(key)
            if cached and (time.monotonic() - cached[0]) < _RUNNER_LIST_TTL_SECONDS:
                return [dict(r) for r in cached[1]]
        collected: list[dict[str, Any]] = []
        for page in range(1, _MAX_RUNNER_PAGES + 1):
            data = await self.request(
                installation,
                "GET",
                f"{base}/actions/runners?per_page={_RUNNERS_PER_PAGE}&page={page}",
            )
            runners = [r for r in data.get("runners", []) if isinstance(r, dict)]
            collected.extend(dict(r) for r in runners)
            if len(runners) < _RUNNERS_PER_PAGE:
                self._runner_lists[key] = (time.monotonic(), collected)
                return [dict(r) for r in collected]
        # A truncated list is dangerous, not merely incomplete: a missing runner
        # reads as "orphan" to the warm health check, which then destroys a
        # healthy container. Say so loudly.
        LOGGER.warning(
            "list_runners hit the %d-page cap for %s; results may be truncated",
            _MAX_RUNNER_PAGES,
            repository,
        )
        # Deliberately NOT cached: a truncated list must not be reused, or the
        # warm health check would keep destroying runners it simply cannot see.
        return collected

    async def find_runner(
        self, installation: GitHubInstallation, repository: str, name: str, *, fresh: bool = False
    ) -> dict[str, Any] | None:
        """Look up one runner by name.

        `fresh=True` bypasses the short list cache. Only the loops that poll for
        a state change need it — everything on the reconcile path is reading a
        world that changes far slower than the TTL.
        """
        return next(
            (
                runner
                for runner in await self.list_runners(installation, repository, fresh=fresh)
                if runner.get("name") == name
            ),
            None,
        )

    async def delete_runner(
        self, installation: GitHubInstallation, repository: str, runner_id: int
    ) -> None:
        base = self._runner_base(installation, repository)
        await self.request(installation, "DELETE", f"{base}/actions/runners/{runner_id}")
        self.invalidate_runner_list(installation, repository)

    async def try_delete_runner(
        self, installation: GitHubInstallation, repository: str, runner_id: int
    ) -> bool:
        """Best-effort delete. Returns False only if GitHub refuses (still busy).

        Classified on the status code, not on substrings of the formatted
        message. The previous `"422" in msg` test matched any response body or
        repository name containing those three digits — a runner id of 4221, or
        a repo with 422 anywhere in an error payload, silently produced a false
        "busy" verdict that aborted a legitimate delete.
        """
        try:
            await self.delete_runner(installation, repository, runner_id)
            return True
        except GitHubError as exc:
            if exc.status_code == 404:
                # Already gone is the outcome we wanted.
                return True
            if exc.status_code == 422:
                return False
            raise

    def _probe_target(self) -> tuple[str, int, bool]:
        parsed = urlsplit(self.config.api_url)
        host = parsed.hostname or "api.github.com"
        is_ssl = parsed.scheme != "http"
        port = parsed.port or (443 if is_ssl else 80)
        return host, port, is_ssl

    async def network_probe(self, *, timeout: float = 3.0) -> float:
        """Transport-level connectivity and TLS handshake check.

        Verifies DNS resolution, TCP routing, and TLS handshake to the GitHub API host
        without issuing any HTTP REST request or consuming any rate limit quota.
        """
        host, port, is_ssl = self._probe_target()
        started = time.monotonic()
        try:
            reader, writer = await asyncio.wait_for(
                asyncio.open_connection(host, port, ssl=is_ssl),
                timeout=timeout,
            )
        except (OSError, TimeoutError) as exc:
            raise GitHubUnavailable(f"network probe failed: {exc}") from exc
        try:
            writer.close()
            await writer.wait_closed()
        except OSError:
            pass
        return time.monotonic() - started

    async def probe_rest(self, *, timeout: float = 5.0) -> float:
        """Authenticated HTTP REST liveness check against GitHub.

        Authenticated with an installation token (or App JWT) so the probe
        draws from the 5000/h installation budget instead of exhausting the
        60/h unauthenticated IP rate limit.

        Deliberately NOT gated by the breaker — this is the out-of-band signal
        that tells the breaker the dependency came back.
        """
        started = time.monotonic()
        installation = next(iter(self.config.installations.values()), None)
        try:
            if installation is not None:
                token = await self._token(installation)
                headers = self._headers(token)
            else:
                try:
                    headers = self._headers(self._app_jwt())
                except Exception:  # noqa: BLE001 — fallback to basic headers when App key is unavailable
                    headers = {
                        "Accept": "application/vnd.github+json",
                        "X-GitHub-Api-Version": self.config.api_version,
                        "User-Agent": "proxmox-jit-runner-orchestrator/0.1",
                    }
            response = await self._client.get(
                f"{self.config.api_url}/zen", headers=headers, timeout=timeout
            )
            if response.status_code == 401 and installation is not None:
                self.invalidate_token(installation)
                token = await self._token(installation)
                response = await self._client.get(
                    f"{self.config.api_url}/zen", headers=self._headers(token), timeout=timeout
                )
        except httpx.TransportError as exc:
            raise GitHubUnavailable(f"probe failed: {exc}") from exc
        except GitHubError as exc:
            raise GitHubUnavailable(
                f"probe token request failed: {exc}",
                status_code=exc.status_code,
                retry_after=exc.retry_after,
            ) from exc

        if response.is_error:
            raise GitHubUnavailable(
                f"probe got HTTP {response.status_code}",
                status_code=response.status_code,
                retry_after=_parse_retry_after(response.headers),
            )
        latency = time.monotonic() - started
        # Shortens the open window; never forces the circuit closed, because
        # only a real call proves the API usable for what we need it for.
        self.breaker.note_probe_success()
        return latency

    async def probe(self, *, timeout: float = 5.0, force_rest: bool = False) -> float:
        """Hybrid connectivity probe.

        - In normal (CLOSED / HALF_OPEN) state: verifies reachability via
          `network_probe()` without issuing HTTP requests or consuming API quota.
        - In degraded (OPEN) state: issues an authenticated REST probe via `probe_rest()`
          to detect API recovery and shorten the circuit breaker's open window.
        """
        if force_rest or self.breaker.state is CircuitState.OPEN:
            return await self.probe_rest(timeout=timeout)
        return await self.network_probe(timeout=min(timeout, 3.0))

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

from __future__ import annotations

import asyncio
import hashlib
import hmac
import json
import os
import re
from dataclasses import replace
from pathlib import Path
from typing import Any, cast
from unittest import mock

# orchestrator/app.py does `app = create_app()` at import time (for uvicorn),
# which calls load_settings() with no explicit config — point it at a real,
# readable file before the import below, or the module import itself raises.
os.environ.setdefault(
    "RUNNER_ORCHESTRATOR_CONFIG",
    str(Path(__file__).resolve().parent.parent / "orchestrator" / "default-config.yaml"),
)
# configure_telemetry() imports the opentelemetry stack, which isn't
# installed in every environment this suite runs in; the module already
# supports opting out via this env var.
os.environ.setdefault("OTEL_SDK_DISABLED", "true")

import pytest
from fastapi.testclient import TestClient

from orchestrator.app import create_app
from orchestrator.service import Orchestrator
from tests.fakes import FakeBroker, FakeGitHub, FakeProxmox
from tests.helpers import settings


def _build_client(
    tmp_dir: str, *, webhook_secret: str = "test-secret"
) -> tuple[TestClient, Orchestrator]:
    secret_path = Path(tmp_dir) / "webhook-secret"
    secret_path.write_text(webhook_secret, encoding="utf-8")
    db_path = str(Path(tmp_dir) / "state.db")
    base = settings(database_path=db_path, shadow_mode=True)
    cfg = replace(base, github=replace(base.github, webhook_secret_file=str(secret_path)))
    app = create_app(cfg)
    orchestrator: Orchestrator = app.state.orchestrator
    asyncio.run(orchestrator.db.initialize())
    # create_app() wires real GitHub/Proxmox/broker clients; swap in the same
    # fakes used elsewhere so admin routes never touch a real network.
    # NOT using `with TestClient(app)` — that would run the app's lifespan
    # (orchestrator.start()), launching the real reconcile/worker loops.
    orchestrator.github = cast(Any, FakeGitHub())
    orchestrator.proxmox = cast(Any, FakeProxmox())
    orchestrator.broker = cast(Any, FakeBroker())
    return TestClient(app), orchestrator


def _sign(secret: str, body: bytes) -> str:
    return "sha256=" + hmac.new(secret.encode("utf-8"), body, hashlib.sha256).hexdigest()


def _queued_payload() -> bytes:
    return json.dumps(
        {
            "action": "queued",
            "repository": {"full_name": "OldOwner/example-app"},
            "workflow_job": {
                "id": 1,
                "run_id": 1,
                "labels": ["self-hosted", "proxmox-lxc", "crossbuild"],
            },
        }
    ).encode()


@pytest.fixture
def built_client(tmp_path: Path) -> tuple[TestClient, Orchestrator]:
    return _build_client(str(tmp_path))


@pytest.fixture
def client(built_client: tuple[TestClient, Orchestrator]) -> TestClient:
    return built_client[0]


@pytest.fixture
def app_orchestrator(built_client: tuple[TestClient, Orchestrator]) -> Orchestrator:
    return built_client[1]


WEBHOOK_SECRET = "test-secret"


def test_valid_signature_is_accepted(client: TestClient) -> None:
    body = _queued_payload()
    response = client.post(
        "/webhooks/github",
        content=body,
        headers={
            "X-Hub-Signature-256": _sign(WEBHOOK_SECRET, body),
            "X-GitHub-Delivery": "delivery-1",
            "X-GitHub-Event": "workflow_job",
            "Content-Type": "application/json",
        },
    )
    assert response.status_code == 202
    assert response.json()["accepted"]


def test_invalid_signature_is_rejected(client: TestClient) -> None:
    body = _queued_payload()
    response = client.post(
        "/webhooks/github",
        content=body,
        headers={
            "X-Hub-Signature-256": "sha256=" + "0" * 64,
            "X-GitHub-Delivery": "delivery-2",
            "X-GitHub-Event": "workflow_job",
        },
    )
    assert response.status_code == 401


def test_missing_signature_is_rejected(client: TestClient) -> None:
    body = _queued_payload()
    response = client.post(
        "/webhooks/github",
        content=body,
        headers={"X-GitHub-Delivery": "delivery-3", "X-GitHub-Event": "workflow_job"},
    )
    assert response.status_code == 401


def test_missing_delivery_header_is_rejected(client: TestClient) -> None:
    body = _queued_payload()
    response = client.post(
        "/webhooks/github",
        content=body,
        headers={
            "X-Hub-Signature-256": _sign(WEBHOOK_SECRET, body),
            "X-GitHub-Event": "workflow_job",
        },
    )
    assert response.status_code == 400


def test_missing_event_header_is_rejected(client: TestClient) -> None:
    body = _queued_payload()
    response = client.post(
        "/webhooks/github",
        content=body,
        headers={
            "X-Hub-Signature-256": _sign(WEBHOOK_SECRET, body),
            "X-GitHub-Delivery": "delivery-3b",
        },
    )
    assert response.status_code == 400


def test_invalid_json_is_rejected(client: TestClient) -> None:
    body = b"not-json"
    response = client.post(
        "/webhooks/github",
        content=body,
        headers={
            "X-Hub-Signature-256": _sign(WEBHOOK_SECRET, body),
            "X-GitHub-Delivery": "delivery-4",
            "X-GitHub-Event": "workflow_job",
        },
    )
    assert response.status_code == 400


def test_oversized_payload_is_rejected_via_content_length_header(client: TestClient) -> None:
    response = client.post(
        "/webhooks/github",
        content=b"{}",
        headers={
            "X-GitHub-Delivery": "delivery-5",
            "X-GitHub-Event": "workflow_job",
            "Content-Length": str(3 * 1024 * 1024),
        },
    )
    assert response.status_code == 413


def test_redelivery_of_same_delivery_id_is_deduped(client: TestClient) -> None:
    body = _queued_payload()
    headers = {
        "X-Hub-Signature-256": _sign(WEBHOOK_SECRET, body),
        "X-GitHub-Delivery": "delivery-dup",
        "X-GitHub-Event": "workflow_job",
    }
    first = client.post("/webhooks/github", content=body, headers=headers)
    second = client.post("/webhooks/github", content=body, headers=headers)
    assert first.status_code == 202
    assert second.status_code == 202
    assert first.json()["accepted"]
    assert not second.json()["accepted"]  # dedup: second insert is a no-op


def test_liveness_is_always_ok(client: TestClient) -> None:
    response = client.get("/health/live")
    assert response.status_code == 200
    assert response.json() == {"status": "ok"}


def test_readiness_returns_a_status(client: TestClient) -> None:
    response = client.get("/health/ready")
    assert response.status_code in {200, 503}
    assert "checks" in response.json()


def test_metrics_endpoint_serves_prometheus_text(client: TestClient) -> None:
    response = client.get("/metrics")
    assert response.status_code == 200
    assert "runner_orchestrator" in response.text


@pytest.fixture
def admin_client(tmp_path: Path):
    # DRAIN_MARKER defaults to /var/lib/runner-orchestrator/drain, a system
    # path this test must not depend on being writable (e.g. CI runners).
    drain_marker = tmp_path / "drain-marker"
    with (
        mock.patch("orchestrator.api.DRAIN_MARKER", drain_marker),
        mock.patch("orchestrator.service.DRAIN_MARKER", drain_marker),
    ):
        yield _build_client(str(tmp_path))


@pytest.fixture
def admin_test_client(admin_client: tuple[TestClient, Orchestrator]) -> TestClient:
    return admin_client[0]


@pytest.fixture
def admin_orchestrator(admin_client: tuple[TestClient, Orchestrator]) -> Orchestrator:
    return admin_client[1]


def test_drain_then_resume_toggles_marker(
    admin_test_client: TestClient, admin_orchestrator: Orchestrator
) -> None:
    drain = admin_test_client.post("/api/v1/admin/drain")
    assert drain.status_code == 200
    assert drain.json()["drain"]
    assert admin_orchestrator.draining

    resume = admin_test_client.post("/api/v1/admin/resume")
    assert resume.status_code == 200
    assert not resume.json()["drain"]
    assert not admin_orchestrator.draining


def test_reconcile_returns_before_after_summary(admin_test_client: TestClient) -> None:
    response = admin_test_client.post("/api/v1/admin/reconcile")
    assert response.status_code == 200
    body = response.json()
    assert body["ok"]
    assert "before" in body
    assert "after" in body


def test_recycle_ready_succeeds_when_nothing_to_recycle(admin_test_client: TestClient) -> None:
    response = admin_test_client.post("/api/v1/admin/recycle-ready")
    assert response.status_code == 200
    assert response.json()["scheduled"] == 0


async def test_recycle_ready_refuses_when_pool_or_warm_instance_not_ready(
    admin_test_client: TestClient, admin_orchestrator: Orchestrator
) -> None:
    await admin_orchestrator.db.create_instance(
        5000, "10.0.0.100", "warm", "provisioning", "OldOwner/example-app"
    )
    response = admin_test_client.post("/api/v1/admin/recycle-ready")
    assert response.status_code == 409


def test_save_policies_rejects_missing_repositories_field(admin_test_client: TestClient) -> None:
    response = admin_test_client.put("/api/v1/admin/policies", json={})
    assert response.status_code == 400


def test_save_policies_rejects_invalid_installations_type(admin_test_client: TestClient) -> None:
    response = admin_test_client.put(
        "/api/v1/admin/policies",
        json={"repositories": {}, "installations": "not-an-object"},
    )
    assert response.status_code == 400


def test_cache_purge_rejects_missing_repository(admin_test_client: TestClient) -> None:
    response = admin_test_client.post("/api/v1/admin/cache/purge", json={})
    assert response.status_code == 400


def test_instances_and_jobs_routes_return_lists(admin_test_client: TestClient) -> None:
    assert admin_test_client.get("/api/v1/instances").json() == []
    assert admin_test_client.get("/api/v1/jobs").json() == []


def test_snapshot_route_returns_dashboard_payload(admin_test_client: TestClient) -> None:
    response = admin_test_client.get("/api/v1/snapshot")
    assert response.status_code == 200
    assert "instances" in response.json()


# --- Admin setup and authentication ------------------------------------------


def _setup_client(tmp_dir: str) -> tuple[TestClient, Any]:
    """A client on a fresh, unclaimed instance."""
    secret_path = Path(tmp_dir) / "webhook-secret"
    secret_path.write_text("test-secret", encoding="utf-8")
    base = settings(database_path=str(Path(tmp_dir) / "state.db"), shadow_mode=True)
    cfg = replace(base, github=replace(base.github, webhook_secret_file=str(secret_path)))
    app = create_app(cfg)
    orchestrator: Orchestrator = app.state.orchestrator
    asyncio.run(orchestrator.db.initialize())
    orchestrator.github = cast(Any, FakeGitHub())
    orchestrator.proxmox = cast(Any, FakeProxmox())
    orchestrator.broker = cast(Any, FakeBroker())
    return TestClient(app), app.state.admin_credential


def test_a_fresh_instance_reports_itself_unclaimed(tmp_path: Path) -> None:
    client, _ = _setup_client(str(tmp_path))
    body = client.get("/api/v1/setup").json()
    assert body["claimed"] is False
    assert body["can_claim"] is True
    assert body["claim_window_seconds"] > 0


def test_claiming_sets_the_password_without_touching_the_host(tmp_path: Path) -> None:
    """The whole point: setup happens in the app, not in a shell."""
    client, credential = _setup_client(str(tmp_path))

    response = client.post("/api/v1/setup/claim", json={"password": "correct-horse-battery"})

    assert response.status_code == 200
    assert credential.is_claimed
    assert client.get("/api/v1/setup").json()["claimed"] is True


def test_a_claimed_instance_cannot_be_claimed_again(tmp_path: Path) -> None:
    """Otherwise the setup endpoint is a permanent password reset for anyone."""
    client, _ = _setup_client(str(tmp_path))
    client.post("/api/v1/setup/claim", json={"password": "correct-horse-battery"})

    again = client.post("/api/v1/setup/claim", json={"password": "someone-elses-choice"})

    assert again.status_code == 409


def test_claiming_closes_after_the_first_run_window(tmp_path: Path) -> None:
    """An instance left running for weeks must not still be adoptable."""
    import time as _time

    from orchestrator.auth import CLAIM_WINDOW_SECONDS

    client, credential = _setup_client(str(tmp_path))
    credential.started_at = _time.monotonic() - CLAIM_WINDOW_SECONDS - 1

    assert client.get("/api/v1/setup").json()["can_claim"] is False
    response = client.post("/api/v1/setup/claim", json={"password": "too-late-to-claim"})
    assert response.status_code == 409


def test_short_passwords_are_refused(tmp_path: Path) -> None:
    client, _ = _setup_client(str(tmp_path))
    assert client.post("/api/v1/setup/claim", json={"password": "short"}).status_code == 400


def test_admin_routes_open_until_claimed_then_require_the_password(tmp_path: Path) -> None:
    """Unclaimed cannot mean locked out -- there would be no way back in."""
    client, _ = _setup_client(str(tmp_path))
    marker = tmp_path / "drain"
    with (
        mock.patch("orchestrator.api.DRAIN_MARKER", marker),
        mock.patch("orchestrator.service.DRAIN_MARKER", marker),
    ):
        assert client.post("/api/v1/admin/drain").status_code == 200

        client.post("/api/v1/setup/claim", json={"password": "correct-horse-battery"})

        assert client.post("/api/v1/admin/resume").status_code == 401
        assert (
            client.post(
                "/api/v1/admin/resume", headers={"X-Admin-Token": "correct-horse-battery"}
            ).status_code
            == 200
        )
        assert (
            client.post(
                "/api/v1/admin/drain",
                headers={"Authorization": "Bearer correct-horse-battery"},
            ).status_code
            == 200
        )


def test_wrong_password_is_rejected(tmp_path: Path) -> None:
    client, _ = _setup_client(str(tmp_path))
    client.post("/api/v1/setup/claim", json={"password": "correct-horse-battery"})

    response = client.post("/api/v1/admin/drain", headers={"X-Admin-Token": "guess"})

    assert response.status_code == 401


def test_password_can_be_changed_from_the_running_app(tmp_path: Path) -> None:
    client, credential = _setup_client(str(tmp_path))
    client.post("/api/v1/setup/claim", json={"password": "correct-horse-battery"})
    headers = {"X-Admin-Token": "correct-horse-battery"}

    assert (
        client.post(
            "/api/v1/admin/password",
            headers=headers,
            json={"current": "wrong-one-entirely", "new": "another-long-password"},
        ).status_code
        == 401
    )
    assert (
        client.post(
            "/api/v1/admin/password",
            headers=headers,
            json={"current": "correct-horse-battery", "new": "another-long-password"},
        ).status_code
        == 200
    )
    # The new password is live and the old one is dead.
    assert credential.verify("another-long-password")
    assert not credential.verify("correct-horse-battery")


def test_the_credential_file_never_holds_the_password(tmp_path: Path) -> None:
    """A stolen file must not hand over the password itself."""
    client, credential = _setup_client(str(tmp_path))
    client.post("/api/v1/setup/claim", json={"password": "correct-horse-battery"})

    raw = credential.path.read_text(encoding="utf-8")

    assert "correct-horse-battery" not in raw
    assert credential.verify("correct-horse-battery")
    assert not credential.verify("correct-horse-batteru")
    assert oct(credential.path.stat().st_mode)[-3:] == "600"


def test_read_endpoints_stay_unauthenticated(tmp_path: Path) -> None:
    """The dashboard is read-mostly and sits behind a reverse proxy."""
    client, _ = _setup_client(str(tmp_path))
    client.post("/api/v1/setup/claim", json={"password": "correct-horse-battery"})
    assert client.get("/api/v1/overview").status_code == 200
    assert client.get("/api/v1/dependencies").status_code == 200


def test_dependencies_endpoint_reports_setup_state(tmp_path: Path) -> None:
    """Unclaimed must be visible, never silently open."""
    client, _ = _setup_client(str(tmp_path))
    assert client.get("/api/v1/dependencies").json()["admin_auth"]["claimed"] is False
    client.post("/api/v1/setup/claim", json={"password": "correct-horse-battery"})
    assert client.get("/api/v1/dependencies").json()["admin_auth"]["claimed"] is True


def test_verify_endpoint_distinguishes_wrong_password_from_failed_action(
    tmp_path: Path,
) -> None:
    """Sign-in needs to tell a bad password from an action that went wrong."""
    client, _ = _setup_client(str(tmp_path))
    client.post("/api/v1/setup/claim", json={"password": "correct-horse-battery"})

    assert client.post("/api/v1/admin/verify").status_code == 401
    assert client.post("/api/v1/admin/verify", headers={"X-Admin-Token": "nope"}).status_code == 401
    ok = client.post("/api/v1/admin/verify", headers={"X-Admin-Token": "correct-horse-battery"})
    assert ok.status_code == 200
    assert ok.json()["ok"] is True


def test_ui_reports_admin_state_from_the_field_the_api_sends() -> None:
    """Regression: the panel read `admin_auth.enabled`, a field that had been
    renamed to `claimed`, so a secured instance was reported as unprotected --
    a security control stating the opposite of the truth."""
    from orchestrator.api import ui_directory

    html = (ui_directory() / "index.html").read_text(encoding="utf-8")
    assert "admin_auth?.enabled" not in html
    assert "admin_auth?.claimed" in html


def test_ui_gates_changes_behind_a_lock_not_an_account() -> None:
    """There is no user, no session and no profile here -- one password that
    enables changes. Borrowing "sign in / sign out" promises an identity model
    that does not exist and sets the wrong expectation."""
    ui = ui_directory_for_test()
    html = (ui / "index.html").read_text(encoding="utf-8")
    js = (ui / "js" / "app.js").read_text(encoding="utf-8")

    for hook in ("openUnlock", "openPasswordChange", "lock()"):
        assert hook in html, f"{hook} has no affordance in the UI"
    for hook in ("openUnlock", "submitUnlock", "openPasswordChange", "canChange"):
        assert hook in js, f"{hook} is not implemented"

    assert "Sign in" not in html and "Sign out" not in html
    assert "Unlock" in html and "Locked" in html
    # Persistence beyond the tab stays an explicit, labelled choice.
    assert "Stay unlocked on this device" in html
    assert "localStorage" in js


def test_destructive_actions_are_disabled_while_locked() -> None:
    """Letting them fire and 401 teaches nothing. Disabled with a reason does."""
    html = (ui_directory_for_test() / "index.html").read_text(encoding="utf-8")
    assert html.count(':disabled="!canChange"') >= 5
    assert html.count(':title="lockReason"') >= 6


def test_access_control_is_not_presented_as_dependency_health() -> None:
    """It is neither a dependency nor a health signal; it lived on that page
    only because it was convenient to put it there."""
    html = (ui_directory_for_test() / "index.html").read_text(encoding="utf-8")
    assert "Admin access" not in html


def test_webfonts_are_vendored_and_resolve() -> None:
    """The no-external-request rule is the whole reason the fonts are in the
    repo: a CDN reference works on the developer's laptop and renders nothing on
    the air-gapped LAN this actually runs on. A missing FILE fails the same way,
    silently, so every `url()` is resolved against the disk here."""
    ui = ui_directory_for_test()
    css = (ui / "css" / "tokens.css").read_text(encoding="utf-8")

    referenced = re.findall(r'url\("([^"]+)"\)', css)
    assert referenced, "no @font-face src found — the identity lost its type"
    for ref in referenced:
        assert not ref.startswith(("http", "//")), f"{ref} is an external request"
        assert (ui / "css" / ref).resolve().is_file(), f"{ref} is referenced but not vendored"

    # The system stack stays behind Plex, so a font that fails to load degrades
    # to what shipped before rather than to a serif default.
    assert '--sans: "IBM Plex Sans", ui-sans-serif, system-ui' in css
    assert '--mono: "IBM Plex Mono", ui-monospace' in css


def test_preloaded_fonts_match_the_urls_the_css_asks_for() -> None:
    """A preload whose URL differs by even a query string is worse than none:
    the browser downloads the file twice. asset_version() rewrites css/js/vendor
    only, which is what keeps these two spellings identical -- so if that regex
    ever grows `fonts`, this fails rather than quietly doubling the traffic."""
    ui = ui_directory_for_test()
    html = (ui / "index.html").read_text(encoding="utf-8")
    css = (ui / "css" / "tokens.css").read_text(encoding="utf-8")

    preloaded = re.findall(r'<link rel="preload" href="([^"]+)"[^>]*as="font"', html)
    assert preloaded, "fonts are render-blocking; preload them"
    css_urls = {ref.replace("../", "") for ref in re.findall(r'url\("([^"]+)"\)', css)}
    for ref in preloaded:
        assert "?" not in ref, f"{ref} carries a query the CSS url() will not have"
        assert ref in css_urls, f"{ref} is preloaded but no @font-face uses it"
        assert "crossorigin" in html.split(ref)[1].split(">")[0], (
            f"{ref} needs crossorigin or the preload is discarded"
        )


def test_asset_versioning_leaves_font_urls_alone() -> None:
    """Guards the assumption the preload test rests on, at the source."""
    import inspect

    from orchestrator import app as app_module

    source = inspect.getsource(app_module.create_app)
    assert r"(?:css|js|vendor)/" in source, "the version regex changed; recheck fonts"


def test_the_product_is_named_consistently() -> None:
    """The name is `Orchestra`, capitalised, and the subtitle spells out what it
    orchestrates. Lowercase `orchestra` read as a stray word rather than a name."""
    html = (ui_directory_for_test() / "index.html").read_text(encoding="utf-8")

    assert ">Orchestra<" in html
    assert ">Proxmox Runner Orchestrator<" in html
    assert ">orchestra<" not in html
    assert "<title>Orchestra — Proxmox Runner Orchestrator</title>" in html


def test_the_mark_is_a_drawn_gesture_not_a_box_of_bars() -> None:
    """The previous mark was three rounded bars inside a rounded square, which
    is the house style of every dashboard ever shipped. The identity is a brush
    stroke: filled paths, no container, no stroke, no rectangles."""
    html = (ui_directory_for_test() / "index.html").read_text(encoding="utf-8")
    mark = html.split('class="brand__mark"')[1].split("</svg>")[0]

    assert "<rect" not in mark, "the mark is drawn, not assembled from rectangles"
    assert "<path" in mark and mark.count("<circle") == 3
    # The favicon is the same drawing, so the two can never drift apart.
    assert "M5,72 C20,34 56,12 96,32 C62,26 27,54 5,72 Z" in mark
    assert "M5,72 C20,34 56,12 96,32 C62,26 27,54 5,72 Z" in html.split('rel="icon"')[1]


def test_infinite_animations_are_stopped_under_reduced_motion() -> None:
    """tokens.css collapses `var(--t-*)` durations, which silently misses any
    animation whose duration is a literal. The three infinite ones were exactly
    that, so they kept moving for people who asked the OS for stillness."""
    css = (ui_directory_for_test() / "css" / "app.css").read_text(encoding="utf-8")
    body, guard = css.split("@media (prefers-reduced-motion: reduce)")

    # Every perpetual animation outside the guard needs a counterpart inside it.
    # Counting rather than naming is what makes this catch the NEXT one added.
    perpetual = re.findall(r"animation:\s*[^;]*\binfinite\b", body)
    assert len(perpetual) == guard.count("animation: none"), (
        f"{len(perpetual)} infinite animations, "
        f"{guard.count('animation: none')} disabled under reduced motion"
    )
    for selector in (".brand__mark .glyph", ".btn.is-busy::after", ".skel::after"):
        assert selector in guard, f"{selector} keeps animating under reduced motion"


def ui_directory_for_test():
    from orchestrator.api import ui_directory

    return ui_directory()

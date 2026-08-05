from __future__ import annotations

import pytest

from orchestrator.config import (
    CapacityConfig,
    ConfigError,
    GitHubInstallation,
    RepositoryPolicy,
    RunnerProfile,
    parse_repository_policies,
    validate_capacity_budget,
    validate_repository_profiles,
)
from orchestrator.policy import cache_slug, decide
from tests.helpers import settings


def payload(labels: list[str] | None = None) -> dict[str, object]:
    return {
        "action": "queued",
        "repository": {"full_name": "OldOwner/example-app"},
        "workflow_job": {
            "id": 42,
            "run_id": 9,
            "labels": labels or ["self-hosted", "proxmox-lxc", "crossbuild"],
        },
    }


def test_default_ephemeral_mode_is_accepted() -> None:
    decision = decide(settings(), payload(), "push")
    assert decision.accepted
    assert decision.mode == "ephemeral"


def test_explicit_pool_mode_is_accepted() -> None:
    decision = decide(
        settings(),
        payload(["self-hosted", "proxmox-lxc", "crossbuild", "mode-pool"]),
        "workflow_dispatch",
    )
    assert decision.accepted
    assert decision.mode == "pool"


def test_repository_name_is_case_insensitive() -> None:
    case_variant = payload()
    case_variant["repository"] = {"full_name": "OldOwner/example-app"}
    decision = decide(settings(), case_variant, "push")
    assert decision.accepted
    assert decision.repository == "OldOwner/example-app"


def test_untrusted_pull_request_event_is_rejected() -> None:
    assert decide(settings(), payload(), "pull_request").reason == "event_not_allowed"


def test_missing_required_label_is_rejected() -> None:
    assert decide(settings(), payload(["self-hosted"]), "push").reason == "required_labels_missing"


def test_unknown_repository_is_rejected() -> None:
    other = payload()
    other["repository"] = {"full_name": "OldOwner/OtherRepo"}
    decision = decide(settings(), other, "push")
    assert not decision.accepted
    assert decision.reason == "repository_not_allowed"


def test_retired_temp_repository_is_rejected() -> None:
    temp = payload()
    temp["repository"] = {"full_name": "OldOwner/example-app-Temp"}
    decision = decide(settings(), temp, "workflow_dispatch")
    assert not decision.accepted
    assert decision.reason == "repository_not_allowed"


def test_cache_slug_rejects_path_traversal() -> None:
    with pytest.raises(ValueError):
        cache_slug("OldOwner/../example-app")


def test_rejects_warm_plus_pool_over_max_active() -> None:
    capacity = CapacityConfig(max_active=3, pool_size=2)
    repositories = {"a/repo": RepositoryPolicy(warm_capacity=2)}
    with pytest.raises(ConfigError):
        validate_capacity_budget(capacity, repositories)


def test_accepts_warm_plus_pool_within_max_active() -> None:
    capacity = CapacityConfig(max_active=4, pool_size=1)
    repositories = {
        "a/repo": RepositoryPolicy(warm_capacity=1),
        "b/repo": RepositoryPolicy(warm_capacity=2),
    }
    validate_capacity_budget(capacity, repositories)  # must not raise


@pytest.fixture
def installations() -> dict[str, GitHubInstallation]:
    return {
        "personal": GitHubInstallation(account="OldOwner", installation_id=1, scope="repository")
    }


@pytest.mark.parametrize(
    ("overrides", "expected_error"),
    [
        pytest.param({"max_concurrent": -1}, ConfigError, id="rejects_negative_max_concurrent"),
        pytest.param(
            {"warm_capacity": 3, "max_concurrent": 1},
            ConfigError,
            id="rejects_max_concurrent_below_warm_capacity",
        ),
        pytest.param(
            {"warm_capacity": 1, "max_concurrent": 2}, None, id="accepts_valid_max_concurrent"
        ),
        pytest.param({"warm_capacity": 5}, None, id="zero_max_concurrent_means_unlimited"),
    ],
)
def test_max_concurrent_validation(
    installations: dict[str, GitHubInstallation],
    overrides: dict[str, object],
    expected_error: type[Exception] | None,
) -> None:
    raw = {"OldOwner/example-app": {"github_installation": "personal", **overrides}}
    if expected_error is not None:
        with pytest.raises(expected_error):
            parse_repository_policies(raw, installations)
        return
    parsed = parse_repository_policies(raw, installations)
    expected_max_concurrent = overrides.get("max_concurrent", 0)
    assert parsed["OldOwner/example-app"].max_concurrent == expected_max_concurrent


@pytest.mark.parametrize(
    ("overrides", "expected_error", "expected"),
    [
        pytest.param(
            {"warm_capacity": 4},
            None,
            {"warm_idle_capacity": -1, "warm_idle_after_seconds": 0},
            id="disabled_by_default",
        ),
        pytest.param(
            {"warm_capacity": 4, "warm_idle_capacity": 1, "warm_idle_after_seconds": 3600},
            None,
            {"warm_idle_capacity": 1, "warm_idle_after_seconds": 3600},
            id="accepts_valid_idle_scaling_config",
        ),
        pytest.param(
            {"warm_capacity": 2, "warm_idle_capacity": 3, "warm_idle_after_seconds": 60},
            ConfigError,
            None,
            id="rejects_idle_capacity_above_warm_capacity",
        ),
        pytest.param(
            {"warm_capacity": 4, "warm_idle_capacity": 1},
            ConfigError,
            None,
            id="rejects_idle_capacity_without_idle_seconds",
        ),
        pytest.param(
            {"warm_capacity": 4, "warm_idle_capacity": -2},
            ConfigError,
            None,
            id="rejects_below_minus_one",
        ),
    ],
)
def test_warm_idle_capacity_validation(
    installations: dict[str, GitHubInstallation],
    overrides: dict[str, object],
    expected_error: type[Exception] | None,
    expected: dict[str, object] | None,
) -> None:
    raw = {"OldOwner/example-app": {"github_installation": "personal", **overrides}}
    if expected_error is not None:
        with pytest.raises(expected_error):
            parse_repository_policies(raw, installations)
        return
    policy = parse_repository_policies(raw, installations)["OldOwner/example-app"]
    assert expected is not None
    assert policy.warm_idle_capacity == expected["warm_idle_capacity"]
    assert policy.warm_idle_after_seconds == expected["warm_idle_after_seconds"]


def test_defaults_to_crossbuild_small(installations: dict[str, GitHubInstallation]) -> None:
    raw = {"OldOwner/example-app": {"github_installation": "personal"}}
    parsed = parse_repository_policies(raw, installations)
    assert parsed["OldOwner/example-app"].profile == "crossbuild-small"


def test_accepts_custom_profile_name(installations: dict[str, GitHubInstallation]) -> None:
    raw = {
        "OldOwner/example-app": {
            "github_installation": "personal",
            "profile": "crossbuild-large",
        }
    }
    parsed = parse_repository_policies(raw, installations)
    assert parsed["OldOwner/example-app"].profile == "crossbuild-large"


def test_rejects_empty_profile(installations: dict[str, GitHubInstallation]) -> None:
    raw = {"OldOwner/example-app": {"github_installation": "personal", "profile": "  "}}
    with pytest.raises(ConfigError):
        parse_repository_policies(raw, installations)


def test_validate_repository_profiles_requires_default_profile() -> None:
    repositories = {"a/repo": RepositoryPolicy()}
    with pytest.raises(ConfigError):
        validate_repository_profiles(repositories, {})


def test_validate_repository_profiles_rejects_unknown_profile() -> None:
    repositories = {"a/repo": RepositoryPolicy(profile="does-not-exist")}
    profiles = {"crossbuild-small": RunnerProfile()}
    with pytest.raises(ConfigError):
        validate_repository_profiles(repositories, profiles)


def test_validate_repository_profiles_accepts_known_profiles() -> None:
    repositories = {
        "a/repo": RepositoryPolicy(profile="crossbuild-small"),
        "b/repo": RepositoryPolicy(profile="crossbuild-large"),
    }
    profiles = {
        "crossbuild-small": RunnerProfile(),
        "crossbuild-large": RunnerProfile(cores=12),
    }
    validate_repository_profiles(repositories, profiles)  # must not raise

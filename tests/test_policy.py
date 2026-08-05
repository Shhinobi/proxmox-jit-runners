from __future__ import annotations

import unittest

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
        "repository": {"full_name": "MetalDevops/NinjaCrawler"},
        "workflow_job": {
            "id": 42,
            "run_id": 9,
            "labels": labels or ["self-hosted", "proxmox-lxc", "crossbuild"],
        },
    }


class PolicyTests(unittest.TestCase):
    def test_default_ephemeral_mode_is_accepted(self) -> None:
        decision = decide(settings(), payload(), "push")
        self.assertTrue(decision.accepted)
        self.assertEqual(decision.mode, "ephemeral")

    def test_explicit_pool_mode_is_accepted(self) -> None:
        decision = decide(settings(), payload(["self-hosted", "proxmox-lxc", "crossbuild", "mode-pool"]), "workflow_dispatch")
        self.assertTrue(decision.accepted)
        self.assertEqual(decision.mode, "pool")

    def test_repository_name_is_case_insensitive(self) -> None:
        case_variant = payload()
        case_variant["repository"] = {"full_name": "MetalDevOps/NinjaCrawler"}
        decision = decide(settings(), case_variant, "push")
        self.assertTrue(decision.accepted)
        self.assertEqual(decision.repository, "MetalDevops/NinjaCrawler")

    def test_untrusted_pull_request_event_is_rejected(self) -> None:
        self.assertEqual(decide(settings(), payload(), "pull_request").reason, "event_not_allowed")

    def test_missing_required_label_is_rejected(self) -> None:
        self.assertEqual(decide(settings(), payload(["self-hosted"]), "push").reason, "required_labels_missing")

    def test_unknown_repository_is_rejected(self) -> None:
        other = payload()
        other["repository"] = {"full_name": "MetalDevops/OtherRepo"}
        decision = decide(settings(), other, "push")
        self.assertFalse(decision.accepted)
        self.assertEqual(decision.reason, "repository_not_allowed")

    def test_retired_temp_repository_is_rejected(self) -> None:
        temp = payload()
        temp["repository"] = {"full_name": "MetalDevOps/NinjaCrawler-Temp"}
        decision = decide(settings(), temp, "workflow_dispatch")
        self.assertFalse(decision.accepted)
        self.assertEqual(decision.reason, "repository_not_allowed")

    def test_cache_slug_rejects_path_traversal(self) -> None:
        with self.assertRaises(ValueError):
            cache_slug("MetalDevOps/../NinjaCrawler")


class CapacityBudgetTests(unittest.TestCase):
    def test_rejects_warm_plus_pool_over_max_active(self) -> None:
        capacity = CapacityConfig(max_active=3, pool_size=2)
        repositories = {
            "a/repo": RepositoryPolicy(warm_capacity=2),
        }
        with self.assertRaises(ConfigError):
            validate_capacity_budget(capacity, repositories)

    def test_accepts_warm_plus_pool_within_max_active(self) -> None:
        capacity = CapacityConfig(max_active=4, pool_size=1)
        repositories = {
            "a/repo": RepositoryPolicy(warm_capacity=1),
            "b/repo": RepositoryPolicy(warm_capacity=2),
        }
        validate_capacity_budget(capacity, repositories)  # must not raise


class RepositoryPolicyMaxConcurrentTests(unittest.TestCase):
    def _installations(self) -> dict[str, GitHubInstallation]:
        return {"personal": GitHubInstallation(account="MetalDevops", installation_id=1, scope="repository")}

    def test_rejects_negative_max_concurrent(self) -> None:
        raw = {
            "MetalDevops/NinjaCrawler": {
                "github_installation": "personal",
                "max_concurrent": -1,
            }
        }
        with self.assertRaises(ConfigError):
            parse_repository_policies(raw, self._installations())

    def test_rejects_max_concurrent_below_warm_capacity(self) -> None:
        raw = {
            "MetalDevops/NinjaCrawler": {
                "github_installation": "personal",
                "warm_capacity": 3,
                "max_concurrent": 1,
            }
        }
        with self.assertRaises(ConfigError):
            parse_repository_policies(raw, self._installations())

    def test_accepts_valid_max_concurrent(self) -> None:
        raw = {
            "MetalDevops/NinjaCrawler": {
                "github_installation": "personal",
                "warm_capacity": 1,
                "max_concurrent": 2,
            }
        }
        parsed = parse_repository_policies(raw, self._installations())
        self.assertEqual(parsed["MetalDevops/NinjaCrawler"].max_concurrent, 2)

    def test_zero_max_concurrent_means_unlimited(self) -> None:
        raw = {
            "MetalDevops/NinjaCrawler": {
                "github_installation": "personal",
                "warm_capacity": 5,
            }
        }
        parsed = parse_repository_policies(raw, self._installations())
        self.assertEqual(parsed["MetalDevops/NinjaCrawler"].max_concurrent, 0)


class WarmIdleCapacityValidationTests(unittest.TestCase):
    def _installations(self) -> dict[str, GitHubInstallation]:
        return {"personal": GitHubInstallation(account="MetalDevops", installation_id=1, scope="repository")}

    def test_disabled_by_default(self) -> None:
        raw = {
            "MetalDevops/NinjaCrawler": {
                "github_installation": "personal",
                "warm_capacity": 4,
            }
        }
        parsed = parse_repository_policies(raw, self._installations())
        policy = parsed["MetalDevops/NinjaCrawler"]
        self.assertEqual(policy.warm_idle_capacity, -1)
        self.assertEqual(policy.warm_idle_after_seconds, 0)

    def test_accepts_valid_idle_scaling_config(self) -> None:
        raw = {
            "MetalDevops/NinjaCrawler": {
                "github_installation": "personal",
                "warm_capacity": 4,
                "warm_idle_capacity": 1,
                "warm_idle_after_seconds": 3600,
            }
        }
        parsed = parse_repository_policies(raw, self._installations())
        policy = parsed["MetalDevops/NinjaCrawler"]
        self.assertEqual(policy.warm_idle_capacity, 1)
        self.assertEqual(policy.warm_idle_after_seconds, 3600)

    def test_rejects_idle_capacity_above_warm_capacity(self) -> None:
        raw = {
            "MetalDevops/NinjaCrawler": {
                "github_installation": "personal",
                "warm_capacity": 2,
                "warm_idle_capacity": 3,
                "warm_idle_after_seconds": 60,
            }
        }
        with self.assertRaises(ConfigError):
            parse_repository_policies(raw, self._installations())

    def test_rejects_idle_capacity_without_idle_seconds(self) -> None:
        raw = {
            "MetalDevops/NinjaCrawler": {
                "github_installation": "personal",
                "warm_capacity": 4,
                "warm_idle_capacity": 1,
            }
        }
        with self.assertRaises(ConfigError):
            parse_repository_policies(raw, self._installations())

    def test_rejects_below_minus_one(self) -> None:
        raw = {
            "MetalDevops/NinjaCrawler": {
                "github_installation": "personal",
                "warm_capacity": 4,
                "warm_idle_capacity": -2,
            }
        }
        with self.assertRaises(ConfigError):
            parse_repository_policies(raw, self._installations())


class RepositoryProfileValidationTests(unittest.TestCase):
    def _installations(self) -> dict[str, GitHubInstallation]:
        return {"personal": GitHubInstallation(account="MetalDevops", installation_id=1, scope="repository")}

    def test_defaults_to_crossbuild_small(self) -> None:
        raw = {"MetalDevops/NinjaCrawler": {"github_installation": "personal"}}
        parsed = parse_repository_policies(raw, self._installations())
        self.assertEqual(parsed["MetalDevops/NinjaCrawler"].profile, "crossbuild-small")

    def test_accepts_custom_profile_name(self) -> None:
        raw = {
            "MetalDevops/NinjaCrawler": {
                "github_installation": "personal",
                "profile": "crossbuild-large",
            }
        }
        parsed = parse_repository_policies(raw, self._installations())
        self.assertEqual(parsed["MetalDevops/NinjaCrawler"].profile, "crossbuild-large")

    def test_rejects_empty_profile(self) -> None:
        raw = {"MetalDevops/NinjaCrawler": {"github_installation": "personal", "profile": "  "}}
        with self.assertRaises(ConfigError):
            parse_repository_policies(raw, self._installations())

    def test_validate_repository_profiles_requires_default_profile(self) -> None:
        repositories = {"a/repo": RepositoryPolicy()}
        with self.assertRaises(ConfigError):
            validate_repository_profiles(repositories, {})

    def test_validate_repository_profiles_rejects_unknown_profile(self) -> None:
        repositories = {"a/repo": RepositoryPolicy(profile="does-not-exist")}
        profiles = {"crossbuild-small": RunnerProfile()}
        with self.assertRaises(ConfigError):
            validate_repository_profiles(repositories, profiles)

    def test_validate_repository_profiles_accepts_known_profiles(self) -> None:
        repositories = {
            "a/repo": RepositoryPolicy(profile="crossbuild-small"),
            "b/repo": RepositoryPolicy(profile="crossbuild-large"),
        }
        profiles = {
            "crossbuild-small": RunnerProfile(),
            "crossbuild-large": RunnerProfile(cores=12),
        }
        validate_repository_profiles(repositories, profiles)  # must not raise

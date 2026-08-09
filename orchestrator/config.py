from __future__ import annotations

import logging
import os
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any

import yaml

LOGGER = logging.getLogger("runner_orchestrator")


class ConfigError(ValueError):
    pass


# Fallback profile name: the only one a fresh install ships with, and the one
# pool-mode CTs always use (pool clones are pre-staged before any repository
# claims them, so there's no per-repo preference to read yet).
DEFAULT_PROFILE_NAME = "crossbuild-small"


@dataclass(frozen=True)
class RepositoryPolicy:
    github_installation: str = ""
    default_mode: str = "ephemeral"
    warm_capacity: int = 0
    # 0 = no per-repo ceiling (only the global capacity.max_active applies).
    max_concurrent: int = 0
    # Elastic warm pool: scale warm_capacity down to warm_idle_capacity after
    # warm_idle_after_seconds with no job activity for this repo, then scale
    # back up to warm_capacity as soon as a job request arrives again.
    # -1 = disabled (warm_capacity stays fixed, today's behavior).
    warm_idle_capacity: int = -1
    warm_idle_after_seconds: int = 0
    # Must name a key in Settings.profiles (validated at load/save time).
    profile: str = DEFAULT_PROFILE_NAME
    allowed_modes: tuple[str, ...] = ("ephemeral", "pool", "persistent")
    allowed_events: tuple[str, ...] = ("push", "workflow_dispatch", "schedule")
    required_labels: tuple[str, ...] = ("self-hosted", "proxmox-lxc", "crossbuild")


@dataclass(frozen=True)
class RunnerProfile:
    cores: int = 4
    memory_mb: int = 4096
    swap_mb: int = 2048
    rootfs_gb: int = 25


@dataclass(frozen=True)
class GitHubConfig:
    app_id: str
    private_key_file: str
    webhook_secret_file: str
    installations: dict[str, GitHubInstallation]
    api_url: str = "https://api.github.com"
    api_version: str = "2022-11-28"


@dataclass(frozen=True)
class GitHubInstallation:
    account: str
    installation_id: int
    scope: str
    runner_group_id: int = 1


@dataclass(frozen=True)
class ProxmoxConfig:
    endpoint: str
    api_token_file: str
    node: str = "pve"
    template_vmid: int = 9101
    storage: str = "local-lvm"
    bridge: str = "vci"
    gateway: str = "10.0.0.1"
    vmid_start: int = 5000
    vmid_end: int = 5099
    ip_start: int = 100
    ip_end: int = 199
    verify_tls: bool = True
    # Outbound exceptions to the RFC1918 deny applied to every runner.
    egress_allow: tuple[EgressRule, ...] = ()


@dataclass(frozen=True)
class EgressRule:
    """One outbound exception punched through the runner firewall.

    Runner containers deny lateral RFC1918 access by default. Anything a build
    legitimately needs to reach inside the network -- a telemetry collector, a
    deploy target -- is site-specific, so it belongs in configuration rather
    than baked into the application.
    """

    dest: str
    proto: str = "tcp"
    dport: str = ""
    comment: str = "configured egress exception"


@dataclass(frozen=True)
class BrokerConfig:
    # No default: the broker runs on your Proxmox host, and guessing an
    # address would silently point a fresh install at someone else's network.
    host: str = ""
    user: str = "runner-broker"
    key_file: str = "/run/credentials/runner_broker_ssh_key"
    known_hosts_file: str = "/etc/runner-orchestrator/ssh_known_hosts"


@dataclass(frozen=True)
class CacheConfig:
    root: str = "/srv/runner-cache"
    mount_path: str = "/cache"
    # Usage/quota enforcement lives entirely on the Proxmox host, in the
    # broker script
    # (RUNNER_CACHE_MAX_PERCENT / RUNNER_CACHE_QUOTA_GB) — there is no field
    # here for it. The dashboard reads those values from the broker's own
    # cache-list output, not from this config.
    # Auto-GC orphan dirs (not on allowlist) older than N days during reconcile.
    # 0 disables automatic GC (manual purge/gc via API still works).
    gc_orphan_days: int = 7


@dataclass(frozen=True)
class ApiConfig:
    """Protection for the mutating API surface."""

    admin_token_file: str = ""
    allow_unauthenticated_admin: bool = True
    """Permitted for one release so an existing deployment does not break on
    restart; flipped to False once operators have provisioned a token."""


@dataclass(frozen=True)
class StorageConfig:
    """Admission control against the Proxmox storage the runners clone into.

    `max_active` is a *count*, not a budget: nothing ever multiplied it by
    `rootfs_gb` or compared the result to real free space. This section is the
    resource dimension that was missing.
    """

    guard_enabled: bool = True
    min_free_gb: int = 100
    """Hard floor. Below this, speculative capacity is refused."""
    max_used_percent: float = 80.0
    """Hard floor expressed as a ratio; whichever binds first wins."""
    warn_free_gb: int = 150
    """Soft floor: still allowed, but alerted on."""
    poll_seconds: int = 60
    total_budget_gb: int = 0
    """The orchestrator's declared share of a shared pool. 0 = warn only."""
    fail_open: bool = True
    """If the status query itself fails, allow clones rather than halting CI."""


@dataclass(frozen=True)
class CapacityConfig:
    max_active: int = 2
    pool_size: int = 2
    ready_timeout_seconds: int = 300
    max_job_seconds: int = 10800
    persistent_idle_seconds: int = 86400
    reconcile_seconds: int = 60
    # Warm recycle-in-place limits (re-JIT without clone). Exceeding forces destroy.
    warm_max_uses: int = 25
    warm_max_age_seconds: int = 14400  # 4h
    warm_destroy_on_failure: bool = True
    warm_min_root_mib: int = 2048  # destroy if rootfs free space below this after clean
    # Wall-clock a single reconcile pass may spend before deferring the rest.
    # Must exceed one worst-case teardown or nothing ever completes; validated below.
    reconcile_budget_seconds: int = 45
    # Retire idle pool containers past this age (0 disables). A week-old clone of
    # a golden template that has since been rebuilt is not what the operator thinks.
    pool_max_age_seconds: int = 86400
    # How often the out-of-band dependency probe runs. Independent of the
    # reconcile loop on purpose: a probe sharing the loop it diagnoses is
    # useless exactly when that loop stalls.
    dependency_probe_seconds: int = 30


# Fields the dashboard may edit at runtime (persisted in repositories-override.yaml).
CAPACITY_RUNTIME_FIELDS = ("max_active", "pool_size")


@dataclass(frozen=True)
class Settings:
    github: GitHubConfig
    proxmox: ProxmoxConfig
    broker: BrokerConfig
    cache: CacheConfig
    capacity: CapacityConfig
    repositories: dict[str, RepositoryPolicy]
    profiles: dict[str, RunnerProfile]
    # Defaulted so every existing construction keeps working; the guard is on
    # by default but only bites once a real storage reading contradicts it.
    storage: StorageConfig = field(default_factory=StorageConfig)
    api: ApiConfig = field(default_factory=ApiConfig)
    database_path: str = "/var/lib/runner-orchestrator/state.db"
    log_level: str = "INFO"
    shadow_mode: bool = True
    drain: bool = False


def _load_yaml(path: Path) -> dict[str, Any]:
    try:
        data = yaml.safe_load(path.read_text(encoding="utf-8"))
    except FileNotFoundError as exc:
        raise ConfigError(f"configuration file not found: {path}") from exc
    if not isinstance(data, dict):
        raise ConfigError("configuration root must be a mapping")
    return data


def _secret_override(value: str, env_name: str) -> str:
    return os.environ.get(env_name, value)


def config_path(path: str | Path | None = None) -> Path:
    return Path(
        path or os.environ.get("RUNNER_ORCHESTRATOR_CONFIG", "/etc/runner-orchestrator/config.yaml")
    )


def repositories_override_path(settings_database_path: str | None = None) -> Path:
    """Writable runtime override for repository policies (service user owns this tree)."""
    explicit = os.environ.get("RUNNER_ORCHESTRATOR_REPOSITORIES_FILE")
    if explicit:
        return Path(explicit)
    if settings_database_path:
        return Path(settings_database_path).resolve().parent / "repositories-override.yaml"
    return Path("/var/lib/runner-orchestrator/repositories-override.yaml")


def repository_policy_to_dict(policy: RepositoryPolicy) -> dict[str, Any]:
    return {
        "github_installation": policy.github_installation,
        "default_mode": policy.default_mode,
        "warm_capacity": int(policy.warm_capacity),
        "max_concurrent": int(policy.max_concurrent),
        "warm_idle_capacity": int(policy.warm_idle_capacity),
        "warm_idle_after_seconds": int(policy.warm_idle_after_seconds),
        "profile": policy.profile,
        "allowed_modes": list(policy.allowed_modes),
        "allowed_events": list(policy.allowed_events),
        "required_labels": list(policy.required_labels),
    }


def installation_to_dict(installation: GitHubInstallation) -> dict[str, Any]:
    return {
        "account": installation.account,
        "installation_id": int(installation.installation_id),
        "scope": installation.scope,
        "runner_group_id": int(installation.runner_group_id),
    }


def capacity_runtime_to_dict(capacity: CapacityConfig) -> dict[str, int]:
    """Dashboard-editable capacity knobs only."""
    return {field: int(getattr(capacity, field)) for field in CAPACITY_RUNTIME_FIELDS}


def parse_capacity_config(
    raw: dict[str, Any] | None,
    *,
    base: CapacityConfig | None = None,
) -> CapacityConfig:
    """Parse capacity mapping, optionally layering onto an existing base config."""
    seed = base or CapacityConfig()
    if raw is None:
        return seed
    if not isinstance(raw, dict):
        raise ConfigError("capacity must be a mapping")
    values: dict[str, Any] = {
        "max_active": int(seed.max_active),
        "pool_size": int(seed.pool_size),
        "ready_timeout_seconds": int(seed.ready_timeout_seconds),
        "max_job_seconds": int(seed.max_job_seconds),
        "persistent_idle_seconds": int(seed.persistent_idle_seconds),
        "reconcile_seconds": int(seed.reconcile_seconds),
        "warm_max_uses": int(seed.warm_max_uses),
        "warm_max_age_seconds": int(seed.warm_max_age_seconds),
        "warm_destroy_on_failure": bool(seed.warm_destroy_on_failure),
        "warm_min_root_mib": int(seed.warm_min_root_mib),
        "reconcile_budget_seconds": int(seed.reconcile_budget_seconds),
        "pool_max_age_seconds": int(seed.pool_max_age_seconds),
        "dependency_probe_seconds": int(seed.dependency_probe_seconds),
    }
    int_keys = {
        "max_active",
        "pool_size",
        "ready_timeout_seconds",
        "max_job_seconds",
        "persistent_idle_seconds",
        "reconcile_seconds",
        "warm_max_uses",
        "warm_max_age_seconds",
        "warm_min_root_mib",
        "reconcile_budget_seconds",
        "pool_max_age_seconds",
        "dependency_probe_seconds",
    }
    for key in int_keys:
        if key in raw and raw[key] is not None:
            try:
                values[key] = int(raw[key])
            except (TypeError, ValueError) as exc:
                raise ConfigError(f"capacity.{key} must be an integer") from exc
    if "warm_destroy_on_failure" in raw and raw["warm_destroy_on_failure"] is not None:
        values["warm_destroy_on_failure"] = bool(raw["warm_destroy_on_failure"])
    if values["max_active"] < 1:
        raise ConfigError("capacity.max_active must be >= 1")
    if values["pool_size"] < 0:
        raise ConfigError("capacity.pool_size must be >= 0")
    if values["ready_timeout_seconds"] < 1:
        raise ConfigError("capacity.ready_timeout_seconds must be >= 1")
    if values["max_job_seconds"] < 1:
        raise ConfigError("capacity.max_job_seconds must be >= 1")
    if values["persistent_idle_seconds"] < 1:
        raise ConfigError("capacity.persistent_idle_seconds must be >= 1")
    if values["reconcile_seconds"] < 1:
        raise ConfigError("capacity.reconcile_seconds must be >= 1")
    if values["warm_max_uses"] < 1:
        raise ConfigError("capacity.warm_max_uses must be >= 1")
    if values["warm_max_age_seconds"] < 60:
        raise ConfigError("capacity.warm_max_age_seconds must be >= 60")
    if values["warm_min_root_mib"] < 256:
        raise ConfigError("capacity.warm_min_root_mib must be >= 256")
    if values["pool_max_age_seconds"] < 0:
        raise ConfigError("capacity.pool_max_age_seconds must be >= 0")
    # A budget below one worst-case teardown would truncate every pass before
    # anything finished, which is worse than no budget at all.
    if values["dependency_probe_seconds"] < 5:
        raise ConfigError("capacity.dependency_probe_seconds must be >= 5")
    if values["reconcile_budget_seconds"] < 30:
        raise ConfigError("capacity.reconcile_budget_seconds must be >= 30")
    return CapacityConfig(**values)


def parse_egress_rules(raw: Any) -> tuple[EgressRule, ...]:
    if not raw:
        return ()
    if not isinstance(raw, list):
        raise ConfigError("proxmox.egress_allow must be a list")
    rules: list[EgressRule] = []
    for index, item in enumerate(raw):
        if not isinstance(item, dict):
            raise ConfigError(f"proxmox.egress_allow[{index}] must be a mapping")
        dest = str(item.get("dest", "")).strip()
        if not dest:
            raise ConfigError(f"proxmox.egress_allow[{index}].dest is required")
        rules.append(
            EgressRule(
                dest=dest,
                proto=str(item.get("proto", "tcp")),
                dport=str(item.get("dport", "") or ""),
                comment=str(item.get("comment", "configured egress exception")),
            )
        )
    return tuple(rules)


def parse_storage_config(raw: Any, seed: StorageConfig) -> StorageConfig:
    if raw is None:
        return seed
    if not isinstance(raw, dict):
        raise ConfigError("storage must be a mapping")
    values: dict[str, Any] = {
        "guard_enabled": bool(seed.guard_enabled),
        "min_free_gb": int(seed.min_free_gb),
        "max_used_percent": float(seed.max_used_percent),
        "warn_free_gb": int(seed.warn_free_gb),
        "poll_seconds": int(seed.poll_seconds),
        "total_budget_gb": int(seed.total_budget_gb),
        "fail_open": bool(seed.fail_open),
    }
    for key in ("min_free_gb", "warn_free_gb", "poll_seconds", "total_budget_gb"):
        if key in raw and raw[key] is not None:
            try:
                values[key] = int(raw[key])
            except (TypeError, ValueError) as exc:
                raise ConfigError(f"storage.{key} must be an integer") from exc
    if "max_used_percent" in raw and raw["max_used_percent"] is not None:
        try:
            values["max_used_percent"] = float(raw["max_used_percent"])
        except (TypeError, ValueError) as exc:
            raise ConfigError("storage.max_used_percent must be a number") from exc
    for key in ("guard_enabled", "fail_open"):
        if key in raw and raw[key] is not None:
            values[key] = bool(raw[key])
    if values["min_free_gb"] < 0:
        raise ConfigError("storage.min_free_gb must be >= 0")
    if not 1.0 <= values["max_used_percent"] <= 100.0:
        raise ConfigError("storage.max_used_percent must be between 1 and 100")
    if values["poll_seconds"] < 5:
        raise ConfigError("storage.poll_seconds must be >= 5")
    if values["total_budget_gb"] < 0:
        raise ConfigError("storage.total_budget_gb must be >= 0")
    return StorageConfig(**values)


def projected_disk_demand_gb(
    capacity: CapacityConfig,
    repositories: dict[str, RepositoryPolicy],
    profiles: dict[str, RunnerProfile],
) -> int:
    """Worst-case disk the current configuration is allowed to consume.

    `rootfs_gb` existed but was only ever echoed to the dashboard, so nothing
    connected "max_active = 11" to "11 x 25 GiB of thin pool". Thin provisioning
    means this over-estimates day-one usage — conservative in the right
    direction, since clones grow toward it.
    """
    warm_gb = 0
    warm_slots = 0
    for policy in repositories.values():
        profile = profiles.get(policy.profile) or profiles[DEFAULT_PROFILE_NAME]
        warm_gb += int(policy.warm_capacity) * int(profile.rootfs_gb)
        warm_slots += int(policy.warm_capacity)
    default_rootfs = int(profiles[DEFAULT_PROFILE_NAME].rootfs_gb)
    pool_gb = int(capacity.pool_size) * default_rootfs
    largest = max((int(p.rootfs_gb) for p in profiles.values()), default=default_rootfs)
    spare_slots = max(0, int(capacity.max_active) - warm_slots - int(capacity.pool_size))
    return warm_gb + pool_gb + spare_slots * largest


def validate_capacity_budget(
    capacity: CapacityConfig, repositories: dict[str, RepositoryPolicy]
) -> None:
    """Ensure max_active can hold both the warm targets and the pool.

    Ready pool/warm instances are pre-staged standby capacity (count_active()
    excludes idle pool rows on purpose), but there's still no point letting an
    operator configure more standby capacity than max_active could ever put to
    work — the excess would just sit there, cloned, unused.
    """
    warm_target = sum(int(policy.warm_capacity) for policy in repositories.values())
    budget = warm_target + int(capacity.pool_size)
    if budget > capacity.max_active:
        raise ConfigError(
            f"capacity.max_active ({capacity.max_active}) must be >= sum of "
            f"repository warm_capacity ({warm_target}) + capacity.pool_size "
            f"({capacity.pool_size}); raise max_active or lower warm/pool targets"
        )


def parse_github_installations(raw_installations: dict[str, Any]) -> dict[str, GitHubInstallation]:
    if not isinstance(raw_installations, dict) or not raw_installations:
        raise ConfigError("at least one GitHub installation is required")
    installations: dict[str, GitHubInstallation] = {}
    for name, value in raw_installations.items():
        key = str(name).strip()
        if not key:
            raise ConfigError("installation key must not be empty")
        if not isinstance(value, dict):
            raise ConfigError(f"{key}: installation must be a mapping")
        try:
            account = str(value["account"]).strip()
            installation_id = int(value["installation_id"])
            scope = str(value["scope"]).strip()
            runner_group_id = int(value.get("runner_group_id", 1))
        except (KeyError, TypeError, ValueError) as exc:
            raise ConfigError(f"{key}: invalid installation fields: {exc}") from exc
        if not account:
            raise ConfigError(f"{key}: account must not be empty")
        if installation_id <= 0:
            raise ConfigError(f"{key}: installation_id must be positive")
        if scope not in {"organization", "repository"}:
            raise ConfigError(f"{key}: scope must be organization or repository")
        if runner_group_id <= 0:
            raise ConfigError(f"{key}: runner_group_id must be positive")
        installations[key] = GitHubInstallation(
            account=account,
            installation_id=installation_id,
            scope=scope,
            runner_group_id=runner_group_id,
        )
    return installations


def parse_repository_policies(
    raw_repositories: dict[str, Any],
    installations: dict[str, GitHubInstallation],
) -> dict[str, RepositoryPolicy]:
    if not isinstance(raw_repositories, dict) or not raw_repositories:
        raise ConfigError("at least one repository policy is required")
    repositories = {
        str(name): RepositoryPolicy(
            github_installation=str(value["github_installation"]),
            default_mode=str(value.get("default_mode", "ephemeral")),
            warm_capacity=int(value.get("warm_capacity", 0)),
            max_concurrent=int(value.get("max_concurrent", 0)),
            warm_idle_capacity=int(value.get("warm_idle_capacity", -1)),
            warm_idle_after_seconds=int(value.get("warm_idle_after_seconds", 0)),
            profile=str(value.get("profile", DEFAULT_PROFILE_NAME)),
            allowed_modes=tuple(value.get("allowed_modes", ["ephemeral", "pool", "persistent"])),
            allowed_events=tuple(
                value.get("allowed_events", ["push", "workflow_dispatch", "schedule"])
            ),
            required_labels=tuple(
                value.get("required_labels", ["self-hosted", "proxmox-lxc", "crossbuild"])
            ),
        )
        for name, value in raw_repositories.items()
    }
    for repo, policy in repositories.items():
        if "/" not in repo or repo.count("/") != 1:
            raise ConfigError(f"{repo}: repository must be OWNER/NAME")
        owner, name = repo.split("/", 1)
        if not owner or not name:
            raise ConfigError(f"{repo}: repository must be OWNER/NAME")
        installation = installations.get(policy.github_installation)
        if installation is None:
            raise ConfigError(f"{repo}: unknown github_installation {policy.github_installation!r}")
        if installation.scope not in {"organization", "repository"}:
            raise ConfigError(
                f"{repo}: GitHub installation scope must be organization or repository"
            )
        if owner.lower() != installation.account.lower():
            raise ConfigError(
                f"{repo}: repository owner does not match GitHub installation account"
            )
        if policy.default_mode not in policy.allowed_modes:
            raise ConfigError(f"{repo}: default_mode must be allowed")
        if policy.warm_capacity < 0:
            raise ConfigError(f"{repo}: warm_capacity must be non-negative")
        if policy.max_concurrent < 0:
            raise ConfigError(f"{repo}: max_concurrent must be non-negative")
        if 0 < policy.max_concurrent < policy.warm_capacity:
            raise ConfigError(
                f"{repo}: max_concurrent ({policy.max_concurrent}) must be >= "
                f"warm_capacity ({policy.warm_capacity}) when set"
            )
        if policy.warm_idle_capacity < -1:
            raise ConfigError(f"{repo}: warm_idle_capacity must be >= -1")
        if policy.warm_idle_capacity >= 0:
            if policy.warm_idle_capacity > policy.warm_capacity:
                raise ConfigError(
                    f"{repo}: warm_idle_capacity ({policy.warm_idle_capacity}) must be "
                    f"<= warm_capacity ({policy.warm_capacity})"
                )
            if policy.warm_idle_after_seconds <= 0:
                raise ConfigError(
                    f"{repo}: warm_idle_after_seconds must be > 0 when warm_idle_capacity is set"
                )
        if not policy.allowed_modes:
            raise ConfigError(f"{repo}: allowed_modes must not be empty")
        if not policy.allowed_events:
            raise ConfigError(f"{repo}: allowed_events must not be empty")
        if not policy.required_labels:
            raise ConfigError(f"{repo}: required_labels must not be empty")
        if not policy.profile.strip():
            raise ConfigError(f"{repo}: profile must not be empty")
    return repositories


def validate_repository_profiles(
    repositories: dict[str, RepositoryPolicy], profiles: dict[str, RunnerProfile]
) -> None:
    """Every repo's profile (and the pool-mode default) must be a defined profile.

    Catches a misconfigured `profiles:` map at load/save time instead of a
    KeyError deep inside job provisioning, silently retried 3x.
    """
    if DEFAULT_PROFILE_NAME not in profiles:
        raise ConfigError(
            f"profiles must define {DEFAULT_PROFILE_NAME!r} (used for pool-mode CTs, "
            "which are pre-staged before any repository claims them)"
        )
    for repo, policy in repositories.items():
        if policy.profile not in profiles:
            raise ConfigError(
                f"{repo}: profile {policy.profile!r} is not defined in profiles "
                f"(known: {sorted(profiles)})"
            )


def save_repository_policies(
    repositories: dict[str, Any],
    path: str | Path | None = None,
    installations: dict[str, Any] | None = None,
    capacity: dict[str, Any] | None = None,
) -> Settings:
    """Persist repository (+ optional installation/capacity) policies to override and reload."""
    cfg_path = config_path(path)
    base = _load_yaml(cfg_path)
    if "github" not in base or "installations" not in base["github"]:
        raise ConfigError("configuration is missing github.installations")
    current = load_settings(cfg_path)
    if installations is None:
        # Keep current effective installations (base + existing override).
        parsed_installations = current.github.installations
    else:
        parsed_installations = parse_github_installations(installations)
    parsed = parse_repository_policies(repositories, parsed_installations)
    if capacity is None:
        parsed_capacity = current.capacity
    else:
        parsed_capacity = parse_capacity_config(capacity, base=current.capacity)
    validate_capacity_budget(parsed_capacity, parsed)
    validate_repository_profiles(parsed, current.profiles)
    payload: dict[str, Any] = {
        "capacity": capacity_runtime_to_dict(parsed_capacity),
        "installations": {
            name: installation_to_dict(inst) for name, inst in parsed_installations.items()
        },
        "repositories": {
            name: repository_policy_to_dict(policy) for name, policy in parsed.items()
        },
    }
    db_path = str(base.get("database_path", "/var/lib/runner-orchestrator/state.db"))
    override = repositories_override_path(db_path)
    override.parent.mkdir(parents=True, exist_ok=True)
    tmp_path = override.with_name(override.name + ".tmp")
    serialized = yaml.safe_dump(payload, sort_keys=False, default_flow_style=False)
    try:
        tmp_path.write_text(serialized, encoding="utf-8")
        os.replace(tmp_path, override)
    except OSError as exc:
        if tmp_path.exists():
            tmp_path.unlink(missing_ok=True)
        raise ConfigError(f"failed to write repository override: {exc}") from exc
    return load_settings(cfg_path)


def load_settings(path: str | Path | None = None) -> Settings:
    config_path_value = config_path(path)
    raw = _load_yaml(config_path_value)
    # Runtime dashboard edits land in a writable override next to the SQLite DB.
    db_hint = str(raw.get("database_path", "/var/lib/runner-orchestrator/state.db"))
    override = repositories_override_path(db_hint)
    override_capacity: dict[str, Any] | None = None
    if override.is_file():
        override_raw = _load_yaml(override)
        if isinstance(override_raw, dict):
            if isinstance(override_raw.get("repositories"), dict):
                raw["repositories"] = override_raw["repositories"]
            if isinstance(override_raw.get("installations"), dict):
                raw.setdefault("github", {})
                if not isinstance(raw["github"], dict):
                    raw["github"] = {}
                raw["github"]["installations"] = override_raw["installations"]
            if isinstance(override_raw.get("capacity"), dict):
                override_capacity = override_raw["capacity"]
    try:
        gh_raw = raw["github"]
        pve_raw = raw["proxmox"]
        installations = parse_github_installations(gh_raw["installations"])
        github = GitHubConfig(
            app_id=_secret_override(str(gh_raw.get("app_id", "")), "GITHUB_APP_ID"),
            private_key_file=_secret_override(
                str(gh_raw["private_key_file"]), "GITHUB_PRIVATE_KEY_FILE"
            ),
            webhook_secret_file=_secret_override(
                str(gh_raw["webhook_secret_file"]), "GITHUB_WEBHOOK_SECRET_FILE"
            ),
            installations=installations,
            api_url=str(gh_raw.get("api_url", "https://api.github.com")).rstrip("/"),
            api_version=str(gh_raw.get("api_version", "2022-11-28")),
        )
        proxmox = ProxmoxConfig(
            endpoint=_secret_override(str(pve_raw["endpoint"]), "PROXMOX_VE_ENDPOINT").rstrip("/"),
            api_token_file=_secret_override(
                str(pve_raw["api_token_file"]), "PROXMOX_API_TOKEN_FILE"
            ),
            node=str(pve_raw.get("node", "pve")),
            template_vmid=int(pve_raw.get("template_vmid", 9101)),
            storage=str(pve_raw.get("storage", "local-lvm")),
            bridge=str(pve_raw.get("bridge", "vci")),
            gateway=str(pve_raw.get("gateway", "10.0.0.1")),
            vmid_start=int(pve_raw.get("vmid_start", 5000)),
            vmid_end=int(pve_raw.get("vmid_end", 5099)),
            ip_start=int(pve_raw.get("ip_start", 100)),
            ip_end=int(pve_raw.get("ip_end", 199)),
            verify_tls=bool(pve_raw.get("verify_tls", True)),
            egress_allow=parse_egress_rules(pve_raw.get("egress_allow")),
        )
    except (KeyError, TypeError, ValueError) as exc:
        raise ConfigError(f"invalid required configuration: {exc}") from exc

    broker_raw = raw.get("broker", {})
    broker = BrokerConfig(
        host=str(broker_raw.get("host", "")),
        user=str(broker_raw.get("user", "runner-broker")),
        key_file=_secret_override(
            str(broker_raw.get("key_file", "/run/credentials/runner_broker_ssh_key")),
            "RUNNER_BROKER_KEY_FILE",
        ),
        known_hosts_file=str(
            broker_raw.get("known_hosts_file", "/etc/runner-orchestrator/ssh_known_hosts")
        ),
    )
    cache = CacheConfig(**raw.get("cache", {}))
    base_capacity = parse_capacity_config(
        raw.get("capacity") if isinstance(raw.get("capacity"), dict) else {}
    )
    capacity = parse_capacity_config(override_capacity, base=base_capacity)
    if not github.installations:
        raise ConfigError("at least one GitHub installation is required")
    repositories = parse_repository_policies(raw.get("repositories", {}), github.installations)
    validate_capacity_budget(capacity, repositories)
    profiles = {
        name: RunnerProfile(**value)
        for name, value in raw.get("profiles", {DEFAULT_PROFILE_NAME: {}}).items()
    }
    validate_repository_profiles(repositories, profiles)
    if proxmox.vmid_start > proxmox.vmid_end or proxmox.ip_start > proxmox.ip_end:
        raise ConfigError("invalid Proxmox VMID or IP range")
    storage = parse_storage_config(raw.get("storage"), StorageConfig())
    api_raw = raw.get("api") or {}
    if not isinstance(api_raw, dict):
        raise ConfigError("api must be a mapping")
    api = ApiConfig(
        admin_token_file=_secret_override(
            str(api_raw.get("admin_token_file", "")),
            "RUNNER_ORCHESTRATOR_ADMIN_TOKEN_FILE",
        ),
        allow_unauthenticated_admin=bool(api_raw.get("allow_unauthenticated_admin", True)),
    )
    # Warning, not an error, by default. `load_settings()` runs at import via
    # `create_app()`, so a new hard failure here would brick a running
    # deployment on its next restart. Operators opt in with total_budget_gb.
    projected = projected_disk_demand_gb(capacity, repositories, profiles)
    if storage.total_budget_gb > 0 and projected > storage.total_budget_gb:
        raise ConfigError(
            f"projected disk demand {projected} GiB exceeds storage.total_budget_gb "
            f"({storage.total_budget_gb} GiB); lower max_active/warm_capacity/pool_size"
        )
    if storage.total_budget_gb == 0:
        LOGGER.info(
            "projected worst-case disk demand is %d GiB; set storage.total_budget_gb to enforce it",
            projected,
        )
    return Settings(
        github=github,
        proxmox=proxmox,
        broker=broker,
        cache=cache,
        capacity=capacity,
        repositories=repositories,
        profiles=profiles,
        storage=storage,
        api=api,
        database_path=str(raw.get("database_path", "/var/lib/runner-orchestrator/state.db")),
        log_level=str(raw.get("log_level", "INFO")),
        shadow_mode=bool(raw.get("shadow_mode", True)),
        drain=bool(raw.get("drain", False)),
    )

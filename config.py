from __future__ import annotations

import os
from dataclasses import dataclass
from pathlib import Path
from typing import Any

import yaml


class ConfigError(ValueError):
    pass


@dataclass(frozen=True)
class RepositoryPolicy:
    github_installation: str = ""
    default_mode: str = "ephemeral"
    warm_capacity: int = 0
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
    installations: dict[str, "GitHubInstallation"]
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
    node: str = "pve02"
    template_vmid: int = 9101
    storage: str = "local-lvm"
    bridge: str = "vci"
    gateway: str = "10.20.60.1"
    vmid_start: int = 5000
    vmid_end: int = 5099
    ip_start: int = 100
    ip_end: int = 199
    verify_tls: bool = True


@dataclass(frozen=True)
class BrokerConfig:
    host: str = "192.168.68.159"
    user: str = "runner-broker"
    key_file: str = "/run/credentials/runner_broker_ssh_key"
    known_hosts_file: str = "/etc/runner-orchestrator/ssh_known_hosts"


@dataclass(frozen=True)
class CacheConfig:
    root: str = "/srv/runner-cache"
    mount_path: str = "/cache"
    max_usage_percent: int = 80
    quota_gb: int = 40
    # Auto-GC orphan dirs (not on allowlist) older than N days during reconcile.
    # 0 disables automatic GC (manual purge/gc via API still works).
    gc_orphan_days: int = 7


@dataclass(frozen=True)
class CapacityConfig:
    max_active: int = 2
    pool_size: int = 2
    ready_timeout_seconds: int = 300
    max_job_seconds: int = 10800
    persistent_idle_seconds: int = 86400
    reconcile_seconds: int = 60


@dataclass(frozen=True)
class Settings:
    github: GitHubConfig
    proxmox: ProxmoxConfig
    broker: BrokerConfig
    cache: CacheConfig
    capacity: CapacityConfig
    repositories: dict[str, RepositoryPolicy]
    profiles: dict[str, RunnerProfile]
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
    return Path(path or os.environ.get("RUNNER_ORCHESTRATOR_CONFIG", "/etc/runner-orchestrator/config.yaml"))


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
            raise ConfigError(f"{repo}: GitHub installation scope must be organization or repository")
        if owner.lower() != installation.account.lower():
            raise ConfigError(f"{repo}: repository owner does not match GitHub installation account")
        if policy.default_mode not in policy.allowed_modes:
            raise ConfigError(f"{repo}: default_mode must be allowed")
        if policy.warm_capacity < 0:
            raise ConfigError(f"{repo}: warm_capacity must be non-negative")
        if not policy.allowed_modes:
            raise ConfigError(f"{repo}: allowed_modes must not be empty")
        if not policy.allowed_events:
            raise ConfigError(f"{repo}: allowed_events must not be empty")
        if not policy.required_labels:
            raise ConfigError(f"{repo}: required_labels must not be empty")
    return repositories


def save_repository_policies(
    repositories: dict[str, Any],
    path: str | Path | None = None,
    installations: dict[str, Any] | None = None,
) -> Settings:
    """Persist repository (+ optional installation) policies to override and reload."""
    cfg_path = config_path(path)
    base = _load_yaml(cfg_path)
    if "github" not in base or "installations" not in base["github"]:
        raise ConfigError("configuration is missing github.installations")
    if installations is None:
        # Keep current effective installations (base + existing override).
        current = load_settings(cfg_path)
        parsed_installations = current.github.installations
    else:
        parsed_installations = parse_github_installations(installations)
    parsed = parse_repository_policies(repositories, parsed_installations)
    payload = {
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
    try:
        gh_raw = raw["github"]
        pve_raw = raw["proxmox"]
        installations = parse_github_installations(gh_raw["installations"])
        github = GitHubConfig(
            app_id=_secret_override(str(gh_raw.get("app_id", "")), "GITHUB_APP_ID"),
            private_key_file=_secret_override(str(gh_raw["private_key_file"]), "GITHUB_PRIVATE_KEY_FILE"),
            webhook_secret_file=_secret_override(str(gh_raw["webhook_secret_file"]), "GITHUB_WEBHOOK_SECRET_FILE"),
            installations=installations,
            api_url=str(gh_raw.get("api_url", "https://api.github.com")).rstrip("/"),
            api_version=str(gh_raw.get("api_version", "2022-11-28")),
        )
        proxmox = ProxmoxConfig(
            endpoint=_secret_override(str(pve_raw["endpoint"]), "PROXMOX_VE_ENDPOINT").rstrip("/"),
            api_token_file=_secret_override(str(pve_raw["api_token_file"]), "PROXMOX_API_TOKEN_FILE"),
            node=str(pve_raw.get("node", "pve02")),
            template_vmid=int(pve_raw.get("template_vmid", 9101)),
            storage=str(pve_raw.get("storage", "local-lvm")),
            bridge=str(pve_raw.get("bridge", "vci")),
            gateway=str(pve_raw.get("gateway", "10.20.60.1")),
            vmid_start=int(pve_raw.get("vmid_start", 5000)),
            vmid_end=int(pve_raw.get("vmid_end", 5099)),
            ip_start=int(pve_raw.get("ip_start", 100)),
            ip_end=int(pve_raw.get("ip_end", 199)),
            verify_tls=bool(pve_raw.get("verify_tls", True)),
        )
    except (KeyError, TypeError, ValueError) as exc:
        raise ConfigError(f"invalid required configuration: {exc}") from exc

    broker_raw = raw.get("broker", {})
    broker = BrokerConfig(
        host=str(broker_raw.get("host", "192.168.68.159")),
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
    capacity = CapacityConfig(**raw.get("capacity", {}))
    if not github.installations:
        raise ConfigError("at least one GitHub installation is required")
    repositories = parse_repository_policies(raw.get("repositories", {}), github.installations)
    profiles = {name: RunnerProfile(**value) for name, value in raw.get("profiles", {"crossbuild-small": {}}).items()}
    if proxmox.vmid_start > proxmox.vmid_end or proxmox.ip_start > proxmox.ip_end:
        raise ConfigError("invalid Proxmox VMID or IP range")
    return Settings(
        github=github,
        proxmox=proxmox,
        broker=broker,
        cache=cache,
        capacity=capacity,
        repositories=repositories,
        profiles=profiles,
        database_path=str(raw.get("database_path", "/var/lib/runner-orchestrator/state.db")),
        log_level=str(raw.get("log_level", "INFO")),
        shadow_mode=bool(raw.get("shadow_mode", True)),
        drain=bool(raw.get("drain", False)),
    )

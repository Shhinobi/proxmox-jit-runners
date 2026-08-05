from __future__ import annotations

from orchestrator.config import (
    BrokerConfig,
    CacheConfig,
    CapacityConfig,
    GitHubConfig,
    GitHubInstallation,
    ProxmoxConfig,
    RepositoryPolicy,
    RunnerProfile,
    Settings,
)


def settings(database_path: str = ":memory:", shadow_mode: bool = False) -> Settings:
    return Settings(
        github=GitHubConfig(
            app_id="1",
            private_key_file="/unused/key",
            webhook_secret_file="/unused/webhook",
            installations={
                "personal": GitHubInstallation(
                    account="MetalDevOps",
                    installation_id=146433491,
                    scope="repository",
                )
            },
        ),
        proxmox=ProxmoxConfig(endpoint="https://pve.invalid", api_token_file="/unused/token"),
        broker=BrokerConfig(),
        cache=CacheConfig(),
        capacity=CapacityConfig(),
        repositories={
            "MetalDevops/NinjaCrawler": RepositoryPolicy(github_installation="personal"),
        },

        profiles={"crossbuild-small": RunnerProfile()},
        database_path=database_path,
        shadow_mode=shadow_mode,
    )

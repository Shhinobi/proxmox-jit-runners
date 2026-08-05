from __future__ import annotations

from dataclasses import dataclass
from typing import Any

from .config import RepositoryPolicy, Settings

MODES = {"ephemeral", "pool", "persistent"}


@dataclass(frozen=True)
class Decision:
    accepted: bool
    reason: str
    repository: str = ""
    mode: str = ""
    labels: tuple[str, ...] = ()


def repository_policy(settings: Settings, repository: str) -> tuple[str, RepositoryPolicy] | None:
    """Resolve GitHub repository names using GitHub's case-insensitive semantics."""
    wanted = repository.casefold()
    return next(
        (
            (configured_name, policy)
            for configured_name, policy in settings.repositories.items()
            if configured_name.casefold() == wanted
        ),
        None,
    )


def decide(settings: Settings, payload: dict[str, Any], run_event: str | None = None) -> Decision:
    payload_repository = str(payload.get("repository", {}).get("full_name", ""))
    resolved = repository_policy(settings, payload_repository)
    if resolved is None:
        return Decision(False, "repository_not_allowed", repository=payload_repository)
    repository, policy = resolved
    if payload.get("action") != "queued":
        return Decision(False, "action_not_queued", repository=repository)

    job = payload.get("workflow_job") or {}
    labels = tuple(str(label).lower() for label in job.get("labels") or [])
    missing = {label.lower() for label in policy.required_labels} - set(labels)
    if missing:
        return Decision(False, "required_labels_missing", repository=repository, labels=labels)

    requested_modes = [label.removeprefix("mode-") for label in labels if label.startswith("mode-")]
    requested_modes = [mode for mode in requested_modes if mode in MODES]
    if len(set(requested_modes)) > 1:
        return Decision(False, "conflicting_mode_labels", repository=repository, labels=labels)
    mode = requested_modes[0] if requested_modes else policy.default_mode
    if mode not in policy.allowed_modes:
        return Decision(False, "mode_not_allowed", repository=repository, mode=mode, labels=labels)
    if run_event is not None and run_event not in policy.allowed_events:
        return Decision(False, "event_not_allowed", repository=repository, mode=mode, labels=labels)
    return Decision(True, "accepted", repository=repository, mode=mode, labels=labels)


def cache_slug(repository: str) -> str:
    parts = repository.split("/", 1)
    if len(parts) != 2 or not all(
        part.replace("-", "").replace("_", "").isalnum() for part in parts
    ):
        raise ValueError("invalid repository name")
    return "/".join(parts)

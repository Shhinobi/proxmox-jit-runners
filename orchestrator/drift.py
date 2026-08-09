"""Compare what the database believes against what the hypervisor actually has.

During the incident that motivated this work, the only way to clear rows
referencing containers that no longer existed was a hand-written
`DELETE FROM instances` against the production SQLite file, over SSH, at the
end of a long night. That is a bad tool to
reach for under pressure and an easy one to get wrong, so it is now a
first-class, dry-run-by-default operation.

Safety posture, deliberately conservative:

- Detection is read-only and always available.
- Application requires an explicit list of vmids that must match the current
  scan exactly. If anything changed in between, the request is refused rather
  than reinterpreted.
- Only containers that are BOTH inside the configured VMID range AND tagged
  `managed-orchestrator` are ever candidates for destruction. Both conditions,
  always: this runs on a hypervisor shared with nine production services.
"""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any

MANAGED_TAG = "managed-orchestrator"


@dataclass
class DriftReport:
    checked_at: int
    node: str
    rows_without_container: list[dict[str, Any]] = field(default_factory=list)
    containers_without_row: list[dict[str, Any]] = field(default_factory=list)
    stuck_deleting: list[dict[str, Any]] = field(default_factory=list)
    scan_incomplete: bool = False
    """True when the hypervisor view could not be read; nothing may be applied."""

    @property
    def total(self) -> int:
        return (
            len(self.rows_without_container)
            + len(self.containers_without_row)
            + len(self.stuck_deleting)
        )

    def selectable_vmids(self) -> set[int]:
        return {
            int(item["vmid"])
            for group in (
                self.rows_without_container,
                self.containers_without_row,
                self.stuck_deleting,
            )
            for item in group
        }

    def to_dict(self) -> dict[str, Any]:
        return {
            "checked_at": self.checked_at,
            "node": self.node,
            "scan_incomplete": self.scan_incomplete,
            "total": self.total,
            "rows_without_container": self.rows_without_container,
            "containers_without_row": self.containers_without_row,
            "stuck_deleting": self.stuck_deleting,
        }


def _tags_of(container: dict[str, Any]) -> set[str]:
    raw = str(container.get("tags") or "")
    return {tag.strip().lower() for tag in raw.replace(",", ";").split(";") if tag.strip()}


def is_managed(container: dict[str, Any], *, vmid_start: int, vmid_end: int) -> bool:
    """Both conditions, never either: in range AND tagged as ours."""
    vmid = int(container.get("vmid") or 0)
    if not vmid_start <= vmid <= vmid_end:
        return False
    return MANAGED_TAG in _tags_of(container)

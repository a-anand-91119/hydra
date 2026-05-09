from __future__ import annotations

from dataclasses import dataclass, field
from enum import Enum
from typing import List, Optional


class Level(str, Enum):
    OK = "ok"
    WARN = "warn"
    ERROR = "error"


@dataclass
class Finding:
    """A single observation from a `doctor` check."""

    section: str
    level: Level
    message: str
    fix_id: Optional[str] = None  # if set, doctor --fix can resolve this finding
    details: Optional[str] = None  # multi-line context surfaced under --verbose


@dataclass
class Report:
    findings: List[Finding] = field(default_factory=list)

    def add(self, finding: Finding) -> None:
        self.findings.append(finding)

    def by_level(self, level: Level) -> List[Finding]:
        return [f for f in self.findings if f.level is level]

    @property
    def warnings(self) -> List[Finding]:
        return self.by_level(Level.WARN)

    @property
    def errors(self) -> List[Finding]:
        return self.by_level(Level.ERROR)

    @property
    def fixable(self) -> List[Finding]:
        return [f for f in self.findings if f.fix_id is not None]

    @property
    def is_clean(self) -> bool:
        return not self.warnings and not self.errors


__all__ = ["Finding", "Level", "Report"]

"""Serializable observation contract."""

from dataclasses import asdict, dataclass
from datetime import datetime, timezone
from typing import Any


VALID_STATUSES = {"PASS", "WARN", "FAIL"}
VALID_SEVERITIES = {"INFO", "WARNING", "CRITICAL"}


@dataclass(frozen=True)
class Observation:
    pipeline_name: str
    run_id: str
    table_name: str
    metric_name: str
    metric_value: Any
    expected_min: Any = None
    expected_max: Any = None
    status: str = "PASS"
    severity: str = "INFO"
    details: str = ""
    observation_timestamp: str = ""

    def __post_init__(self) -> None:
        if self.status not in VALID_STATUSES:
            raise ValueError(f"Unsupported observation status: {self.status}")
        if self.severity not in VALID_SEVERITIES:
            raise ValueError(f"Unsupported observation severity: {self.severity}")
        if not self.observation_timestamp:
            object.__setattr__(
                self,
                "observation_timestamp",
                datetime.now(timezone.utc).isoformat(),
            )

    def to_dict(self) -> dict[str, Any]:
        return asdict(self)

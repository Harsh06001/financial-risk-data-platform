"""Credential-free configuration validation for the live GCP demo preflight."""

import argparse
import os
from dataclasses import dataclass


TRUE_VALUES = {"1", "true", "yes"}


def env_bool(name: str, default: bool = False) -> bool:
    return os.environ.get(name, str(default)).strip().lower() in TRUE_VALUES


@dataclass(frozen=True)
class CostConfig:
    project_id: str
    region: str = "us-central1"
    demo_event_count: int = 1000
    max_demo_minutes: int = 15
    num_workers: int = 1
    max_workers: int = 1
    machine_type: str = "n1-standard-1"
    acknowledged: bool = False

    @classmethod
    def from_environment(cls, acknowledged: bool = False) -> "CostConfig":
        return cls(
            project_id=os.environ.get("GCP_PROJECT_ID", "").strip(),
            region=os.environ.get("GCP_STREAMING_REGION", "us-central1").strip(),
            demo_event_count=int(os.environ.get("GCP_STREAMING_DEMO_EVENT_COUNT", "1000")),
            max_demo_minutes=int(os.environ.get("GCP_STREAMING_MAX_DEMO_MINUTES", "15")),
            num_workers=int(os.environ.get("GCP_STREAMING_NUM_WORKERS", "1")),
            max_workers=int(os.environ.get("GCP_STREAMING_MAX_WORKERS", "1")),
            machine_type=os.environ.get(
                "GCP_STREAMING_MACHINE_TYPE", "n1-standard-1"
            ).strip(),
            acknowledged=acknowledged or env_bool("ACKNOWLEDGE_GCP_COST_RISK"),
        )

    def errors(self) -> list[str]:
        errors = []
        if not self.project_id:
            errors.append("GCP_PROJECT_ID is required")
        if not self.region:
            errors.append("GCP_STREAMING_REGION is required")
        if not 1 <= self.demo_event_count <= 10_000:
            errors.append("GCP_STREAMING_DEMO_EVENT_COUNT must be between 1 and 10000")
        if not 1 <= self.max_demo_minutes <= 15:
            errors.append("GCP_STREAMING_MAX_DEMO_MINUTES must be between 1 and 15")
        if self.num_workers != 1:
            errors.append(
                "GCP_STREAMING_NUM_WORKERS must configure exactly one worker for the guarded demo"
            )
        if not 1 <= self.max_workers <= 2:
            errors.append("GCP_STREAMING_MAX_WORKERS must be 1 or 2")
        if self.num_workers > self.max_workers:
            errors.append("worker count cannot exceed maximum worker count")
        if not self.machine_type:
            errors.append("GCP_STREAMING_MACHINE_TYPE is required")
        if not self.acknowledged:
            errors.append(
                "cost risk acknowledgement is required: pass --acknowledge-cost-risk "
                "or set ACKNOWLEDGE_GCP_COST_RISK=true"
            )
        return errors


def parse_arguments() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Validate guarded GCP demo settings.")
    parser.add_argument("--acknowledge-cost-risk", action="store_true")
    return parser.parse_args()


def main() -> None:
    args = parse_arguments()
    config = CostConfig.from_environment(args.acknowledge_cost_risk)
    print("GCP STREAMING COST PREFLIGHT")
    for label, value in (
        ("project_id", config.project_id or "<unset>"),
        ("region", config.region),
        ("demo_event_count", config.demo_event_count),
        ("max_demo_minutes", config.max_demo_minutes),
        ("num_workers", config.num_workers),
        ("max_workers", config.max_workers),
        ("machine_type", config.machine_type),
        ("cost_risk_acknowledged", config.acknowledged),
    ):
        print(f"{label}={value}")
    errors = config.errors()
    if errors:
        for error in errors:
            print(f"ERROR: {error}")
        raise SystemExit(2)
    print("PREFLIGHT CONFIG PASSED (no GCP resources were created)")


if __name__ == "__main__":
    main()

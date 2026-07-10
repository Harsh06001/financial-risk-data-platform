import argparse
import csv
import os
import platform
import subprocess
import sys
import time
from pathlib import Path

import pyspark


PROJECT_ROOT = Path(__file__).resolve().parents[1]
RESULTS_PATH = PROJECT_ROOT / "benchmarks" / "results" / "scale_test_results.csv"
REPORT_PATH = PROJECT_ROOT / "benchmarks" / "results" / "scale_test_report.md"
DATA_ROOT = PROJECT_ROOT / "data" / "scale-tests"
DEFAULT_SIZES = [1_000_000, 5_000_000, 10_000_000]
FIELDS = [
    "requested_rows",
    "actual_input_rows",
    "output_rows",
    "distinct_event_dates",
    "processing_runtime_seconds",
    "rows_per_second",
    "parquet_file_count",
    "output_bytes",
    "reconciliation",
    "status",
    "failure",
    "seed",
    "write_strategy",
    "spark_master",
    "spark_sql_session_timezone",
    "spark_version",
    "python_version",
    "java_version",
    "cpu_count",
    "platform",
]


def run_command(
    command: list[str],
    timeout_seconds: int,
    environment: dict[str, str] | None = None,
) -> subprocess.CompletedProcess[str]:
    return subprocess.run(
        command,
        cwd=PROJECT_ROOT,
        capture_output=True,
        text=True,
        timeout=timeout_seconds,
        env=environment,
    )


def count_csv_rows(path: Path) -> int:
    with path.open(encoding="utf-8") as input_file:
        return max(sum(1 for _ in input_file) - 1, 0)


def count_parquet_files(path: Path) -> int:
    return sum(1 for _ in path.rglob("*.parquet"))


def output_bytes(path: Path) -> int:
    return sum(item.stat().st_size for item in path.rglob("*") if item.is_file())


def java_version() -> str:
    completed = subprocess.run(
        ["java", "-version"],
        capture_output=True,
        text=True,
    )
    output = (completed.stderr or completed.stdout).splitlines()
    return output[0] if output else "unavailable"


def extract_metric(output: str, label: str) -> int:
    for line in output.splitlines():
        if line.startswith(label):
            return int(line.split(":", 1)[1].strip().replace(",", ""))
    raise RuntimeError(f"Validation output did not contain {label!r}")


def base_result(size: int, seed: int) -> dict[str, object]:
    return {
        "requested_rows": size,
        "actual_input_rows": "",
        "output_rows": "",
        "distinct_event_dates": "",
        "processing_runtime_seconds": "",
        "rows_per_second": "",
        "parquet_file_count": "",
        "output_bytes": "",
        "reconciliation": "NOT_RUN",
        "status": "FAILED",
        "failure": "",
        "seed": seed,
        "write_strategy": "repartitioned",
        "spark_master": "local[*]",
        "spark_sql_session_timezone": "UTC",
        "spark_version": pyspark.__version__,
        "python_version": platform.python_version(),
        "java_version": java_version(),
        "cpu_count": os.cpu_count() or "unknown",
        "platform": platform.platform(),
    }


def run_scale(size: int, seed: int, timeout_seconds: int) -> dict[str, object]:
    result = base_result(size, seed)
    scale_root = DATA_ROOT / str(size)
    raw_dir = scale_root / "raw"
    output_dir = scale_root / "processed"
    raw_path = raw_dir / f"transactions_{size}_seed_{seed}.csv"
    spark_environment = os.environ.copy()
    spark_environment["SPARK_SQL_SESSION_TIMEZONE"] = "UTC"

    try:
        generation = run_command(
            [
                sys.executable,
                "data-generator/generate_transactions.py",
                "--rows",
                str(size),
                "--seed",
                str(seed),
                "--event-dates",
                "31",
                "--output-dir",
                str(raw_dir),
                "--output-name",
                raw_path.name,
            ],
            timeout_seconds,
        )
        if generation.returncode != 0:
            raise RuntimeError(f"generator failed: {generation.stderr or generation.stdout}")

        actual_input_rows = count_csv_rows(raw_path)
        result["actual_input_rows"] = actual_input_rows
        start = time.perf_counter()
        processing = run_command(
            [
                sys.executable,
                "batch-processing/process_transactions.py",
                "--input",
                str(raw_path),
                "--output",
                str(output_dir),
                "--write-strategy",
                "repartitioned",
            ],
            timeout_seconds,
            spark_environment,
        )
        duration = time.perf_counter() - start
        result["processing_runtime_seconds"] = round(duration, 4)
        if processing.returncode != 0:
            raise RuntimeError(f"processing failed: {processing.stdout[-4000:]}")

        validation = run_command(
            [
                sys.executable,
                "batch-processing/validate_processed_transactions.py",
                "--input",
                str(output_dir),
                "--expected-rows",
                str(actual_input_rows),
                "--expected-event-dates",
                "31",
            ],
            timeout_seconds,
        )
        if validation.returncode != 0:
            raise RuntimeError(f"validation failed: {validation.stdout[-4000:]}")

        output_rows = extract_metric(validation.stdout, "Total rows")
        distinct_dates = extract_metric(validation.stdout, "Distinct event dates")
        result.update(
            {
                "output_rows": output_rows,
                "distinct_event_dates": distinct_dates,
                "rows_per_second": round(output_rows / duration, 2),
                "parquet_file_count": count_parquet_files(output_dir),
                "output_bytes": output_bytes(output_dir),
                "reconciliation": (
                    "PASSED" if output_rows == actual_input_rows else "FAILED"
                ),
                "status": "PASSED",
            }
        )
    except subprocess.TimeoutExpired as exc:
        result["failure"] = f"timeout after {exc.timeout} seconds"
    except Exception as exc:  # evidence must retain exact resource/tool failure
        result["failure"] = str(exc).replace("\n", " ")

    return result


def write_results(results: list[dict[str, object]]) -> None:
    RESULTS_PATH.parent.mkdir(parents=True, exist_ok=True)
    with RESULTS_PATH.open("w", newline="", encoding="utf-8") as csv_file:
        writer = csv.DictWriter(csv_file, fieldnames=FIELDS)
        writer.writeheader()
        writer.writerows(results)


def write_report(results: list[dict[str, object]]) -> None:
    passed = [result for result in results if result["status"] == "PASSED"]
    highest = max((int(result["requested_rows"]) for result in passed), default=None)
    lines = [
        "# Local Synthetic Scale Test Report",
        "",
        "These runs are local synthetic scale tests, not distributed-cluster or production traffic claims.",
        "They are separate from the tracked 100,350-row Spark write-strategy benchmark.",
        "",
        "| Requested | Input | Output | Dates | Runtime (s) | Rows/s | Files | Bytes | Status |",
        "|---:|---:|---:|---:|---:|---:|---:|---:|---|",
    ]
    for result in results:
        lines.append(
            "| {requested_rows} | {actual_input_rows} | {output_rows} | "
            "{distinct_event_dates} | {processing_runtime_seconds} | "
            "{rows_per_second} | {parquet_file_count} | {output_bytes} | "
            "{status} |".format(**result)
        )
    lines.extend(
        [
            "",
            f"Highest successfully completed scale: {highest or 'none'} rows.",
            "",
            "## Environment",
            "",
        ]
    )
    if results:
        first = results[0]
        for key in (
            "spark_master",
            "spark_sql_session_timezone",
            "spark_version",
            "python_version",
            "java_version",
            "cpu_count",
            "platform",
            "write_strategy",
            "seed",
        ):
            lines.append(f"- {key}: {first[key]}")
    failures = [result for result in results if result["status"] != "PASSED"]
    if failures:
        lines.extend(["", "## Failed Attempts", ""])
        for result in failures:
            lines.append(
                f"- {result['requested_rows']} rows: {result['failure']}"
            )
    REPORT_PATH.write_text("\n".join(lines) + "\n", encoding="utf-8")


def parse_arguments() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Run progressive local synthetic Spark scale tests."
    )
    parser.add_argument("--sizes", nargs="+", type=int, default=DEFAULT_SIZES)
    parser.add_argument("--seed", type=int, default=202611)
    parser.add_argument("--timeout-seconds", type=int, default=1800)
    return parser.parse_args()


def main() -> None:
    args = parse_arguments()
    results: list[dict[str, object]] = []
    for size in args.sizes:
        print(f"Starting local synthetic scale test: {size:,} rows")
        result = run_scale(size, args.seed, args.timeout_seconds)
        results.append(result)
        write_results(results)
        write_report(results)
        print(result)
    print(f"Results: {RESULTS_PATH}")
    print(f"Report: {REPORT_PATH}")


if __name__ == "__main__":
    main()

import csv
import statistics
import subprocess
import sys
import time
from pathlib import Path


PROJECT_ROOT = Path(__file__).resolve().parents[1]

PROCESSING_SCRIPT = (
    PROJECT_ROOT
    / "batch-processing"
    / "process_transactions.py"
)

RESULTS_DIR = PROJECT_ROOT / "benchmarks" / "results"

OUTPUT_ROOT = (
    PROJECT_ROOT
    / "data"
    / "benchmark-output"
)

ROUND_ORDERS = [
    ("baseline", "repartitioned"),
    ("repartitioned", "baseline"),
    ("baseline", "repartitioned"),
    ("repartitioned", "baseline"),
]


def count_parquet_files(output_path: Path) -> int:
    return sum(
        1
        for _ in output_path.rglob("*.parquet")
    )


def calculate_output_bytes(output_path: Path) -> int:
    return sum(
        path.stat().st_size
        for path in output_path.rglob("*")
        if path.is_file()
    )


def run_benchmark(
    strategy: str,
    round_number: int,
    order_position: int,
) -> dict:
    output_path = OUTPUT_ROOT / strategy

    command = [
        sys.executable,
        str(PROCESSING_SCRIPT),
        "--write-strategy",
        strategy,
        "--output",
        str(output_path),
    ]

    print(
        f"Round {round_number} | "
        f"Position {order_position} | "
        f"Strategy: {strategy}"
    )

    start_time = time.perf_counter()

    result = subprocess.run(
        command,
        cwd=PROJECT_ROOT,
        stdout=subprocess.PIPE,
        stderr=subprocess.STDOUT,
        text=True,
    )

    duration_seconds = time.perf_counter() - start_time

    if result.returncode != 0:
        print(result.stdout)

        raise RuntimeError(
            f"Benchmark failed for strategy: {strategy}"
        )

    if "Row count check: PASSED" not in result.stdout:
        raise RuntimeError(
            f"Output verification was not confirmed for {strategy}."
        )

    parquet_files = count_parquet_files(output_path)

    output_bytes = calculate_output_bytes(output_path)

    print(
        f"Completed in {duration_seconds:.2f}s | "
        f"Files: {parquet_files} | "
        f"Bytes: {output_bytes:,}"
    )

    print()

    return {
        "round": round_number,
        "order_position": order_position,
        "strategy": strategy,
        "duration_seconds": round(
            duration_seconds,
            4,
        ),
        "parquet_files": parquet_files,
        "output_bytes": output_bytes,
        "verification": "PASSED",
    }


def print_summary(results: list[dict]) -> None:
    print()
    print("BENCHMARK SUMMARY")
    print("=" * 70)

    for strategy in ("baseline", "repartitioned"):
        durations = [
            result["duration_seconds"]
            for result in results
            if result["strategy"] == strategy
        ]

        strategy_results = [
            result
            for result in results
            if result["strategy"] == strategy
        ]

        print()
        print(f"Strategy: {strategy}")
        print(f"Runs: {len(durations)}")

        print(
            f"Mean runtime: "
            f"{statistics.mean(durations):.2f}s"
        )

        print(
            f"Median runtime: "
            f"{statistics.median(durations):.2f}s"
        )

        print(
            f"Minimum runtime: "
            f"{min(durations):.2f}s"
        )

        print(
            f"Maximum runtime: "
            f"{max(durations):.2f}s"
        )

        print(
            f"Parquet files: "
            f"{strategy_results[-1]['parquet_files']}"
        )

        print(
            f"Output bytes: "
            f"{strategy_results[-1]['output_bytes']:,}"
        )


def write_results(results: list[dict]) -> Path:
    RESULTS_DIR.mkdir(
        parents=True,
        exist_ok=True,
    )

    output_path = (
        RESULTS_DIR
        / "batch_100k_strategy_comparison.csv"
    )

    fieldnames = [
        "round",
        "order_position",
        "strategy",
        "duration_seconds",
        "parquet_files",
        "output_bytes",
        "verification",
    ]

    with output_path.open(
        "w",
        newline="",
        encoding="utf-8",
    ) as csv_file:
        writer = csv.DictWriter(
            csv_file,
            fieldnames=fieldnames,
        )

        writer.writeheader()
        writer.writerows(results)

    return output_path


def main() -> None:
    results: list[dict] = []

    for round_number, strategies in enumerate(
        ROUND_ORDERS,
        start=1,
    ):
        for order_position, strategy in enumerate(
            strategies,
            start=1,
        ):
            result = run_benchmark(
                strategy=strategy,
                round_number=round_number,
                order_position=order_position,
            )

            results.append(result)

    results_path = write_results(results)

    print_summary(results)

    print()
    print(f"Results saved to: {results_path}")


if __name__ == "__main__":
    main()
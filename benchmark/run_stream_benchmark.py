"""Benchmark the complete local speed-layer pipeline."""

from __future__ import annotations

import argparse
import json
from pathlib import Path
from statistics import mean
from time import perf_counter, perf_counter_ns

from producer.log_parser import parse_log_line
from producer.replay_logs import build_kinesis_record
from speed.stream_consumer import process_kinesis_record
from speed.window_analytics import SlidingWindowAnalytics


def calculate_percentile(
    values: list[float],
    percentile: float,
) -> float | None:
    """Calculate a percentile using linear interpolation."""

    if not values:
        return None

    ordered = sorted(values)

    if len(ordered) == 1:
        return ordered[0]

    position = (len(ordered) - 1) * percentile
    lower_index = int(position)
    upper_index = min(
        lower_index + 1,
        len(ordered) - 1,
    )

    fraction = position - lower_index

    return (
        ordered[lower_index]
        + (
            ordered[upper_index]
            - ordered[lower_index]
        )
        * fraction
    )


def run_benchmark(
    input_path: Path,
    output_path: Path,
    window_seconds: int,
    latency_sample_every: int,
    progress_every: int,
    limit: int | None,
) -> dict:
    """
    Process raw Nginx lines through the complete local streaming path.

    Pipeline:

        raw log line
        -> parser
        -> Kinesis-style JSON record
        -> consumer
        -> sliding-window analytics
    """

    if not input_path.is_file():
        raise FileNotFoundError(
            f"Dataset not found: {input_path}"
        )

    if latency_sample_every <= 0:
        raise ValueError(
            "latency_sample_every must be greater than zero."
        )

    analytics = SlidingWindowAnalytics(
        window_seconds=window_seconds,
    )

    total_lines = 0
    valid_records = 0
    invalid_lines = 0
    processed_records = 0
    failed_records = 0

    # Only selected records are timed to avoid excessive memory use
    # and reduce measurement overhead on the full dataset.
    latency_values_ms: list[float] = []

    benchmark_start = perf_counter()

    with input_path.open(
        mode="r",
        encoding="utf-8",
        errors="replace",
    ) as log_file:

        for line in log_file:
            total_lines += 1

            # Stop early only when a development limit was supplied.
            if limit is not None and total_lines > limit:
                total_lines -= 1
                break

            # Sample one complete line-to-analytics operation.
            sample_latency = (
                total_lines % latency_sample_every == 0
            )

            if sample_latency:
                record_start_ns = perf_counter_ns()

            event = parse_log_line(line)

            if event is None:
                invalid_lines += 1
                continue

            valid_records += 1

            # Build the same JSON payload structure used for Kinesis.
            kinesis_record = build_kinesis_record(event)

            processed = process_kinesis_record(
                record=kinesis_record,
                analytics=analytics,
            )

            if processed:
                processed_records += 1

                if sample_latency:
                    record_end_ns = perf_counter_ns()

                    latency_ms = (
                        record_end_ns - record_start_ns
                    ) / 1_000_000

                    latency_values_ms.append(
                        latency_ms
                    )
            else:
                failed_records += 1

            # Print progress without producing one message per line.
            if (
                progress_every > 0
                and total_lines % progress_every == 0
            ):
                elapsed = (
                    perf_counter() - benchmark_start
                )

                current_throughput = (
                    total_lines / elapsed
                    if elapsed > 0
                    else 0.0
                )

                print(
                    f"Processed {total_lines:,} lines | "
                    f"{current_throughput:,.2f} lines/s"
                )

    runtime_seconds = (
        perf_counter() - benchmark_start
    )

    throughput_lines = (
        total_lines / runtime_seconds
        if runtime_seconds > 0
        else 0.0
    )

    throughput_records = (
        processed_records / runtime_seconds
        if runtime_seconds > 0
        else 0.0
    )

    snapshot = analytics.snapshot()

    results = {
        "benchmark_type": (
            "full_dataset"
            if limit is None
            else "development_sample"
        ),
        "dataset_path": str(
            input_path.resolve()
        ),
        "dataset_size_bytes": input_path.stat().st_size,
        "line_limit": limit,
        "window_seconds": window_seconds,
        "total_lines": total_lines,
        "valid_records": valid_records,
        "invalid_lines": invalid_lines,
        "processed_records": processed_records,
        "failed_records": failed_records,
        "runtime_seconds": runtime_seconds,
        "throughput_lines_per_second": throughput_lines,
        "throughput_records_per_second": throughput_records,
        "latency_definition": (
            "Local raw-line-to-window processing time. "
            "It excludes AWS network and Kinesis service latency."
        ),
        "latency_sample_every": latency_sample_every,
        "latency_samples": len(
            latency_values_ms
        ),
        "latency_min_ms": (
            min(latency_values_ms)
            if latency_values_ms
            else None
        ),
        "latency_mean_ms": (
            mean(latency_values_ms)
            if latency_values_ms
            else None
        ),
        "latency_p50_ms": calculate_percentile(
            latency_values_ms,
            0.50,
        ),
        "latency_p95_ms": calculate_percentile(
            latency_values_ms,
            0.95,
        ),
        "latency_max_ms": (
            max(latency_values_ms)
            if latency_values_ms
            else None
        ),
        "final_window_event_count": snapshot[
            "window_event_count"
        ],
        "distinct_endpoints_in_final_window": len(
            snapshot["requests_per_endpoint"]
        ),
    }

    output_path.parent.mkdir(
        parents=True,
        exist_ok=True,
    )

    with output_path.open(
        mode="w",
        encoding="utf-8",
    ) as output_file:
        json.dump(
            results,
            output_file,
            indent=2,
        )

    return results


def parse_arguments() -> argparse.Namespace:
    """Read benchmark options from the command line."""

    parser = argparse.ArgumentParser(
        description=(
            "Benchmark the complete local streaming pipeline."
        )
    )

    parser.add_argument(
        "--input",
        type=Path,
        required=True,
        help="Path to access.log.",
    )

    parser.add_argument(
        "--output",
        type=Path,
        default=Path(
            "results/stream_benchmark_results.json"
        ),
        help="Path for the JSON benchmark results.",
    )

    parser.add_argument(
        "--window-seconds",
        type=int,
        default=60,
        help="Sliding-window duration in seconds.",
    )

    parser.add_argument(
        "--latency-sample-every",
        type=int,
        default=1000,
        help=(
            "Measure latency for every Nth input line."
        ),
    )

    parser.add_argument(
        "--progress-every",
        type=int,
        default=500000,
        help=(
            "Print progress after every N lines."
        ),
    )

    parser.add_argument(
        "--limit",
        type=int,
        default=None,
        help=(
            "Optional development-only line limit."
        ),
    )

    return parser.parse_args()


def main() -> None:
    """Run the benchmark and display the final results."""

    arguments = parse_arguments()

    results = run_benchmark(
        input_path=arguments.input,
        output_path=arguments.output,
        window_seconds=arguments.window_seconds,
        latency_sample_every=(
            arguments.latency_sample_every
        ),
        progress_every=arguments.progress_every,
        limit=arguments.limit,
    )

    print("\n--- Streaming benchmark results ---")

    for name, value in results.items():
        print(f"{name}: {value}")


if __name__ == "__main__":
    main()

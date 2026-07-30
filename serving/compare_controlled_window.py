"""Compare batch and streaming analytics for one controlled input file."""

from __future__ import annotations

import argparse
import json
import math
from pathlib import Path
from typing import Any

from batch.batch_job import (
    build_spark_session,
    compute_batch_metrics,
    read_raw_data_with_quality,
)
from producer.log_parser import parse_log_line
from speed.window_analytics import SlidingWindowAnalytics


COMPARABLE_METRICS = (
    "total_valid_records",
    "total_response_bytes",
    "requests_per_endpoint",
    "error_rates",
    "traffic_by_hour",
    "status_code_distribution",
    "response_byte_totals",
)


def collect_batch_metrics(results: dict[str, Any]) -> dict[str, Any]:
    """Convert Spark DataFrames into normal Python dictionaries."""

    summary = results["summary"].first()

    requests_per_endpoint = {
        row["endpoint"]: int(row["total_requests"])
        for row in results["requests_per_endpoint"].collect()
    }

    error_rates = {
        row["endpoint"]: {
            "total_requests": int(row["total_requests"]),
            "error_count": int(row["error_count"] or 0),
            "error_rate": float(row["error_rate"] or 0.0),
        }
        for row in results["error_rates"].collect()
    }

    traffic_by_hour = {
        int(row["hour"]): int(row["request_count"])
        for row in results["traffic_by_hour"].collect()
    }

    status_code_distribution = {
        int(row["status_code"]): int(row["request_count"])
        for row in results["status_code_distribution"].collect()
    }

    response_byte_totals = {
        row["endpoint"]: int(row["total_response_bytes"] or 0)
        for row in results["response_byte_totals"].collect()
    }

    return {
        "total_valid_records": int(
            summary["total_valid_records"]
        ),
        "total_response_bytes": int(
            summary["total_response_bytes"] or 0
        ),
        "requests_per_endpoint": requests_per_endpoint,
        "error_rates": error_rates,
        "traffic_by_hour": traffic_by_hour,
        "status_code_distribution": status_code_distribution,
        "response_byte_totals": response_byte_totals,
    }


def calculate_stream_metrics(
    input_path: Path,
    window_seconds: int,
) -> tuple[dict[str, Any], dict[str, int]]:
    """Parse the input and calculate streaming window metrics."""

    analytics = SlidingWindowAnalytics(
        window_seconds=window_seconds,
    )

    total_raw_lines = 0
    rejected_records = 0

    for line in input_path.read_text(
        encoding="utf-8"
    ).splitlines():
        total_raw_lines += 1

        event = parse_log_line(line)

        if event is None:
            rejected_records += 1
            continue

        analytics.add_event(event)

    snapshot = analytics.snapshot()

    metrics = {
        "total_valid_records": int(
            snapshot["total_valid_records"]
        ),
        "total_response_bytes": int(
            snapshot["total_response_bytes"]
        ),
        "requests_per_endpoint": snapshot[
            "requests_per_endpoint"
        ],
        "error_rates": snapshot["error_rates"],
        "traffic_by_hour": snapshot["traffic_by_hour"],
        "status_code_distribution": snapshot[
            "status_code_distribution"
        ],
        "response_byte_totals": snapshot[
            "response_byte_totals"
        ],
    }

    quality = {
        "total_raw_lines": total_raw_lines,
        "valid_records": metrics["total_valid_records"],
        "rejected_records": rejected_records,
    }

    return metrics, quality


def find_differences(
    batch_value: Any,
    stream_value: Any,
    path: str,
) -> list[str]:
    """Return readable differences between two nested values."""

    differences: list[str] = []

    if isinstance(batch_value, dict) and isinstance(
        stream_value,
        dict,
    ):
        all_keys = sorted(
            set(batch_value) | set(stream_value),
            key=str,
        )

        for key in all_keys:
            child_path = f"{path}.{key}"

            if key not in batch_value:
                differences.append(
                    f"{child_path}: missing from batch; "
                    f"stream={stream_value[key]!r}"
                )
                continue

            if key not in stream_value:
                differences.append(
                    f"{child_path}: "
                    f"batch={batch_value[key]!r}; "
                    "missing from stream"
                )
                continue

            differences.extend(
                find_differences(
                    batch_value[key],
                    stream_value[key],
                    child_path,
                )
            )

        return differences

    if (
        isinstance(batch_value, (int, float))
        and isinstance(stream_value, (int, float))
    ):
        if not math.isclose(
            float(batch_value),
            float(stream_value),
            rel_tol=1e-9,
            abs_tol=1e-12,
        ):
            differences.append(
                f"{path}: batch={batch_value!r}; "
                f"stream={stream_value!r}"
            )

        return differences

    if batch_value != stream_value:
        differences.append(
            f"{path}: batch={batch_value!r}; "
            f"stream={stream_value!r}"
        )

    return differences


def json_safe(value: Any) -> Any:
    """Convert dictionary keys into strings for consistent JSON."""

    if isinstance(value, dict):
        return {
            str(key): json_safe(item)
            for key, item in value.items()
        }

    if isinstance(value, list):
        return [
            json_safe(item)
            for item in value
        ]

    return value


def compare_metrics(
    batch_metrics: dict[str, Any],
    stream_metrics: dict[str, Any],
) -> tuple[list[dict[str, Any]], bool]:
    """Compare every agreed batch and streaming metric."""

    comparisons: list[dict[str, Any]] = []

    for metric in COMPARABLE_METRICS:
        differences = find_differences(
            batch_metrics[metric],
            stream_metrics[metric],
            metric,
        )

        match = not differences

        comparisons.append(
            {
                "metric": metric,
                "match": match,
                "differences": differences,
            }
        )

        status = "PASS" if match else "FAIL"
        print(f"[{status}] {metric}")

        for difference in differences:
            print(f"  {difference}")

    all_metrics_match = all(
        item["match"]
        for item in comparisons
    )

    return comparisons, all_metrics_match


def main() -> int:
    parser = argparse.ArgumentParser(
        description=(
            "Compare PySpark batch analytics with "
            "streaming sliding-window analytics."
        )
    )

    parser.add_argument(
        "--input",
        default=(
            "tests/fixtures/"
            "integration_window.log"
        ),
        help="Controlled Nginx access-log file.",
    )

    parser.add_argument(
        "--window-seconds",
        type=int,
        default=300,
        help="Streaming event-time window.",
    )

    parser.add_argument(
        "--output",
        default=(
            "results/integration/"
            "batch_stream_comparison.json"
        ),
        help="JSON evidence output.",
    )

    args = parser.parse_args()

    input_path = Path(args.input)

    if not input_path.is_file():
        raise FileNotFoundError(
            f"Input file not found: {input_path}"
        )

    spark = build_spark_session(
        "day4-batch-stream-comparison"
    )
    spark.sparkContext.setLogLevel("ERROR")

    batch_df = None

    try:
        batch_df, batch_quality = (
            read_raw_data_with_quality(
                spark,
                str(input_path),
            )
        )

        batch_results = compute_batch_metrics(
            batch_df
        )

        batch_metrics = collect_batch_metrics(
            batch_results
        )

        stream_metrics, stream_quality = (
            calculate_stream_metrics(
                input_path,
                args.window_seconds,
            )
        )

        print()
        print("=== BATCH VERSUS STREAM COMPARISON ===")

        comparisons, all_metrics_match = (
            compare_metrics(
                batch_metrics,
                stream_metrics,
            )
        )

        print()

        if all_metrics_match:
            print(
                "FINAL RESULT: "
                "ALL COMPARABLE METRICS MATCH"
            )
        else:
            print(
                "FINAL RESULT: "
                "DIFFERENCES WERE FOUND"
            )

        evidence = {
            "input_path": str(input_path),
            "window_seconds": args.window_seconds,
            "comparison_rule": (
                "The same controlled input is processed "
                "by both pipelines. Streaming uses a "
                "300-second event-time window containing "
                "all valid fixture records."
            ),
            "batch_quality": batch_quality,
            "stream_quality": stream_quality,
            "batch_metrics": batch_metrics,
            "stream_metrics": stream_metrics,
            "comparisons": comparisons,
            "all_metrics_match": all_metrics_match,
        }

        output_path = Path(args.output)
        output_path.parent.mkdir(
            parents=True,
            exist_ok=True,
        )

        output_path.write_text(
            json.dumps(
                json_safe(evidence),
                indent=2,
                sort_keys=True,
            ),
            encoding="utf-8",
        )

        print(
            f"Evidence saved to: {output_path}"
        )

        return 0 if all_metrics_match else 1

    finally:
        if batch_df is not None:
            batch_df.unpersist()

        spark.stop()


if __name__ == "__main__":
    raise SystemExit(main())

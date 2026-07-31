"""Benchmark Kinesis-to-Lambda-to-S3 processing under controlled loads.

Each benchmark run uses a unique endpoint. The script sends synthetic
web-log events to Kinesis and polls the speed-layer S3 snapshot until
the expected endpoint count appears or the timeout is reached.

This measures time to materialise the recent serving snapshot. It is
not a maximum-capacity benchmark.
"""

from __future__ import annotations

import argparse
import json
import time
from datetime import datetime, timezone
from pathlib import Path
from time import perf_counter
from typing import Any, Iterator
from uuid import uuid4

import boto3

from producer.replay_logs import (
    build_kinesis_record,
    send_batch_with_retries,
)


DEFAULT_REGION = "us-east-1"
DEFAULT_STREAM = "scp-access-log-stream-25186396"
DEFAULT_BUCKET = "scp-speed-results-25186396"
DEFAULT_KEY = "speed/latest_snapshot.json"
DEFAULT_OUTPUT = (
    "results/benchmark/"
    "lambda_s3_multi_load.json"
)


def chunk_records(
    records: list[dict[str, Any]],
    batch_size: int = 500,
) -> Iterator[list[dict[str, Any]]]:
    """Yield Kinesis PutRecords-compatible batches."""

    if batch_size < 1 or batch_size > 500:
        raise ValueError(
            "batch_size must be between 1 and 500"
        )

    for start in range(
        0,
        len(records),
        batch_size,
    ):
        yield records[
            start : start + batch_size
        ]


def generate_events(
    *,
    volume: int,
    endpoint: str,
) -> list[dict[str, Any]]:
    """Create deterministic valid web-log events."""

    if volume < 1:
        raise ValueError(
            "volume must be at least 1"
        )

    timestamp = datetime.now(
        timezone.utc
    ).isoformat()

    events: list[dict[str, Any]] = []

    for index in range(volume):
        # Five percent of events receive HTTP 500.
        status_code = (
            500
            if (index + 1) % 20 == 0
            else 200
        )

        event = {
            "client_ip": (
                f"10.20."
                f"{(index // 250) % 250}."
                f"{(index % 250) + 1}"
            ),
            "timestamp": timestamp,
            "method": "GET",
            "endpoint": endpoint,
            "protocol": "HTTP/1.1",
            "status_code": status_code,
            "response_bytes": (
                200 + index % 50
            ),
            "referrer": "-",
            "user_agent": (
                "lambda-s3-load-benchmark"
            ),
        }

        events.append(event)

    return events


def load_speed_snapshot(
    *,
    s3_client: Any,
    bucket: str,
    key: str,
) -> dict[str, Any]:
    """Load the latest speed-layer JSON object."""

    response = s3_client.get_object(
        Bucket=bucket,
        Key=key,
    )

    raw_body = response["Body"].read()

    document = json.loads(
        raw_body.decode("utf-8-sig")
    )

    if not isinstance(document, dict):
        raise ValueError(
            "Expected the S3 snapshot "
            "to contain a JSON object."
        )

    return document


def extract_endpoint_count(
    document: dict[str, Any],
    endpoint: str,
) -> int:
    """Read one endpoint count from a speed snapshot."""

    snapshot = document.get(
        "snapshot",
        {},
    )

    if not isinstance(snapshot, dict):
        return 0

    requests = snapshot.get(
        "requests_per_endpoint",
        {},
    )

    if not isinstance(requests, dict):
        return 0

    try:
        return int(
            requests.get(
                endpoint,
                0,
            )
        )
    except (TypeError, ValueError):
        return 0


def poll_for_materialised_count(
    *,
    s3_client: Any,
    bucket: str,
    key: str,
    endpoint: str,
    expected_count: int,
    previous_generated_at: str | None,
    timeout_seconds: float,
    poll_seconds: float,
) -> dict[str, Any]:
    """Wait until the expected endpoint count is visible in S3."""

    started = perf_counter()
    maximum_observed = 0
    latest_document: dict[str, Any] = {}

    while (
        perf_counter() - started
        <= timeout_seconds
    ):
        latest_document = (
            load_speed_snapshot(
                s3_client=s3_client,
                bucket=bucket,
                key=key,
            )
        )

        generated_at = (
            latest_document.get(
                "generated_at"
            )
        )

        observed_count = (
            extract_endpoint_count(
                latest_document,
                endpoint,
            )
        )

        maximum_observed = max(
            maximum_observed,
            observed_count,
        )

        snapshot_changed = (
            generated_at
            != previous_generated_at
        )

        if (
            snapshot_changed
            and observed_count
            >= expected_count
        ):
            elapsed = (
                perf_counter() - started
            )

            return {
                "completed": True,
                "observed_count": (
                    observed_count
                ),
                "maximum_observed_count": (
                    maximum_observed
                ),
                "poll_seconds": elapsed,
                "document": latest_document,
            }

        if poll_seconds > 0:
            time.sleep(poll_seconds)

    return {
        "completed": False,
        "observed_count": (
            extract_endpoint_count(
                latest_document,
                endpoint,
            )
            if latest_document
            else 0
        ),
        "maximum_observed_count": (
            maximum_observed
        ),
        "poll_seconds": (
            perf_counter() - started
        ),
        "document": latest_document,
    }


def run_load(
    *,
    volume: int,
    kinesis_client: Any,
    s3_client: Any,
    stream_name: str,
    bucket: str,
    key: str,
    timeout_seconds: float,
    poll_seconds: float,
) -> dict[str, Any]:
    """Execute one controlled load and wait for its S3 view."""

    run_id = (
        datetime.now(timezone.utc)
        .strftime("%Y%m%dT%H%M%S")
        + "-"
        + uuid4().hex[:8]
    )

    endpoint = (
        f"/benchmark/lambda-s3/"
        f"{volume}/{run_id}"
    )

    previous_snapshot = load_speed_snapshot(
        s3_client=s3_client,
        bucket=bucket,
        key=key,
    )

    previous_generated_at = (
        previous_snapshot.get(
            "generated_at"
        )
    )

    events = generate_events(
        volume=volume,
        endpoint=endpoint,
    )

    records = [
        build_kinesis_record(event)
        for event in events
    ]

    total_started = perf_counter()
    producer_started = perf_counter()

    successful_records = 0
    failed_records = 0
    batches_sent = 0

    for batch in chunk_records(
        records,
        batch_size=500,
    ):
        successful, failed = (
            send_batch_with_retries(
                client=kinesis_client,
                stream_name=stream_name,
                records=batch,
                max_attempts=3,
                retry_delay=1.0,
            )
        )

        successful_records += successful
        failed_records += failed
        batches_sent += 1

    producer_seconds = (
        perf_counter() - producer_started
    )

    poll_result = (
        poll_for_materialised_count(
            s3_client=s3_client,
            bucket=bucket,
            key=key,
            endpoint=endpoint,
            expected_count=(
                successful_records
            ),
            previous_generated_at=(
                previous_generated_at
            ),
            timeout_seconds=(
                timeout_seconds
            ),
            poll_seconds=poll_seconds,
        )
    )

    total_seconds = (
        perf_counter() - total_started
    )

    snapshot_document = (
        poll_result["document"]
    )

    snapshot = snapshot_document.get(
        "snapshot",
        {},
    )

    if not isinstance(snapshot, dict):
        snapshot = {}

    endpoint_error_rates = (
        snapshot.get(
            "error_rates",
            {},
        )
    )

    if not isinstance(
        endpoint_error_rates,
        dict,
    ):
        endpoint_error_rates = {}

    endpoint_error_details = (
        endpoint_error_rates.get(
            endpoint,
            {},
        )
    )

    return {
        "run_id": run_id,
        "endpoint": endpoint,
        "requested_records": volume,
        "successful_records": (
            successful_records
        ),
        "failed_records": failed_records,
        "batches_sent": batches_sent,
        "producer_seconds": round(
            producer_seconds,
            6,
        ),
        "producer_throughput_records_per_second": (
            round(
                successful_records
                / producer_seconds,
                3,
            )
            if producer_seconds > 0
            else None
        ),
        "snapshot_completed": (
            poll_result["completed"]
        ),
        "snapshot_observed_records": (
            poll_result[
                "observed_count"
            ]
        ),
        "snapshot_maximum_observed_records": (
            poll_result[
                "maximum_observed_count"
            ]
        ),
        "time_to_snapshot_seconds": round(
            poll_result["poll_seconds"],
            6,
        ),
        "complete_runtime_seconds": round(
            total_seconds,
            6,
        ),
        "end_to_end_throughput_records_per_second": (
            round(
                poll_result[
                    "observed_count"
                ]
                / total_seconds,
                3,
            )
            if total_seconds > 0
            else None
        ),
        "snapshot_generated_at": (
            snapshot_document.get(
                "generated_at"
            )
        ),
        "lambda_window_event_count": (
            snapshot.get(
                "window_event_count"
            )
        ),
        "endpoint_error_rate": (
            endpoint_error_details.get(
                "error_rate"
            )
            if isinstance(
                endpoint_error_details,
                dict,
            )
            else None
        ),
    }


def parse_arguments() -> argparse.Namespace:
    """Parse command-line settings."""

    parser = argparse.ArgumentParser(
        description=(
            "Benchmark Kinesis to Lambda to "
            "S3 snapshot materialisation."
        )
    )

    parser.add_argument(
        "--stream-name",
        default=DEFAULT_STREAM,
    )

    parser.add_argument(
        "--region",
        default=DEFAULT_REGION,
    )

    parser.add_argument(
        "--bucket",
        default=DEFAULT_BUCKET,
    )

    parser.add_argument(
        "--key",
        default=DEFAULT_KEY,
    )

    parser.add_argument(
        "--volumes",
        type=int,
        nargs="+",
        default=[100, 500, 1000],
    )

    parser.add_argument(
        "--timeout-seconds",
        type=float,
        default=120.0,
    )

    parser.add_argument(
        "--poll-seconds",
        type=float,
        default=1.0,
    )

    parser.add_argument(
        "--output",
        default=DEFAULT_OUTPUT,
    )

    return parser.parse_args()


def main() -> None:
    """Run every requested load and write JSON evidence."""

    arguments = parse_arguments()

    kinesis_client = boto3.client(
        "kinesis",
        region_name=arguments.region,
    )

    s3_client = boto3.client(
        "s3",
        region_name=arguments.region,
    )

    benchmark_started_at = (
        datetime.now(
            timezone.utc
        ).isoformat()
    )

    runs: list[dict[str, Any]] = []

    for volume in arguments.volumes:
        print(
            f"\n=== LOAD {volume} ==="
        )

        result = run_load(
            volume=volume,
            kinesis_client=(
                kinesis_client
            ),
            s3_client=s3_client,
            stream_name=(
                arguments.stream_name
            ),
            bucket=arguments.bucket,
            key=arguments.key,
            timeout_seconds=(
                arguments.timeout_seconds
            ),
            poll_seconds=(
                arguments.poll_seconds
            ),
        )

        runs.append(result)

        print(
            "Successful records:",
            result["successful_records"],
        )
        print(
            "Failed records:",
            result["failed_records"],
        )
        print(
            "Snapshot observed records:",
            result[
                "snapshot_observed_records"
            ],
        )
        print(
            "Snapshot completed:",
            result["snapshot_completed"],
        )
        print(
            "Producer throughput:",
            result[
                "producer_throughput_records_per_second"
            ],
        )
        print(
            "End-to-end throughput:",
            result[
                "end_to_end_throughput_records_per_second"
            ],
        )
        print(
            "Time to snapshot:",
            result[
                "time_to_snapshot_seconds"
            ],
        )

    document = {
        "benchmark_type": (
            "kinesis_lambda_s3_multi_load"
        ),
        "started_at": (
            benchmark_started_at
        ),
        "region": arguments.region,
        "stream_name": (
            arguments.stream_name
        ),
        "speed_snapshot": (
            f"s3://{arguments.bucket}/"
            f"{arguments.key}"
        ),
        "trigger_configuration": {
            "batch_size": 100,
            "parallelization_factor": 1,
        },
        "all_runs_completed": all(
            run["snapshot_completed"]
            and run["failed_records"] == 0
            for run in runs
        ),
        "runs": runs,
    }

    output_path = Path(
        arguments.output
    )

    output_path.parent.mkdir(
        parents=True,
        exist_ok=True,
    )

    output_path.write_text(
        json.dumps(
            document,
            indent=2,
        )
        + "\n",
        encoding="utf-8",
        newline="\n",
    )

    print(
        "\n=== BENCHMARK SUMMARY ==="
    )
    print(
        "Output:",
        output_path,
    )
    print(
        "All runs completed:",
        document[
            "all_runs_completed"
        ],
    )


if __name__ == "__main__":
    main()

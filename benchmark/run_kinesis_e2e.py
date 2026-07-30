"""Run a real end-to-end smoke benchmark through Amazon Kinesis."""

from __future__ import annotations

import argparse
import json
from pathlib import Path
from time import perf_counter
from typing import Any

import boto3
from botocore.exceptions import BotoCoreError, ClientError

from producer.replay_logs import replay_file
from speed.kinesis_reader import (
    create_shard_iterator,
    get_first_shard_id,
    read_records,
)
from speed.stream_consumer import decode_kinesis_record
from speed.stream_metrics import benchmark_stream_records
from speed.window_analytics import SlidingWindowAnalytics


def round_value(
    value: Any,
    digits: int = 6,
) -> Any:
    """Round floating-point values while preserving None and integers."""

    if isinstance(value, float):
        return round(value, digits)

    return value


def run_kinesis_benchmark(
    stream_name: str,
    region: str,
    input_path: Path,
    output_path: Path,
    expected_records: int,
) -> dict[str, Any]:
    """
    Send sample records to Kinesis and consume the same new records.

    The LATEST iterator is created before the producer sends its events.
    This prevents older records already in the shard from entering the
    current benchmark.
    """

    if not input_path.is_file():
        raise FileNotFoundError(
            f"Input file not found: {input_path}"
        )

    # Use the default AWS Learner Lab credentials.
    client = boto3.client(
        "kinesis",
        region_name=region,
    )

    # Find the stream shard before creating the read position.
    shard_id = get_first_shard_id(
        client=client,
        stream_name=stream_name,
    )

    # LATEST means that only records written after this point
    # should be returned by the consumer.
    shard_iterator = create_shard_iterator(
        client=client,
        stream_name=stream_name,
        shard_id=shard_id,
        iterator_type="LATEST",
    )

    # Start measuring the complete producer-to-consumer operation.
    end_to_end_start = perf_counter()

    # Send the fixture through the real PutRecords API.
    replay_results = replay_file(
        client=client,
        stream_name=stream_name,
        input_path=input_path,
        batch_size=expected_records,
        max_attempts=3,
        retry_delay=1.0,
    )

    # Measure how long the consumer waits for records to become available.
    read_start = perf_counter()

    read_results = read_records(
        client=client,
        shard_iterator=shard_iterator,
        expected_records=expected_records,
        max_attempts=10,
        limit=10,
        sleep_seconds=1.0,
    )

    read_wait_seconds = (
        perf_counter() - read_start
    )

    received_records = read_results[
        "records"
    ]

    # Keep a large window so all fixture records remain together.
    analytics = SlidingWindowAnalytics(
        window_seconds=10 * 365 * 24 * 60 * 60,
    )

    # Process the real Kinesis records and calculate latency from
    # each event's ingested_at value to consumer completion.
    consumer_metrics = benchmark_stream_records(
        records=received_records,
        analytics=analytics,
    )

    complete_runtime_seconds = (
        perf_counter() - end_to_end_start
    )

    # Decode records again only to produce readable evidence fields.
    decoded_events = []

    for record in received_records:
        event = decode_kinesis_record(
            record
        )

        if event is not None:
            decoded_events.append(
                event
            )

    snapshot = analytics.snapshot()

    results = {
        "benchmark_type": "real_kinesis_smoke_test",
        "stream_name": stream_name,
        "region": region,
        "shard_id": shard_id,
        "iterator_type": "LATEST",
        "input_file": input_path.as_posix(),
        "complete_runtime_seconds": round_value(
            complete_runtime_seconds
        ),
        "producer": replay_results,
        "reader": {
            "records_received": read_results[
                "records_received"
            ],
            "read_attempts": read_results[
                "read_attempts"
            ],
            "read_wait_seconds": round_value(
                read_wait_seconds
            ),
        },
        "consumer": {
            "records_attempted": consumer_metrics[
                "records_attempted"
            ],
            "records_processed": consumer_metrics[
                "records_processed"
            ],
            "invalid_records": consumer_metrics[
                "invalid_records"
            ],
            "local_processing_runtime_seconds": (
                round_value(
                    consumer_metrics[
                        "runtime_seconds"
                    ]
                )
            ),
            "local_processing_throughput_records_per_second": (
                round_value(
                    consumer_metrics[
                        "throughput_records_per_second"
                    ]
                )
            ),
        },
        "end_to_end_latency": {
            "definition": (
                "Time from producer ingested_at to completion of "
                "consumer sliding-window processing. It includes "
                "PutRecords, Kinesis availability, polling, JSON "
                "decoding and window analytics."
            ),
            "samples": consumer_metrics[
                "latency_samples"
            ],
            "minimum_ms": round_value(
                consumer_metrics[
                    "latency_min_ms"
                ]
            ),
            "mean_ms": round_value(
                consumer_metrics[
                    "latency_mean_ms"
                ]
            ),
            "p50_ms": round_value(
                consumer_metrics[
                    "latency_p50_ms"
                ]
            ),
            "p95_ms": round_value(
                consumer_metrics[
                    "latency_p95_ms"
                ]
            ),
            "maximum_ms": round_value(
                consumer_metrics[
                    "latency_max_ms"
                ]
            ),
        },
        "event_validation": {
            "decoded_events": len(
                decoded_events
            ),
            "event_ids_present": all(
                bool(event.get("event_id"))
                for event in decoded_events
            ),
            "sources": sorted(
                {
                    event.get("source")
                    for event in decoded_events
                    if event.get("source")
                }
            ),
            "endpoints": [
                event.get("endpoint")
                for event in decoded_events
            ],
        },
        "window": {
            "event_count": snapshot[
                "window_event_count"
            ],
            "requests_per_endpoint": snapshot[
                "requests_per_endpoint"
            ],
            "error_rates": snapshot[
                "error_rates"
            ],
            "status_code_distribution": snapshot[
                "status_code_distribution"
            ],
        },
    }

    # Store the evidence without account IDs or AWS credentials.
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
    """Read the benchmark configuration from the command line."""

    parser = argparse.ArgumentParser(
        description=(
            "Run a real producer-to-consumer Kinesis smoke benchmark."
        )
    )

    parser.add_argument(
        "--stream-name",
        required=True,
        help="Existing Kinesis stream name.",
    )

    parser.add_argument(
        "--region",
        default="us-east-1",
        help="AWS region containing the stream.",
    )

    parser.add_argument(
        "--input",
        type=Path,
        default=Path(
            "tests/fixtures/sample_access.log"
        ),
        help="Small Nginx fixture used for the smoke test.",
    )

    parser.add_argument(
        "--output",
        type=Path,
        default=Path(
            "results/kinesis_e2e_smoke_test.json"
        ),
        help="JSON evidence output.",
    )

    parser.add_argument(
        "--expected-records",
        type=int,
        default=3,
        help="Number of valid records expected from the fixture.",
    )

    return parser.parse_args()


def main() -> None:
    """Run the benchmark and print its results."""

    arguments = parse_arguments()

    try:
        results = run_kinesis_benchmark(
            stream_name=arguments.stream_name,
            region=arguments.region,
            input_path=arguments.input,
            output_path=arguments.output,
            expected_records=arguments.expected_records,
        )

    except (BotoCoreError, ClientError) as error:
        raise SystemExit(
            f"Real Kinesis benchmark failed: {error}"
        ) from error

    print(
        "\n--- Real Kinesis end-to-end benchmark ---"
    )

    print(
        json.dumps(
            results,
            indent=2,
        )
    )


if __name__ == "__main__":
    main()

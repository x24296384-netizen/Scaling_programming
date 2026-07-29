"""Utilities for replaying parsed access-log events to Amazon Kinesis."""

import json
import time
from collections.abc import Iterable
from datetime import datetime, timezone
from pathlib import Path
from typing import Any
from uuid import uuid4

from producer.log_parser import parse_log_line

def build_kinesis_record(
    event: dict[str, Any],
) -> dict[str, Any]:
    """
    Convert one parsed access-log event into a Kinesis record.

    Streaming metadata is added before the event is serialised.
    Events from the same client use the same partition key.
    """

    # Create a new dictionary so the original parsed event is not changed.
    streaming_event = {
        **event,
        "event_id": str(uuid4()),
        "ingested_at": datetime.now(
            timezone.utc
        ).isoformat(),
        "source": "nginx_access_log",
    }

    # Use the client IP to preserve ordering for the same client.
    partition_key = str(
        streaming_event.get("client_ip") or "unknown"
    )

    # Kinesis expects record data as bytes.
    payload = json.dumps(
        streaming_event,
        separators=(",", ":"),
        ensure_ascii=False,
    ).encode("utf-8")

    return {
        "Data": payload,
        "PartitionKey": partition_key,
    }

def prepare_kinesis_records(
    lines: Iterable[str],
) -> tuple[list[dict[str, Any]], int]:
    """
    Parse raw log lines and prepare valid Kinesis records.

    Invalid lines are skipped and counted so that one malformed line
    does not stop the complete replay operation.
    """

    records: list[dict[str, Any]] = []
    invalid_count = 0

    for line in lines:
        event = parse_log_line(line)

        # Skip lines that do not match the expected Nginx format.
        if event is None:
            invalid_count += 1
            continue

        records.append(
            build_kinesis_record(event)
        )

    return records, invalid_count


def send_batch_with_retries(
    client: Any,
    stream_name: str,
    records: list[dict[str, Any]],
    max_attempts: int = 3,
    retry_delay: float = 1.0,
) -> tuple[int, int]:
    """
    Send one batch of records to Kinesis.

    Only records that fail are retried. The function returns the
    number of successful and permanently failed records.
    """

    if max_attempts < 1:
        raise ValueError(
            "max_attempts must be at least 1"
        )

    # Work with a copy so the original list is not modified.
    pending_records = list(records)
    successful_count = 0

    for attempt in range(
        1,
        max_attempts + 1,
    ):
        # Stop early when every record has succeeded.
        if not pending_records:
            break

        response = client.put_records(
            Records=pending_records,
            StreamName=stream_name,
        )

        response_records = response.get(
            "Records",
            [],
        )

        failed_records: list[dict[str, Any]] = []

        # Kinesis returns one result for each submitted record.
        for index, original_record in enumerate(
            pending_records
        ):
            # Treat a missing response item as a failed record.
            if index >= len(response_records):
                failed_records.append(
                    original_record
                )
                continue

            result = response_records[index]

            # A result containing ErrorCode represents a failed record.
            if result.get("ErrorCode"):
                failed_records.append(
                    original_record
                )
            else:
                successful_count += 1

        # Only failed records remain pending for the next attempt.
        pending_records = failed_records

        should_wait = (
            pending_records
            and attempt < max_attempts
            and retry_delay > 0
        )

        if should_wait:
            time.sleep(retry_delay)

    permanent_failures = len(
        pending_records
    )

    return (
        successful_count,
        permanent_failures,
    )


def replay_lines(
    client: Any,
    stream_name: str,
    lines: Iterable[str],
    batch_size: int = 500,
    max_attempts: int = 3,
    retry_delay: float = 1.0,
) -> dict[str, int]:
    """
    Parse raw log lines and send valid events to Kinesis in batches.

    Lines are processed progressively, so the complete dataset does
    not need to be loaded into memory.
    """

    # Kinesis PutRecords supports a maximum of 500 records per request.
    if batch_size < 1 or batch_size > 500:
        raise ValueError(
            "batch_size must be between 1 and 500"
        )

    statistics = {
        "total_lines": 0,
        "valid_records": 0,
        "invalid_lines": 0,
        "successful_records": 0,
        "failed_records": 0,
        "batches_sent": 0,
    }

    pending_batch: list[dict[str, Any]] = []

    for line in lines:
        statistics["total_lines"] += 1

        event = parse_log_line(line)

        # Invalid records are counted but do not stop the replay.
        if event is None:
            statistics["invalid_lines"] += 1
            continue

        pending_batch.append(
            build_kinesis_record(event)
        )
        statistics["valid_records"] += 1

        # Send the batch when it reaches the configured size.
        if len(pending_batch) == batch_size:
            successful, failed = send_batch_with_retries(
                client=client,
                stream_name=stream_name,
                records=pending_batch,
                max_attempts=max_attempts,
                retry_delay=retry_delay,
            )

            statistics["successful_records"] += successful
            statistics["failed_records"] += failed
            statistics["batches_sent"] += 1

            # Start a new empty batch.
            pending_batch = []

    # Send any remaining records in the final partial batch.
    if pending_batch:
        successful, failed = send_batch_with_retries(
            client=client,
            stream_name=stream_name,
            records=pending_batch,
            max_attempts=max_attempts,
            retry_delay=retry_delay,
        )

        statistics["successful_records"] += successful
        statistics["failed_records"] += failed
        statistics["batches_sent"] += 1

    return statistics


def replay_file(
    client: Any,
    stream_name: str,
    input_path: str | Path,
    batch_size: int = 500,
    max_attempts: int = 3,
    retry_delay: float = 1.0,
) -> dict[str, int]:
    """
    Read an access-log file and replay its valid records to Kinesis.

    The file is opened safely and processed one line at a time.
    """

    path = Path(input_path)

    # Replace unsupported characters instead of stopping the full replay.
    with path.open(
        mode="r",
        encoding="utf-8",
        errors="replace",
    ) as log_file:
        return replay_lines(
            client=client,
            stream_name=stream_name,
            lines=log_file,
            batch_size=batch_size,
            max_attempts=max_attempts,
            retry_delay=retry_delay,
        )

"""Utilities for sending parsed access-log events to Amazon Kinesis."""


import json
import time
from typing import Any


def build_kinesis_record(event: dict[str, Any]) -> dict[str, Any]:
    """
    Convert one parsed access-log event into a Kinesis record.

    The client IP is used as the partition key so that events from the
    same client are normally sent to the same Kinesis shard.
    """
    partition_key = str(event.get("client_ip") or "unknown")

    payload = json.dumps(
        event,
        separators=(",", ":"),
        ensure_ascii=False,
    ).encode("utf-8")

    return {
        "Data": payload,
        "PartitionKey": partition_key,
    }

def send_batch_with_retries(
        client: Any,
        stream_name: str,
        records: list[dict[str, Any]],
        max_attempts: int = 3,
        retry_delay: float = 1.0,
) -> tuple[int, int]:
    """
    Send a batch of records to Kinesis.

    Only records that fail are retried. The function returns the number
    of successful and permanently failed records.
    """

    if max_attempts < 1:
        raise ValueError("max_attempts must be at least 1")

    pending_records = list(records)
    successful_count = 0

    for attempt in range(1, max_attempts + 1):
        if not pending_records:
            break

        response = client.put_records(
            Records=pending_records,
            StreamName=stream_name,
        )

        response_records = response.get("Records", [])
        failed_records = []

        for index, original_record in enumerate(pending_records):
            if index >= len(response_records):
                # This should not happen, but just in case
                failed_records.append(original_record)
                continue

            result = response_records[index]

            if result.get("ErrorCode"):
                failed_records.append(original_record)
            else:
                successful_count += 1

        pending_records = failed_records

        should_retry = (
            pending_records
            and attempt < max_attempts
            and retry_delay > 0
        )

        if should_retry:
            time.sleep(retry_delay)

    permanent_failures = len(pending_records)

    return successful_count, permanent_failures
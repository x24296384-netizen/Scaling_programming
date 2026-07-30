"""Read records from an Amazon Kinesis Data Stream."""

from __future__ import annotations

import time
from collections.abc import Callable
from typing import Any


def get_first_shard_id(
    client: Any,
    stream_name: str,
) -> str:
    """
    Return the ID of the first available shard in the stream.

    The current project uses one shard, so the first shard is enough
    for the real-time consumer.
    """

    # Ask Kinesis for the shards that belong to the stream.
    response = client.list_shards(
        StreamName=stream_name,
    )

    # Use an empty list when the response contains no Shards field.
    shards = response.get(
        "Shards",
        [],
    )

    # A stream without shards cannot be read.
    if not shards:
        raise ValueError(
            f"No shards found for stream: {stream_name}"
        )

    # The project currently creates a stream with one shard.
    return shards[0]["ShardId"]


def create_shard_iterator(
    client: Any,
    stream_name: str,
    shard_id: str,
    iterator_type: str = "TRIM_HORIZON",
) -> str:
    """
    Create a pointer that identifies where reading should begin.

    TRIM_HORIZON starts from the oldest record still available.
    LATEST starts from records added after the iterator is created.
    """

    # Request a new iterator for the selected shard.
    response = client.get_shard_iterator(
        StreamName=stream_name,
        ShardId=shard_id,
        ShardIteratorType=iterator_type,
    )

    # The iterator is required by the GetRecords operation.
    shard_iterator = response.get(
        "ShardIterator"
    )

    # Stop with a clear message when Kinesis returns no iterator.
    if not shard_iterator:
        raise ValueError(
            "Kinesis did not return a shard iterator."
        )

    return shard_iterator


def read_records(
    client: Any,
    shard_iterator: str,
    expected_records: int,
    max_attempts: int = 5,
    limit: int = 1000,
    sleep_seconds: float = 1.0,
    sleeper: Callable[[float], None] = time.sleep,
) -> dict[str, Any]:
    """
    Read records until the expected number arrives or attempts finish.

    Kinesis can return an empty list even when the request succeeds.
    For this reason, the function may need to try more than once.
    """

    # At least one record must be requested.
    if expected_records < 1:
        raise ValueError(
            "expected_records must be greater than zero."
        )

    # At least one read attempt must be allowed.
    if max_attempts < 1:
        raise ValueError(
            "max_attempts must be greater than zero."
        )

    # Store every record received across all read attempts.
    records: list[dict[str, Any]] = []

    # Keep the current iterator so reading can continue from
    # the position returned by the previous Kinesis response.
    current_iterator: str | None = shard_iterator

    attempts = 0

    while (
        current_iterator
        and attempts < max_attempts
        and len(records) < expected_records
    ):
        attempts += 1

        # Read up to the configured number of records.
        response = client.get_records(
            ShardIterator=current_iterator,
            Limit=limit,
        )

        # A successful Kinesis response may contain no records yet.
        new_records = response.get(
            "Records",
            [],
        )

        # Add newly received records to the complete result.
        records.extend(
            new_records
        )

        # Kinesis returns a new iterator for the next read position.
        current_iterator = response.get(
            "NextShardIterator"
        )

        # Wait before trying again only when more records are needed.
        if (
            len(records) < expected_records
            and attempts < max_attempts
            and current_iterator
        ):
            sleeper(
                sleep_seconds
            )

    # Return the records and useful information about the read.
    return {
        "records": records,
        "records_received": len(records),
        "read_attempts": attempts,
        "next_shard_iterator": current_iterator,
    }


def read_from_stream(
    client: Any,
    stream_name: str,
    expected_records: int,
    iterator_type: str = "TRIM_HORIZON",
    max_attempts: int = 5,
    limit: int = 1000,
    sleep_seconds: float = 1.0,
    sleeper: Callable[[float], None] = time.sleep,
) -> dict[str, Any]:
    """
    Find the stream shard, create an iterator and read its records.

    This function combines the complete Kinesis read workflow into
    one reusable operation.
    """

    # Find the shard that will be read.
    shard_id = get_first_shard_id(
        client=client,
        stream_name=stream_name,
    )

    # Create the pointer that defines where reading starts.
    shard_iterator = create_shard_iterator(
        client=client,
        stream_name=stream_name,
        shard_id=shard_id,
        iterator_type=iterator_type,
    )

    # Read the records, retrying empty responses when necessary.
    results = read_records(
        client=client,
        shard_iterator=shard_iterator,
        expected_records=expected_records,
        max_attempts=max_attempts,
        limit=limit,
        sleep_seconds=sleep_seconds,
        sleeper=sleeper,
    )

    # Add stream information to make the final result easier to document.
    results["stream_name"] = stream_name
    results["shard_id"] = shard_id
    results["iterator_type"] = iterator_type

    return results

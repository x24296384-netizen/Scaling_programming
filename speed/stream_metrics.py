"""Performance metrics for the local streaming pipeline."""

from __future__ import annotations

from datetime import datetime, timezone
from statistics import mean
from time import perf_counter
from typing import Any, Callable, Iterable

from speed.stream_consumer import decode_kinesis_record
from speed.window_analytics import SlidingWindowAnalytics


def _parse_utc_timestamp(
    value: Any,
) -> datetime | None:
    """
    Convert an ISO 8601 timestamp to UTC.

    Return None when the timestamp is missing or invalid.
    """

    # A valid timestamp must be a non-empty string.
    if not isinstance(value, str) or not value.strip():
        return None

    timestamp_text = value.strip()

    # Python expects +00:00 instead of the ISO shortcut Z.
    if timestamp_text.endswith("Z"):
        timestamp_text = (
            timestamp_text[:-1] + "+00:00"
        )

    try:
        parsed = datetime.fromisoformat(
            timestamp_text
        )
    except ValueError:
        return None

    # A timezone is required so latency comparisons are reliable.
    if (
        parsed.tzinfo is None
        or parsed.utcoffset() is None
    ):
        return None

    # Normalise every timestamp to UTC.
    return parsed.astimezone(timezone.utc)


def _percentile(
    values: list[float],
    percentile: float,
) -> float | None:
    """
    Calculate a percentile using linear interpolation.

    For example, the p95 of [100, 200, 300] is 290.
    """

    if not values:
        return None

    # Percentiles must be calculated on ordered values.
    ordered = sorted(values)

    # A single value is every percentile of that dataset.
    if len(ordered) == 1:
        return ordered[0]

    # Find the percentile position between two list values.
    position = (
        len(ordered) - 1
    ) * percentile

    lower_index = int(position)

    upper_index = min(
        lower_index + 1,
        len(ordered) - 1,
    )

    fraction = position - lower_index

    # Interpolate when the position is between two values.
    return (
        ordered[lower_index]
        + (
            ordered[upper_index]
            - ordered[lower_index]
        )
        * fraction
    )


def benchmark_stream_records(
    records: Iterable[dict[str, Any]],
    analytics: SlidingWindowAnalytics,
    timer: Callable[[], float] = perf_counter,
    clock: Callable[[], datetime] | None = None,
) -> dict[str, int | float | None]:
    """
    Process Kinesis-style records and calculate performance metrics.

    The timer measures the complete processing runtime.

    The clock records when each valid event finishes processing.
    Latency is calculated using:

        processed_at - ingested_at
    """

    # Use the real UTC clock unless a fake test clock is provided.
    if clock is None:
        clock = lambda: datetime.now(
            timezone.utc
        )

    records_attempted = 0
    records_processed = 0
    invalid_records = 0

    # Store one latency value for every valid event that contains
    # a usable ingested_at timestamp.
    latency_values_ms: list[float] = []

    # Start the complete benchmark timer.
    start_time = timer()

    for record in records:
        records_attempted += 1

        # Convert the Kinesis Data payload from JSON bytes to a dict.
        event = decode_kinesis_record(
            record
        )

        if event is None:
            invalid_records += 1
            continue

        try:
            # Add the valid event to the sliding-window analytics.
            analytics.add_event(event)
        except ValueError:
            invalid_records += 1
            continue

        records_processed += 1

        # Read when the producer originally prepared the event.
        ingested_at = _parse_utc_timestamp(
            event.get("ingested_at")
        )

        # Latency cannot be calculated without ingested_at.
        if ingested_at is None:
            continue

        # Record when the consumer finished processing this event.
        processed_at = clock()

        # Normalise the processing timestamp to UTC.
        if (
            processed_at.tzinfo is None
            or processed_at.utcoffset() is None
        ):
            processed_at = processed_at.replace(
                tzinfo=timezone.utc
            )
        else:
            processed_at = (
                processed_at.astimezone(
                    timezone.utc
                )
            )

        # Convert the time difference from seconds to milliseconds.
        latency_ms = (
            processed_at - ingested_at
        ).total_seconds() * 1000

        # Negative latency normally indicates inconsistent clocks.
        if latency_ms >= 0:
            latency_values_ms.append(
                latency_ms
            )

    # Stop the complete benchmark timer.
    end_time = timer()

    runtime_seconds = max(
        end_time - start_time,
        0.0,
    )

    # Throughput uses only successfully processed records.
    throughput = (
        records_processed / runtime_seconds
        if runtime_seconds > 0
        else 0.0
    )

    # Calculate latency statistics only when samples exist.
    if latency_values_ms:
        latency_min_ms = min(
            latency_values_ms
        )

        latency_mean_ms = mean(
            latency_values_ms
        )

        latency_p50_ms = _percentile(
            latency_values_ms,
            0.50,
        )

        latency_p95_ms = _percentile(
            latency_values_ms,
            0.95,
        )

        latency_max_ms = max(
            latency_values_ms
        )
    else:
        latency_min_ms = None
        latency_mean_ms = None
        latency_p50_ms = None
        latency_p95_ms = None
        latency_max_ms = None

    # Return measured counts and calculated performance values.
    return {
        "records_attempted": records_attempted,
        "records_processed": records_processed,
        "invalid_records": invalid_records,
        "runtime_seconds": runtime_seconds,
        "throughput_records_per_second": throughput,
        "latency_samples": len(
            latency_values_ms
        ),
        "latency_min_ms": latency_min_ms,
        "latency_mean_ms": latency_mean_ms,
        "latency_p50_ms": latency_p50_ms,
        "latency_p95_ms": latency_p95_ms,
        "latency_max_ms": latency_max_ms,
    }
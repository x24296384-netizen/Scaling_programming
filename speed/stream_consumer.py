"""Decode Kinesis records and update the speed-layer analytics."""

from __future__ import annotations

import json
from typing import Any

from speed.window_analytics import SlidingWindowAnalytics


def decode_kinesis_record(
    record: dict[str, Any],
) -> dict[str, Any] | None:
    """
    Decode the JSON payload contained in one Kinesis record.

    Return None when the record has missing, invalid or malformed data.
    """

    payload = record.get("Data")

    if isinstance(payload, bytes):
        try:
            payload_text = payload.decode("utf-8")
        except UnicodeDecodeError:
            return None
    elif isinstance(payload, str):
        payload_text = payload
    else:
        return None

    try:
        event = json.loads(payload_text)
    except (json.JSONDecodeError, TypeError):
        return None

    # The speed layer expects one JSON object per Kinesis record.
    if not isinstance(event, dict):
        return None

    return event


def process_kinesis_record(
    record: dict[str, Any],
    analytics: SlidingWindowAnalytics,
) -> bool:
    """
    Decode one Kinesis record and add it to the sliding window.

    Return True when the event is processed successfully.
    Return False when the record or event is invalid.
    """

    event = decode_kinesis_record(record)

    if event is None:
        return False

    try:
        analytics.add_event(event)
    except ValueError:
        return False

    return True

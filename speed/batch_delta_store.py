"""Persist immutable speed-layer batch deltas in Amazon S3.

A Lambda execution environment cannot share in-memory analytics state
with another execution environment. Therefore, each invocation stores
the valid events that it processed in a unique S3 object.

A serving-layer aggregator can later read these objects, remove
duplicates by event_id and calculate one global recent-window view.
"""

from __future__ import annotations

import json
import os
import re
from datetime import datetime, timezone
from typing import Any
from uuid import uuid4

import boto3


DEFAULT_BATCH_PREFIX = "speed/batches"


def _utc_datetime(
    value: str | None = None,
) -> datetime:
    """Return a timezone-aware UTC datetime."""

    if value is None:
        return datetime.now(
            timezone.utc
        )

    normalised = value.replace(
        "Z",
        "+00:00",
    )

    parsed = datetime.fromisoformat(
        normalised
    )

    if parsed.tzinfo is None:
        parsed = parsed.replace(
            tzinfo=timezone.utc
        )

    return parsed.astimezone(
        timezone.utc
    )


def _safe_component(
    value: str,
) -> str:
    """Make one value safe for use inside an S3 key."""

    cleaned = re.sub(
        r"[^A-Za-z0-9._-]+",
        "-",
        value,
    ).strip("-")

    return cleaned or uuid4().hex


def build_batch_delta_document(
    *,
    events: list[dict[str, Any]],
    request_id: str,
    generated_at: str | None = None,
) -> dict[str, Any]:
    """Build one immutable per-invocation event document."""

    generated_datetime = _utc_datetime(
        generated_at
    )

    stored_events: list[
        dict[str, Any]
    ] = []

    for event in events:
        if not isinstance(event, dict):
            continue

        stored_events.append(
            {
                "event_id": event.get(
                    "event_id"
                ),
                "sequence_number": event.get(
                    "sequence_number"
                ),
                "timestamp": event.get(
                    "timestamp"
                ),
                "client_ip": event.get(
                    "client_ip"
                ),
                "method": event.get(
                    "method"
                ),
                "endpoint": event.get(
                    "endpoint"
                ),
                "status_code": event.get(
                    "status_code"
                ),
                "response_bytes": event.get(
                    "response_bytes"
                ),
                "source": event.get(
                    "source"
                ),
            }
        )

    return {
        "schema_version": 1,
        "document_type": (
            "speed_batch_delta"
        ),
        "generated_at": (
            generated_datetime.isoformat()
        ),
        "lambda_request_id": request_id,
        "record_count": len(
            stored_events
        ),
        "events": stored_events,
    }


def build_batch_delta_key(
    *,
    request_id: str,
    generated_at: str | None = None,
    prefix: str = DEFAULT_BATCH_PREFIX,
) -> str:
    """Build a unique time-partitioned S3 object key."""

    generated_datetime = _utc_datetime(
        generated_at
    )

    effective_prefix = (
        prefix.strip("/")
        or DEFAULT_BATCH_PREFIX
    )

    partition = generated_datetime.strftime(
        "%Y/%m/%d/%H"
    )

    timestamp_component = (
        generated_datetime.strftime(
            "%Y%m%dT%H%M%S%fZ"
        )
    )

    request_component = _safe_component(
        request_id
    )

    return (
        f"{effective_prefix}/"
        f"{partition}/"
        f"{timestamp_component}-"
        f"{request_component}.json"
    )


def persist_batch_delta(
    *,
    events: list[dict[str, Any]],
    request_id: str,
    s3_client: Any | None = None,
    bucket: str | None = None,
    prefix: str | None = None,
    generated_at: str | None = None,
) -> dict[str, Any]:
    """Persist one immutable invocation delta in S3."""

    effective_bucket = (
        bucket
        if bucket is not None
        else os.getenv(
            "SPEED_RESULTS_BUCKET",
            "",
        )
    ).strip()

    effective_prefix = (
        prefix
        if prefix is not None
        else os.getenv(
            "SPEED_BATCH_PREFIX",
            DEFAULT_BATCH_PREFIX,
        )
    ).strip()

    if not effective_prefix:
        effective_prefix = (
            DEFAULT_BATCH_PREFIX
        )

    if not effective_bucket:
        return {
            "enabled": False,
            "stored": False,
            "bucket": None,
            "key": None,
            "record_count": len(events),
            "reason": (
                "SPEED_RESULTS_BUCKET "
                "is not configured"
            ),
        }

    if not events:
        return {
            "enabled": True,
            "stored": False,
            "bucket": effective_bucket,
            "key": None,
            "record_count": 0,
            "reason": (
                "no processed events"
            ),
        }

    document = build_batch_delta_document(
        events=events,
        request_id=request_id,
        generated_at=generated_at,
    )

    key = build_batch_delta_key(
        request_id=request_id,
        generated_at=document[
            "generated_at"
        ],
        prefix=effective_prefix,
    )

    body = json.dumps(
        document,
        ensure_ascii=False,
        sort_keys=True,
        separators=(",", ":"),
    ).encode("utf-8")

    client = (
        s3_client
        if s3_client is not None
        else boto3.client("s3")
    )

    response = client.put_object(
        Bucket=effective_bucket,
        Key=key,
        Body=body,
        ContentType="application/json",
        ServerSideEncryption="AES256",
    )

    return {
        "enabled": True,
        "stored": True,
        "bucket": effective_bucket,
        "key": key,
        "record_count": document[
            "record_count"
        ],
        "bytes_written": len(body),
        "etag": response.get("ETag"),
    }

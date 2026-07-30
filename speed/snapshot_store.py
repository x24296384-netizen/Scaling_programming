"""Persist speed-layer analytics snapshots in Amazon S3.

The Lambda handler calculates recent sliding-window metrics.
This module converts those results into a stable JSON document and
stores the most recent version in an S3 object.

Expected object:

s3://<bucket>/speed/latest_snapshot.json
"""

from __future__ import annotations

import json
import os
from datetime import datetime, timezone
from typing import Any

import boto3


DEFAULT_SNAPSHOT_KEY = (
    "speed/latest_snapshot.json"
)


def build_snapshot_document(
    *,
    summary: dict[str, Any],
    anomalies: list[dict[str, Any]],
    snapshot: dict[str, Any],
    generated_at: str | None = None,
) -> dict[str, Any]:
    """Build the JSON document stored by the speed layer.

    Keeping this transformation separate makes it possible to test
    the document without connecting to AWS.
    """

    effective_generated_at = (
        generated_at
        if generated_at is not None
        else datetime.now(
            timezone.utc
        ).isoformat()
    )

    return {
        "schema_version": 1,
        "layer": "speed",
        "generated_at": (
            effective_generated_at
        ),
        "summary": summary,
        "anomalies": anomalies,
        "snapshot": snapshot,
    }


def persist_speed_snapshot(
    *,
    summary: dict[str, Any],
    anomalies: list[dict[str, Any]],
    snapshot: dict[str, Any],
    s3_client: Any | None = None,
    bucket: str | None = None,
    key: str | None = None,
    generated_at: str | None = None,
) -> dict[str, Any]:
    """Write the latest speed-layer snapshot to Amazon S3.

    The bucket normally comes from the Lambda environment variable
    SPEED_RESULTS_BUCKET.

    The key normally comes from SPEED_RESULTS_KEY. When that variable
    is missing, speed/latest_snapshot.json is used.

    A client can be supplied by unit tests so that tests do not make
    real AWS requests.
    """

    effective_bucket = (
        bucket
        if bucket is not None
        else os.getenv(
            "SPEED_RESULTS_BUCKET",
            "",
        )
    ).strip()

    effective_key = (
        key
        if key is not None
        else os.getenv(
            "SPEED_RESULTS_KEY",
            DEFAULT_SNAPSHOT_KEY,
        )
    ).strip()

    if not effective_key:
        effective_key = (
            DEFAULT_SNAPSHOT_KEY
        )

    # Local tests and development environments may not configure an
    # S3 bucket. In that case, persistence is safely skipped.
    if not effective_bucket:
        return {
            "enabled": False,
            "stored": False,
            "bucket": None,
            "key": effective_key,
            "reason": (
                "SPEED_RESULTS_BUCKET "
                "is not configured"
            ),
        }

    document = build_snapshot_document(
        summary=summary,
        anomalies=anomalies,
        snapshot=snapshot,
        generated_at=generated_at,
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
        Key=effective_key,
        Body=body,
        ContentType="application/json",
        ServerSideEncryption="AES256",
    )

    return {
        "enabled": True,
        "stored": True,
        "bucket": effective_bucket,
        "key": effective_key,
        "bytes_written": len(body),
        "etag": response.get("ETag"),
    }

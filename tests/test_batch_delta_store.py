"""Tests for immutable speed-layer batch-delta persistence."""

from __future__ import annotations

import json
import unittest
from typing import Any

from speed.batch_delta_store import (
    build_batch_delta_document,
    build_batch_delta_key,
    persist_batch_delta,
)


class FakeS3Client:
    """Small in-memory replacement for boto3 S3."""

    def __init__(self) -> None:
        self.calls: list[
            dict[str, Any]
        ] = []

    def put_object(
        self,
        **kwargs: Any,
    ) -> dict[str, str]:
        self.calls.append(kwargs)

        return {
            "ETag": '"delta-etag"',
        }


class TestBatchDeltaStore(
    unittest.TestCase
):
    """Validate immutable S3 delta documents."""

    def setUp(self) -> None:
        self.events = [
            {
                "event_id": "event-1",
                "sequence_number": "1001",
                "timestamp": (
                    "2026-07-31T02:16:00+00:00"
                ),
                "client_ip": "10.0.0.1",
                "method": "GET",
                "endpoint": "/api/test",
                "status_code": 200,
                "response_bytes": 250,
                "source": "nginx_access_log",
            },
            {
                "event_id": "event-2",
                "sequence_number": "1002",
                "timestamp": (
                    "2026-07-31T02:16:01+00:00"
                ),
                "client_ip": "10.0.0.2",
                "method": "GET",
                "endpoint": "/api/test",
                "status_code": 500,
                "response_bytes": 100,
                "source": "nginx_access_log",
            },
        ]

    def test_document_contains_batch_events(
        self,
    ) -> None:
        document = (
            build_batch_delta_document(
                events=self.events,
                request_id="request-123",
                generated_at=(
                    "2026-07-31T02:16:05+00:00"
                ),
            )
        )

        self.assertEqual(
            document["schema_version"],
            1,
        )
        self.assertEqual(
            document["document_type"],
            "speed_batch_delta",
        )
        self.assertEqual(
            document["record_count"],
            2,
        )
        self.assertEqual(
            document["events"][1][
                "status_code"
            ],
            500,
        )

    def test_key_is_unique_and_partitioned(
        self,
    ) -> None:
        key = build_batch_delta_key(
            request_id="request:123",
            generated_at=(
                "2026-07-31T02:16:05+00:00"
            ),
        )

        self.assertTrue(
            key.startswith(
                "speed/batches/"
                "2026/07/31/02/"
            )
        )
        self.assertIn(
            "request-123.json",
            key,
        )

    def test_missing_bucket_skips_write(
        self,
    ) -> None:
        client = FakeS3Client()

        result = persist_batch_delta(
            events=self.events,
            request_id="request-123",
            s3_client=client,
            bucket="",
        )

        self.assertFalse(
            result["enabled"]
        )
        self.assertFalse(
            result["stored"]
        )
        self.assertEqual(
            client.calls,
            [],
        )

    def test_delta_is_written_as_json(
        self,
    ) -> None:
        client = FakeS3Client()

        result = persist_batch_delta(
            events=self.events,
            request_id="request-123",
            s3_client=client,
            bucket="test-bucket",
            generated_at=(
                "2026-07-31T02:16:05+00:00"
            ),
        )

        self.assertTrue(
            result["stored"]
        )
        self.assertEqual(
            result["record_count"],
            2,
        )
        self.assertEqual(
            len(client.calls),
            1,
        )

        request = client.calls[0]

        self.assertEqual(
            request["Bucket"],
            "test-bucket",
        )
        self.assertEqual(
            request["ContentType"],
            "application/json",
        )
        self.assertEqual(
            request[
                "ServerSideEncryption"
            ],
            "AES256",
        )

        document = json.loads(
            request["Body"].decode(
                "utf-8"
            )
        )

        self.assertEqual(
            document["record_count"],
            2,
        )
        self.assertEqual(
            document["events"][0][
                "event_id"
            ],
            "event-1",
        )


if __name__ == "__main__":
    unittest.main()

"""Unit tests for speed-layer S3 snapshot persistence."""

from __future__ import annotations

import json
import unittest
from typing import Any

from speed.snapshot_store import (
    DEFAULT_SNAPSHOT_KEY,
    build_snapshot_document,
    persist_speed_snapshot,
)


class FakeS3Client:
    """Small in-memory substitute for boto3 S3."""

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
            "ETag": '"test-etag"',
        }


class TestSnapshotStore(
    unittest.TestCase
):
    """Validate JSON creation and S3 writes."""

    def setUp(self) -> None:
        self.summary = {
            "received_records": 2,
            "processed_records": 2,
            "invalid_records": 0,
            "window_event_count": 2,
            "anomaly_count": 1,
        }

        self.anomalies = [
            {
                "endpoint": "/api/login",
                "error_count": 2,
                "error_rate": 1.0,
                "total_requests": 2,
            }
        ]

        self.snapshot = {
            "window_event_count": 2,
            "total_response_bytes": 200,
        }

    def test_snapshot_document_has_stable_schema(
        self,
    ) -> None:
        document = build_snapshot_document(
            summary=self.summary,
            anomalies=self.anomalies,
            snapshot=self.snapshot,
            generated_at=(
                "2026-07-30T20:45:00+00:00"
            ),
        )

        self.assertEqual(
            document["schema_version"],
            1,
        )
        self.assertEqual(
            document["layer"],
            "speed",
        )
        self.assertEqual(
            document["summary"],
            self.summary,
        )
        self.assertEqual(
            document["anomalies"],
            self.anomalies,
        )
        self.assertEqual(
            document["snapshot"],
            self.snapshot,
        )

    def test_missing_bucket_skips_aws_write(
        self,
    ) -> None:
        client = FakeS3Client()

        result = persist_speed_snapshot(
            summary=self.summary,
            anomalies=self.anomalies,
            snapshot=self.snapshot,
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
            result["key"],
            DEFAULT_SNAPSHOT_KEY,
        )
        self.assertEqual(
            client.calls,
            [],
        )

    def test_snapshot_is_written_as_json(
        self,
    ) -> None:
        client = FakeS3Client()

        result = persist_speed_snapshot(
            summary=self.summary,
            anomalies=self.anomalies,
            snapshot=self.snapshot,
            s3_client=client,
            bucket=(
                "scp-speed-results-test"
            ),
            key=(
                "speed/latest_snapshot.json"
            ),
            generated_at=(
                "2026-07-30T20:45:00+00:00"
            ),
        )

        self.assertTrue(
            result["enabled"]
        )
        self.assertTrue(
            result["stored"]
        )
        self.assertEqual(
            len(client.calls),
            1,
        )

        request = client.calls[0]

        self.assertEqual(
            request["Bucket"],
            "scp-speed-results-test",
        )
        self.assertEqual(
            request["Key"],
            "speed/latest_snapshot.json",
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

        stored_document = json.loads(
            request["Body"].decode(
                "utf-8"
            )
        )

        self.assertEqual(
            stored_document["layer"],
            "speed",
        )
        self.assertEqual(
            stored_document["summary"][
                "processed_records"
            ],
            2,
        )
        self.assertEqual(
            stored_document["anomalies"][
                0
            ]["endpoint"],
            "/api/login",
        )


if __name__ == "__main__":
    unittest.main()

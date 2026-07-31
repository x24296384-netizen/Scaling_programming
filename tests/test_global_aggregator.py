"""Tests for the durable global speed-layer aggregator."""

from __future__ import annotations

import json
import unittest
from io import BytesIO
from typing import Any

from speed.global_aggregator import (
    aggregate_delta_documents,
    aggregate_s3_batch_deltas,
    list_batch_delta_keys,
)


class FakeBody:
    """Provide the read method returned by boto3 S3."""

    def __init__(self, content: bytes) -> None:
        self._content = BytesIO(content)

    def read(self) -> bytes:
        return self._content.read()


class FakeS3Client:
    """Small in-memory S3 fake supporting list/get/put operations."""

    def __init__(self, objects: dict[str, Any]) -> None:
        self.objects = {
            key: json.dumps(value).encode("utf-8")
            for key, value in objects.items()
        }
        self.put_calls: list[dict[str, Any]] = []

    def list_objects_v2(self, **kwargs: Any) -> dict[str, Any]:
        prefix = kwargs.get("Prefix", "")
        keys = sorted(key for key in self.objects if key.startswith(prefix))
        return {
            "Contents": [{"Key": key} for key in keys],
            "IsTruncated": False,
        }

    def get_object(self, **kwargs: Any) -> dict[str, FakeBody]:
        return {"Body": FakeBody(self.objects[kwargs["Key"]])}

    def put_object(self, **kwargs: Any) -> dict[str, str]:
        self.put_calls.append(kwargs)
        self.objects[kwargs["Key"]] = kwargs["Body"]
        return {"ETag": '"global-etag"'}


class TestGlobalAggregator(unittest.TestCase):
    """Validate global aggregation and retry deduplication."""

    @staticmethod
    def _document(events: list[dict[str, Any]]) -> dict[str, Any]:
        return {
            "schema_version": 1,
            "document_type": "speed_batch_delta",
            "generated_at": "2026-07-31T10:00:00+00:00",
            "lambda_request_id": "request-1",
            "record_count": len(events),
            "events": events,
        }

    @staticmethod
    def _event(
        event_id: str,
        timestamp: str,
        status_code: int = 200,
    ) -> dict[str, Any]:
        return {
            "event_id": event_id,
            "sequence_number": event_id.replace("event", "sequence"),
            "timestamp": timestamp,
            "client_ip": "10.0.0.1",
            "method": "GET",
            "endpoint": "/api/test",
            "status_code": status_code,
            "response_bytes": 100,
            "source": "nginx_access_log",
        }

    def test_deduplicates_retried_events(self) -> None:
        first = self._event("event-1", "2026-07-31T10:00:00+00:00")
        second = self._event("event-2", "2026-07-31T10:00:01+00:00", 500)

        result = aggregate_delta_documents(
            [
                self._document([first, second]),
                self._document([first]),
            ],
            window_seconds=300,
            error_rate_threshold=0.50,
            minimum_requests=2,
        )

        self.assertEqual(result["summary"]["raw_events"], 3)
        self.assertEqual(result["summary"]["unique_events"], 2)
        self.assertEqual(result["summary"]["duplicate_events"], 1)
        self.assertEqual(result["snapshot"]["window_event_count"], 2)
        self.assertEqual(result["snapshot"]["requests_per_endpoint"]["/api/test"], 2)
        self.assertEqual(len(result["anomalies"]), 1)

    def test_rebuilds_event_time_window(self) -> None:
        old_event = self._event("event-old", "2026-07-31T09:54:59+00:00")
        boundary_event = self._event("event-boundary", "2026-07-31T09:55:00+00:00")
        newest_event = self._event("event-new", "2026-07-31T10:00:00+00:00")

        result = aggregate_delta_documents(
            [self._document([newest_event, old_event, boundary_event])],
            window_seconds=300,
        )

        self.assertEqual(result["summary"]["unique_events"], 3)
        self.assertEqual(result["snapshot"]["window_event_count"], 2)
        self.assertEqual(result["snapshot"]["window_start"], "2026-07-31T09:55:00+00:00")

    def test_invalid_documents_and_events_are_counted(self) -> None:
        invalid_event = self._event("event-invalid", "not-a-timestamp")

        result = aggregate_delta_documents(
            [
                {"document_type": "other", "events": []},
                self._document([invalid_event]),
            ]
        )

        self.assertEqual(result["summary"]["invalid_documents"], 1)
        self.assertEqual(result["summary"]["invalid_events"], 1)
        self.assertEqual(result["snapshot"]["window_event_count"], 0)

    def test_lists_only_json_delta_objects(self) -> None:
        client = FakeS3Client(
            {
                "speed/batches/2026/07/31/a.json": self._document([]),
                "speed/batches/2026/07/31/readme.txt": {},
                "speed/other.json": {},
            }
        )

        keys = list_batch_delta_keys(
            s3_client=client,
            bucket="test-bucket",
        )

        self.assertEqual(keys, ["speed/batches/2026/07/31/a.json"])

    def test_s3_aggregation_writes_global_snapshot(self) -> None:
        first = self._event("event-1", "2026-07-31T10:00:00+00:00")
        second = self._event("event-2", "2026-07-31T10:00:01+00:00")

        client = FakeS3Client(
            {
                "speed/batches/2026/07/31/a.json": self._document([first]),
                "speed/batches/2026/07/31/b.json": self._document([second]),
            }
        )

        result = aggregate_s3_batch_deltas(
            bucket="test-bucket",
            s3_client=client,
        )

        self.assertTrue(result["persistence"]["stored"])
        self.assertEqual(result["source"]["object_count"], 2)
        self.assertEqual(result["snapshot"]["window_event_count"], 2)
        self.assertEqual(len(client.put_calls), 1)
        self.assertEqual(client.put_calls[0]["Key"], "speed/global_snapshot.json")

        stored = json.loads(client.put_calls[0]["Body"].decode("utf-8"))
        self.assertEqual(stored["summary"]["unique_events"], 2)
        self.assertEqual(stored["snapshot"]["window_event_count"], 2)


if __name__ == "__main__":
    unittest.main()

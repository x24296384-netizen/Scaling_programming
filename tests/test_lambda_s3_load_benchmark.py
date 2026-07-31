"""Tests for the Lambda-to-S3 load benchmark helpers."""

from __future__ import annotations

import json
import unittest
from io import BytesIO
from typing import Any

from benchmark.run_lambda_s3_load_benchmark import (
    chunk_records,
    extract_endpoint_count,
    generate_events,
    poll_for_materialised_count,
)


class FakeBody:
    def __init__(
        self,
        document: dict[str, Any],
    ) -> None:
        self.content = BytesIO(
            json.dumps(
                document
            ).encode("utf-8")
        )

    def read(self) -> bytes:
        return self.content.read()


class FakeS3Client:
    def __init__(
        self,
        documents: list[
            dict[str, Any]
        ],
    ) -> None:
        self.documents = documents
        self.index = 0

    def get_object(
        self,
        **kwargs: str,
    ) -> dict[str, FakeBody]:
        del kwargs

        document = self.documents[
            min(
                self.index,
                len(self.documents) - 1,
            )
        ]

        self.index += 1

        return {
            "Body": FakeBody(document)
        }


class TestLambdaS3LoadBenchmark(
    unittest.TestCase
):
    def test_records_are_chunked_at_500(
        self,
    ) -> None:
        records = [
            {"index": index}
            for index in range(1001)
        ]

        batches = list(
            chunk_records(
                records,
                batch_size=500,
            )
        )

        self.assertEqual(
            [len(batch) for batch in batches],
            [500, 500, 1],
        )

    def test_generated_events_use_shared_schema(
        self,
    ) -> None:
        events = generate_events(
            volume=100,
            endpoint="/benchmark/test",
        )

        self.assertEqual(
            len(events),
            100,
        )
        self.assertEqual(
            events[0]["endpoint"],
            "/benchmark/test",
        )
        self.assertEqual(
            sum(
                event["status_code"] == 500
                for event in events
            ),
            5,
        )
        self.assertTrue(
            all(
                "client_ip" in event
                and "response_bytes" in event
                for event in events
            )
        )

    def test_endpoint_count_is_extracted(
        self,
    ) -> None:
        document = {
            "snapshot": {
                "requests_per_endpoint": {
                    "/benchmark/test": 100,
                }
            }
        }

        self.assertEqual(
            extract_endpoint_count(
                document,
                "/benchmark/test",
            ),
            100,
        )

    def test_poll_waits_for_expected_count(
        self,
    ) -> None:
        endpoint = "/benchmark/test"

        client = FakeS3Client(
            [
                {
                    "generated_at": "new-1",
                    "snapshot": {
                        "requests_per_endpoint": {
                            endpoint: 50,
                        }
                    },
                },
                {
                    "generated_at": "new-2",
                    "snapshot": {
                        "requests_per_endpoint": {
                            endpoint: 100,
                        }
                    },
                },
            ]
        )

        result = (
            poll_for_materialised_count(
                s3_client=client,
                bucket="test-bucket",
                key="speed/latest.json",
                endpoint=endpoint,
                expected_count=100,
                previous_generated_at="old",
                timeout_seconds=1.0,
                poll_seconds=0.0,
            )
        )

        self.assertTrue(
            result["completed"]
        )
        self.assertEqual(
            result["observed_count"],
            100,
        )


if __name__ == "__main__":
    unittest.main()

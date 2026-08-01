"""Tests for the combined serving view."""

from __future__ import annotations

import json
import unittest
from io import BytesIO
from typing import Any

from serving.combined_view import (
    build_combined_view,
    load_s3_json,
)


class FakeBody:
    """Provide the read method returned by boto3 S3."""

    def __init__(
        self,
        content: bytes,
    ) -> None:
        self._content = BytesIO(
            content
        )

    def read(self) -> bytes:
        return self._content.read()


class FakeS3Client:
    """Small fake for S3 get_object."""

    def __init__(
        self,
        document: dict[str, Any],
    ) -> None:
        self.document = document
        self.calls: list[
            dict[str, str]
        ] = []

    def get_object(
        self,
        **kwargs: str,
    ) -> dict[str, FakeBody]:
        self.calls.append(kwargs)

        return {
            "Body": FakeBody(
                json.dumps(
                    self.document
                ).encode("utf-8")
            )
        }


class TestCombinedServingView(
    unittest.TestCase
):
    """Validate historical and recent metric integration."""

    def setUp(self) -> None:
        self.batch_document = {
            "batch_metrics": {
                "total_valid_records": 100,
                "total_response_bytes": 10000,
                "baseline_rpm": {
                    "/api/login": 20.0,
                },
                "requests_per_endpoint": {
                    "/api/login": 40,
                },
                "response_byte_totals": {
                    "/api/login": 4000,
                },
                "error_rates": {
                    "/api/login": {
                        "error_count": 4,
                        "error_rate": 0.1,
                        "total_requests": 40,
                    }
                },
                "status_code_distribution": {
                    "200": 90,
                    "500": 10,
                },
            }
        }

        self.speed_document = {
            "generated_at": (
                "2026-07-31T00:30:00+00:00"
            ),
            "anomalies": [
                {
                    "endpoint": "/api/login",
                    "error_rate": 0.4,
                }
            ],
            "snapshot": {
                "window_seconds": 300,
                "window_event_count": 5,
                "total_valid_records": 5,
                "total_response_bytes": 500,
                "requests_per_endpoint": {
                    "/api/login": 5,
                },
                "response_byte_totals": {
                    "/api/login": 500,
                },
                "error_rates": {
                    "/api/login": {
                        "error_count": 2,
                        "error_rate": 0.4,
                        "total_requests": 5,
                    }
                },
                "status_code_distribution": {
                    "200": 3,
                    "500": 2,
                },
            },
        }

    def test_builds_normalised_comparisons(
        self,
    ) -> None:
        result = build_combined_view(
            batch_document=(
                self.batch_document
            ),
            speed_document=(
                self.speed_document
            ),
            generated_at=(
                "2026-07-31T00:31:00+00:00"
            ),
        )

        self.assertEqual(
            result["totals"][
                "historical_valid_records"
            ],
            100,
        )

        self.assertEqual(
            result["totals"][
                "recent_valid_records"
            ],
            5,
        )

        self.assertEqual(
            result[
                "traffic_comparison"
            ]["recent_rpm"],
            1.0,
        )

        self.assertEqual(
            result[
                "traffic_comparison"
            ]["baseline_rpm"],
            20.0,
        )

        self.assertEqual(
            result[
                "traffic_comparison"
            ]["traffic_status"],
            "below baseline",
        )

        endpoint = result[
            "endpoint_comparison"
        ]["/api/login"]

        self.assertEqual(
            endpoint[
                "historical_requests"
            ],
            40,
        )

        self.assertEqual(
            endpoint["recent_requests"],
            5,
        )

        self.assertAlmostEqual(
            endpoint[
                "error_rate_difference"
            ],
            0.3,
        )

        self.assertEqual(
            endpoint[
                "historical_baseline_rpm"
            ],
            20.0,
        )

        self.assertEqual(
            endpoint[
                "recent_to_baseline_ratio"
            ],
            0.05,
        )

        self.assertFalse(
            endpoint[
                "significant_increase"
            ]
        )

    def test_missing_baseline_rpm_is_explicit(
        self,
    ) -> None:
        batch_document = {
            "batch_metrics": {
                "total_valid_records": 7,
                "requests_per_endpoint": {
                    "/": 7,
                },
            }
        }

        result = build_combined_view(
            batch_document=batch_document,
            speed_document=(
                self.speed_document
            ),
        )

        comparison = result[
            "traffic_comparison"
        ]

        self.assertFalse(
            comparison[
                "comparison_available"
            ]
        )

        self.assertIsNone(
            comparison["baseline_rpm"]
        )

        self.assertEqual(
            comparison["traffic_status"],
            "baseline unavailable",
        )

    def test_s3_json_is_loaded(
        self,
    ) -> None:
        client = FakeS3Client(
            self.speed_document
        )

        result = load_s3_json(
            bucket="test-bucket",
            key="speed/latest.json",
            s3_client=client,
        )

        self.assertEqual(
            result,
            self.speed_document,
        )

        self.assertEqual(
            client.calls,
            [
                {
                    "Bucket": "test-bucket",
                    "Key": "speed/latest.json",
                }
            ],
        )


if __name__ == "__main__":
    unittest.main()

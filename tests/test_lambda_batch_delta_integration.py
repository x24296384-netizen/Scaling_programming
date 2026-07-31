"""Integration tests for Lambda immutable batch-delta persistence."""

from __future__ import annotations

import base64
import json
import unittest
from unittest.mock import patch

from speed.lambda_handler import (
    lambda_handler,
    reset_analytics,
)


class FakeContext:
    """Minimal replacement for the AWS Lambda context."""

    aws_request_id = "request-123"


def build_lambda_record(
    *,
    sequence_number: str,
    status_code: int = 200,
) -> dict:
    """Create one valid Lambda Kinesis record."""

    event = {
        "client_ip": "10.0.0.1",
        "timestamp": (
            "2026-07-31T02:16:00+00:00"
        ),
        "method": "GET",
        "endpoint": "/api/delta-test",
        "protocol": "HTTP/1.1",
        "status_code": status_code,
        "response_bytes": 250,
        "referrer": "-",
        "user_agent": "delta-test",
        "event_id": (
            f"event-{sequence_number}"
        ),
        "ingested_at": (
            "2026-07-31T02:16:01+00:00"
        ),
        "source": "nginx_access_log",
    }

    encoded = base64.b64encode(
        json.dumps(event).encode(
            "utf-8"
        )
    ).decode("ascii")

    return {
        "kinesis": {
            "data": encoded,
            "sequenceNumber": (
                sequence_number
            ),
        }
    }


class TestLambdaBatchDeltaIntegration(
    unittest.TestCase
):
    """Validate Lambda-to-delta behaviour."""

    def setUp(self) -> None:
        reset_analytics(
            window_seconds=300
        )

    def test_valid_events_are_persisted_as_delta(
        self,
    ) -> None:
        delta_result = {
            "enabled": True,
            "stored": True,
            "bucket": "test-bucket",
            "key": (
                "speed/batches/test.json"
            ),
            "record_count": 2,
        }

        snapshot_result = {
            "enabled": False,
            "stored": False,
        }

        with patch(
            "speed.lambda_handler."
            "persist_batch_delta",
            return_value=delta_result,
        ) as mocked_delta:
            with patch(
                "speed.lambda_handler."
                "persist_speed_snapshot",
                return_value=(
                    snapshot_result
                ),
            ):
                result = lambda_handler(
                    {
                        "Records": [
                            build_lambda_record(
                                sequence_number=(
                                    "1001"
                                )
                            ),
                            build_lambda_record(
                                sequence_number=(
                                    "1002"
                                ),
                                status_code=500,
                            ),
                        ]
                    },
                    FakeContext(),
                )

        mocked_delta.assert_called_once()

        arguments = (
            mocked_delta.call_args.kwargs
        )

        self.assertEqual(
            arguments["request_id"],
            "request-123",
        )

        self.assertEqual(
            len(arguments["events"]),
            2,
        )

        self.assertEqual(
            arguments["events"][0][
                "sequence_number"
            ],
            "1001",
        )

        self.assertEqual(
            result["batchItemFailures"],
            [],
        )

        self.assertEqual(
            result["delta_persistence"],
            delta_result,
        )

    def test_delta_failure_requests_kinesis_retry(
        self,
    ) -> None:
        with patch(
            "speed.lambda_handler."
            "persist_batch_delta",
            side_effect=RuntimeError(
                "S3 delta write failed"
            ),
        ):
            with patch(
                "speed.lambda_handler."
                "persist_speed_snapshot",
                return_value={
                    "enabled": False,
                    "stored": False,
                },
            ):
                with patch(
                    "speed.lambda_handler."
                    "LOGGER.exception"
                ):
                    result = lambda_handler(
                        {
                            "Records": [
                                build_lambda_record(
                                    sequence_number=(
                                        "2001"
                                    )
                                ),
                                build_lambda_record(
                                    sequence_number=(
                                        "2002"
                                    )
                                ),
                            ]
                        },
                        FakeContext(),
                    )

        failures = {
            item["itemIdentifier"]
            for item in result[
                "batchItemFailures"
            ]
        }

        self.assertEqual(
            failures,
            {
                "2001",
                "2002",
            },
        )

        self.assertFalse(
            result[
                "delta_persistence"
            ]["stored"]
        )


if __name__ == "__main__":
    unittest.main()

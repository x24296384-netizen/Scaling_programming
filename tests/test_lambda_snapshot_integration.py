"""Integration tests between the Lambda handler and snapshot store."""

from __future__ import annotations

import unittest
from unittest.mock import patch

from speed.lambda_handler import (
    lambda_handler,
    reset_analytics,
)


class TestLambdaSnapshotIntegration(
    unittest.TestCase
):
    """Validate persistence without contacting real AWS services."""

    def setUp(self) -> None:
        reset_analytics(
            window_seconds=300
        )

    def test_handler_attaches_persistence_result(
        self,
    ) -> None:
        expected_persistence = {
            "enabled": True,
            "stored": True,
            "bucket": (
                "scp-speed-results-25186396"
            ),
            "key": (
                "speed/latest_snapshot.json"
            ),
            "bytes_written": 500,
            "etag": '"test-etag"',
        }

        with patch(
            "speed.lambda_handler."
            "persist_speed_snapshot",
            return_value=expected_persistence,
        ) as mocked_persist:
            result = lambda_handler(
                {
                    "Records": [],
                },
                None,
            )

        mocked_persist.assert_called_once()

        persistence_arguments = (
            mocked_persist.call_args.kwargs
        )

        self.assertEqual(
            persistence_arguments[
                "summary"
            ]["received_records"],
            0,
        )
        self.assertEqual(
            persistence_arguments[
                "summary"
            ]["processed_records"],
            0,
        )
        self.assertEqual(
            persistence_arguments[
                "anomalies"
            ],
            [],
        )

        self.assertEqual(
            result["persistence"],
            expected_persistence,
        )
        self.assertTrue(
            result["persistence"]["stored"]
        )

    def test_s3_failure_does_not_fail_kinesis_batch(
        self,
    ) -> None:
        with patch(
            "speed.lambda_handler."
            "persist_speed_snapshot",
            side_effect=RuntimeError(
                "S3 temporarily unavailable"
            ),
        ):
            with patch(
                "speed.lambda_handler."
                "LOGGER.exception"
            ) as mocked_log:
                result = lambda_handler(
                    {
                        "Records": [],
                    },
                    None,
                )

        mocked_log.assert_called_once()

        self.assertEqual(
            result["batchItemFailures"],
            [],
        )
        self.assertTrue(
            result["persistence"]["enabled"]
        )
        self.assertFalse(
            result["persistence"]["stored"]
        )
        self.assertEqual(
            result["persistence"]["reason"],
            "RuntimeError",
        )


if __name__ == "__main__":
    unittest.main()

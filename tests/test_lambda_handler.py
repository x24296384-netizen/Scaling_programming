import base64
import json
import unittest

from speed.lambda_handler import (
    convert_lambda_kinesis_record,
    lambda_handler,
    reset_analytics,
)


def make_event(
    timestamp: str,
    endpoint: str,
    status_code: int,
    response_bytes: int = 100,
) -> dict:
    """Create one valid shared-schema event."""

    return {
        "client_ip": "127.0.0.1",
        "timestamp": timestamp,
        "method": "GET",
        "endpoint": endpoint,
        "protocol": "HTTP/1.1",
        "status_code": status_code,
        "response_bytes": response_bytes,
        "referrer": "-",
        "user_agent": "lambda-unit-test",
        "event_id": "event-test",
        "ingested_at": (
            "2026-07-30T18:00:00+00:00"
        ),
        "source": "nginx_access_log",
    }


def make_lambda_record(
    event: dict,
    sequence_number: str,
) -> dict:
    """Create the structure received from Kinesis by Lambda."""

    payload = json.dumps(event).encode(
        "utf-8"
    )

    encoded_payload = base64.b64encode(
        payload
    ).decode("ascii")

    return {
        "eventID": (
            f"shardId-000:{sequence_number}"
        ),
        "eventSource": "aws:kinesis",
        "kinesis": {
            "partitionKey": event["client_ip"],
            "sequenceNumber": sequence_number,
            "data": encoded_payload,
        },
    }


class TestLambdaHandler(unittest.TestCase):

    def setUp(self):
        """Start every test with an empty window."""

        reset_analytics(
            window_seconds=300,
        )

    def test_lambda_record_is_converted_for_consumer(self):
        """Base64 Lambda data should become consumer bytes."""

        event = make_event(
            "2026-07-30T10:00:00+00:00",
            "/health",
            200,
        )

        lambda_record = make_lambda_record(
            event,
            "100",
        )

        converted = convert_lambda_kinesis_record(
            lambda_record
        )

        self.assertIsNotNone(converted)

        decoded_event = json.loads(
            converted["Data"].decode("utf-8")
        )

        self.assertEqual(
            decoded_event,
            event,
        )

    def test_handler_processes_valid_batch(self):
        """Valid Kinesis records should update the window."""

        first_event = make_event(
            "2026-07-30T10:00:00+00:00",
            "/api/users",
            200,
            150,
        )

        second_event = make_event(
            "2026-07-30T10:01:00+00:00",
            "/api/login",
            404,
            50,
        )

        result = lambda_handler(
            {
                "Records": [
                    make_lambda_record(
                        first_event,
                        "101",
                    ),
                    make_lambda_record(
                        second_event,
                        "102",
                    ),
                ]
            },
            None,
        )

        self.assertEqual(
            result["summary"]["received_records"],
            2,
        )

        self.assertEqual(
            result["summary"]["processed_records"],
            2,
        )

        self.assertEqual(
            result["summary"]["invalid_records"],
            0,
        )

        self.assertEqual(
            result["batchItemFailures"],
            [],
        )

        self.assertEqual(
            result["snapshot"]["total_response_bytes"],
            200,
        )

    def test_invalid_record_is_reported_without_stopping_batch(self):
        """One malformed record should not block valid records."""

        valid_event = make_event(
            "2026-07-30T10:00:00+00:00",
            "/health",
            200,
        )

        invalid_record = {
            "eventID": "invalid-event",
            "kinesis": {
                "partitionKey": "127.0.0.1",
                "sequenceNumber": "202",
                "data": "not-valid-base64***",
            },
        }

        result = lambda_handler(
            {
                "Records": [
                    make_lambda_record(
                        valid_event,
                        "201",
                    ),
                    invalid_record,
                ]
            },
            None,
        )

        self.assertEqual(
            result["summary"]["processed_records"],
            1,
        )

        self.assertEqual(
            result["summary"]["invalid_records"],
            1,
        )

        self.assertEqual(
            result["batchItemFailures"],
            [
                {
                    "itemIdentifier": "202",
                }
            ],
        )

    def test_high_error_rate_creates_anomaly(self):
        """Repeated failures should create an endpoint anomaly."""

        first_error = make_event(
            "2026-07-30T10:00:00+00:00",
            "/api/login",
            500,
        )

        second_error = make_event(
            "2026-07-30T10:00:30+00:00",
            "/api/login",
            404,
        )

        result = lambda_handler(
            {
                "Records": [
                    make_lambda_record(
                        first_error,
                        "301",
                    ),
                    make_lambda_record(
                        second_error,
                        "302",
                    ),
                ]
            },
            None,
        )

        self.assertEqual(
            result["summary"]["anomaly_count"],
            1,
        )

        self.assertEqual(
            result["anomalies"][0]["endpoint"],
            "/api/login",
        )

        self.assertEqual(
            result["anomalies"][0]["error_count"],
            2,
        )

        self.assertAlmostEqual(
            result["anomalies"][0]["error_rate"],
            1.0,
        )

    def test_invalid_event_fields_are_reported(self):
        """Valid JSON with missing fields should still fail safely."""

        invalid_event = {
            "timestamp": (
                "2026-07-30T10:00:00+00:00"
            ),
            "status_code": 200,
            "response_bytes": 10,
        }

        result = lambda_handler(
            {
                "Records": [
                    make_lambda_record(
                        {
                            **invalid_event,
                            "client_ip": "127.0.0.1",
                        },
                        "401",
                    )
                ]
            },
            None,
        )

        self.assertEqual(
            result["summary"]["processed_records"],
            0,
        )

        self.assertEqual(
            result["summary"]["invalid_records"],
            1,
        )

        self.assertEqual(
            result["batchItemFailures"],
            [
                {
                    "itemIdentifier": "401",
                }
            ],
        )


if __name__ == "__main__":
    unittest.main()
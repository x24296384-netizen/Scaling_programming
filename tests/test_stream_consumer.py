import json
import unittest

from speed.stream_consumer import (
    decode_kinesis_record,
    process_kinesis_record,
)
from speed.window_analytics import SlidingWindowAnalytics


SAMPLE_EVENT = {
    "client_ip": "127.0.0.1",
    "timestamp": "2026-07-29T12:00:00+00:00",
    "method": "GET",
    "endpoint": "/api/login",
    "protocol": "HTTP/1.1",
    "status_code": 500,
    "response_bytes": 1234,
    "referrer": "-",
    "user_agent": "unit-test",
    "event_id": "event-001",
    "ingested_at": "2026-07-30T00:00:00+00:00",
    "source": "nginx_access_log",
}


def make_kinesis_record(event: dict) -> dict:
    """Create a simulated record returned by Amazon Kinesis."""

    return {
        "Data": json.dumps(event).encode("utf-8"),
        "PartitionKey": event["client_ip"],
        "SequenceNumber": "1",
    }


class TestStreamConsumer(unittest.TestCase):

    def test_decode_valid_kinesis_record(self):
        """A valid Kinesis payload should become a Python dictionary."""

        record = make_kinesis_record(SAMPLE_EVENT)

        decoded = decode_kinesis_record(record)

        self.assertEqual(decoded, SAMPLE_EVENT)

    def test_invalid_json_returns_none(self):
        """Malformed JSON should be rejected without stopping the consumer."""

        record = {
            "Data": b"not-valid-json",
        }

        decoded = decode_kinesis_record(record)

        self.assertIsNone(decoded)

    def test_process_record_updates_sliding_window(self):
        """A valid record should update the incremental analytics."""

        analytics = SlidingWindowAnalytics(
            window_seconds=60,
        )

        record = make_kinesis_record(SAMPLE_EVENT)

        processed = process_kinesis_record(
            record=record,
            analytics=analytics,
        )

        snapshot = analytics.snapshot()

        self.assertTrue(processed)
        self.assertEqual(
            snapshot["window_event_count"],
            1,
        )
        self.assertEqual(
            snapshot["requests_per_endpoint"],
            {
                "/api/login": 1,
            },
        )
        self.assertEqual(
            snapshot["status_code_distribution"],
            {
                500: 1,
            },
        )
        self.assertEqual(
            snapshot["error_rates"]["/api/login"]["error_count"],
            1,
        )


if __name__ == "__main__":
    unittest.main()

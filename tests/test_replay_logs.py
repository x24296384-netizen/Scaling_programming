import json
import unittest
from datetime import datetime
from uuid import UUID

# Functions from the Kinesis replay module that will be tested.
from producer.replay_logs import (
    build_kinesis_record,
    prepare_kinesis_records,
    replay_lines,
    replay_file,
    send_batch_with_retries,
)


# Example parsed event used by several tests.
SAMPLE_EVENT = {
    "client_ip": "127.0.0.1",
    "timestamp": "2026-07-27T12:00:00+00:00",
    "method": "GET",
    "endpoint": "/index.html",
    "protocol": "HTTP/1.1",
    "status_code": 200,
    "response_bytes": 1234,
    "referrer": "-",
    "user_agent": "Test Agent",
}


class FakeKinesisClient:
    """
    Simple fake Kinesis client.

    It allows retry behaviour to be tested without connecting to AWS.
    """

    def __init__(self, responses):
        # Convert the prepared responses into an iterator.
        self.responses = iter(responses)

        # Store each put_records request for later verification.
        self.calls = []

    def put_records(self, **kwargs):
        # Record the request made by the replay function.
        self.calls.append(kwargs)

        # Return the next prepared Kinesis response.
        return next(self.responses)


class TestReplayLogs(unittest.TestCase):

    def test_build_kinesis_record(self):
        """A parsed event should become a valid Kinesis record."""

        record = build_kinesis_record(SAMPLE_EVENT)

        # The client IP should be used as the partition key.
        self.assertEqual(
            record["PartitionKey"],
            "127.0.0.1",
        )

        # Kinesis data is stored as bytes, so decode it back to JSON.
        decoded_event = json.loads(
            record["Data"].decode("utf-8")
        )

        # All frozen base fields must remain unchanged.
        for field_name, expected_value in SAMPLE_EVENT.items():
            self.assertEqual(
                decoded_event[field_name],
                expected_value,
            )

        # The replay path must add streaming metadata.
        self.assertEqual(
            decoded_event["source"],
            "nginx_access_log",
        )

        # event_id must be a valid UUID string.
        UUID(decoded_event["event_id"])

        # ingested_at must be a timezone-aware ISO 8601 timestamp.
        ingested_at = datetime.fromisoformat(
            decoded_event["ingested_at"]
        )
        self.assertIsNotNone(
            ingested_at.tzinfo
        )

        # Confirm the complete frozen streaming schema.
        self.assertEqual(
            set(decoded_event.keys()),
            {
                "client_ip",
                "timestamp",
                "method",
                "endpoint",
                "protocol",
                "status_code",
                "response_bytes",
                "referrer",
                "user_agent",
                "event_id",
                "ingested_at",
                "source",
            },
        )

    def test_failed_record_is_retried(self):
        """Only failed records should be sent again."""

        first_event = SAMPLE_EVENT.copy()

        second_event = SAMPLE_EVENT.copy()
        second_event["client_ip"] = "127.0.0.2"
        second_event["endpoint"] = "/api/login"

        records = [
            build_kinesis_record(first_event),
            build_kinesis_record(second_event),
        ]

        # First response:
        # - the first record succeeds;
        # - the second record fails.
        #
        # Second response:
        # - the retried record succeeds.
        client = FakeKinesisClient(
            responses=[
                {
                    "FailedRecordCount": 1,
                    "Records": [
                        {
                            "ShardId": "shard-01",
                            "SequenceNumber": "1",
                        },
                        {
                            "ErrorCode": (
                                "ProvisionedThroughputExceededException"
                            ),
                            "ErrorMessage": "Temporary failure",
                        },
                    ],
                },
                {
                    "FailedRecordCount": 0,
                    "Records": [
                        {
                            "ShardId": "shard-01",
                            "SequenceNumber": "2",
                        },
                    ],
                },
            ]
        )

        successful, failed = send_batch_with_retries(
            client=client,
            stream_name="access-log-stream",
            records=records,
            max_attempts=3,
            retry_delay=0,
        )

        # Both records should eventually succeed.
        self.assertEqual(successful, 2)
        self.assertEqual(failed, 0)

        # One original request and one retry should be made.
        self.assertEqual(len(client.calls), 2)

        first_request = client.calls[0]["Records"]
        retry_request = client.calls[1]["Records"]

        # The first request should contain both records.
        self.assertEqual(len(first_request), 2)

        # The retry should contain only the failed record.
        self.assertEqual(len(retry_request), 1)

        self.assertEqual(
            retry_request[0]["PartitionKey"],
            "127.0.0.2",
        )

    def test_permanent_failure_is_reported(self):
        """Records that fail after all retries should be reported."""

        record = build_kinesis_record(SAMPLE_EVENT)

        # Simulate the same record failing on every attempt.
        failure_response = {
            "FailedRecordCount": 1,
            "Records": [
                {
                    "ErrorCode": (
                        "ProvisionedThroughputExceededException"
                    ),
                    "ErrorMessage": "Temporary failure",
                },
            ],
        }

        client = FakeKinesisClient(
            responses=[
                failure_response,
                failure_response,
                failure_response,
            ]
        )

        successful, failed = send_batch_with_retries(
            client=client,
            stream_name="access-log-stream",
            records=[record],
            max_attempts=3,
            retry_delay=0,
        )

        # The record never succeeds and must be reported as failed.
        self.assertEqual(successful, 0)
        self.assertEqual(failed, 1)

        # The initial attempt plus two retries should be made.
        self.assertEqual(len(client.calls), 3)

    def test_prepare_records_skips_invalid_lines(self):
        """Valid log lines become records while invalid lines are counted."""

        lines = [
            (
                '54.63.149.41 - - [10/Oct/2000:13:55:36 -0700] '
                '"GET /filter/test HTTP/1.1" 200 1234 "-" '
                '"Mozilla/5.0" "-"'
            ),
            "This is not a valid access-log line",
            (
                '192.168.1.10 - - [11/Oct/2000:09:15:22 +0000] '
                '"POST /api/login HTTP/1.1" 500 450 '
                '"https://example.com" "TestAgent/1.0" "-"'
            ),
        ]

        # Parse the lines and create Kinesis records for valid events.
        records, invalid_count = prepare_kinesis_records(lines)

        # Two valid lines should produce two records.
        self.assertEqual(len(records), 2)

        # One malformed line should be counted and skipped.
        self.assertEqual(invalid_count, 1)

        # Decode the Kinesis payloads to inspect their event fields.
        first_event = json.loads(
            records[0]["Data"].decode("utf-8")
        )
        second_event = json.loads(
            records[1]["Data"].decode("utf-8")
        )

        self.assertEqual(
            first_event["endpoint"],
            "/filter/test",
        )
        self.assertEqual(
            second_event["endpoint"],
            "/api/login",
        )
        self.assertEqual(
            second_event["status_code"],
            500,
        )

    def test_replay_lines_sends_multiple_batches(self):
        """Three valid lines with batch size two should create two batches."""

        lines = [
            (
                '54.63.149.41 - - [10/Oct/2000:13:55:36 -0700] '
                '"GET /first HTTP/1.1" 200 100 "-" '
                '"Mozilla/5.0" "-"'
            ),
            (
                '192.168.1.10 - - [11/Oct/2000:09:15:22 +0000] '
                '"POST /second HTTP/1.1" 201 200 "-" '
                '"TestAgent/1.0" "-"'
            ),
            (
                '10.0.0.25 - - [11/Oct/2000:09:16:00 +0000] '
                '"GET /third HTTP/1.1" 404 300 "-" '
                '"Mozilla/5.0" "-"'
            ),
        ]

        # The first Kinesis request receives two records.
        # The final partial batch receives one record.
        client = FakeKinesisClient(
            responses=[
                {
                    "FailedRecordCount": 0,
                    "Records": [
                        {
                            "ShardId": "shard-01",
                            "SequenceNumber": "1",
                        },
                        {
                            "ShardId": "shard-01",
                            "SequenceNumber": "2",
                        },
                    ],
                },
                {
                    "FailedRecordCount": 0,
                    "Records": [
                        {
                            "ShardId": "shard-01",
                            "SequenceNumber": "3",
                        },
                    ],
                },
            ]
        )

        statistics = replay_lines(
            client=client,
            stream_name="access-log-stream",
            lines=lines,
            batch_size=2,
            max_attempts=3,
            retry_delay=0,
        )

        # Three valid lines should be sent successfully.
        self.assertEqual(statistics["total_lines"], 3)
        self.assertEqual(statistics["valid_records"], 3)
        self.assertEqual(statistics["invalid_lines"], 0)
        self.assertEqual(statistics["successful_records"], 3)
        self.assertEqual(statistics["failed_records"], 0)

        # Batch size two means one full batch and one partial batch.
        self.assertEqual(statistics["batches_sent"], 2)
        self.assertEqual(len(client.calls), 2)

        self.assertEqual(
            len(client.calls[0]["Records"]),
            2,
        )
        self.assertEqual(
            len(client.calls[1]["Records"]),
            1,
        )

    def test_replay_file_reads_fixture_and_sends_records(self):
        """The fixture file should be read and sent in two batches."""

        # The fixture contains three valid access-log lines.
        # With batch size two, Kinesis receives two requests.
        client = FakeKinesisClient(
            responses=[
                {
                    "FailedRecordCount": 0,
                    "Records": [
                        {
                            "ShardId": "shard-01",
                            "SequenceNumber": "1",
                        },
                        {
                            "ShardId": "shard-01",
                            "SequenceNumber": "2",
                        },
                    ],
                },
                {
                    "FailedRecordCount": 0,
                    "Records": [
                        {
                            "ShardId": "shard-01",
                            "SequenceNumber": "3",
                        },
                    ],
                },
            ]
        )

        statistics = replay_file(
            client=client,
            stream_name="access-log-stream",
            input_path="tests/fixtures/sample_access.log",
            batch_size=2,
            max_attempts=3,
            retry_delay=0,
        )

        # Confirm that all fixture records were processed successfully.
        self.assertEqual(statistics["total_lines"], 3)
        self.assertEqual(statistics["valid_records"], 3)
        self.assertEqual(statistics["invalid_lines"], 0)
        self.assertEqual(statistics["successful_records"], 3)
        self.assertEqual(statistics["failed_records"], 0)
        self.assertEqual(statistics["batches_sent"], 2)

        # Confirm the expected full and partial batch sizes.
        self.assertEqual(len(client.calls), 2)
        self.assertEqual(
            len(client.calls[0]["Records"]),
            2,
        )
        self.assertEqual(
            len(client.calls[1]["Records"]),
            1,
        )

# Allow the tests to run when this file is executed directly.
if __name__ == "__main__":
    unittest.main()

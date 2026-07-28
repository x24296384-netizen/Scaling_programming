import json
import unittest

from producer.replay_logs import (
    build_kinesis_record,
    send_batch_with_retries,
)


SAMPLE_EVENT = {
    "client_ip": "127.0.0.1",
    "timestamp": "2026-07-27T12:00:00+00:00",
    "method": "GET",
    "resource": "/index.html",
    "protocol": "HTTP/1.1",
    "status_code": 200,
    "response_bytes": 1234,
    "referrer": "-",
    "user_agent": "Test Agent",
    "extra": "-",
}


class FakeKinesisClient:
    """Fake Kinesis client used without connecting to AWS."""

    def __init__(self, responses):
        self.responses = iter(responses)
        self.calls = []

    def put_records(self, **kwargs):
        self.calls.append(kwargs)
        return next(self.responses)


class TestReplayLogs(unittest.TestCase):

    def test_build_kinesis_record(self):
        """A parsed event should become a valid Kinesis record."""

        record = build_kinesis_record(SAMPLE_EVENT)

        self.assertEqual(
            record["PartitionKey"],
            "127.0.0.1",
        )

        decoded_event = json.loads(
            record["Data"].decode("utf-8")
        )

        self.assertEqual(decoded_event, SAMPLE_EVENT)

    def test_failed_record_is_retried(self):
        """Only failed records should be sent again."""

        first_event = SAMPLE_EVENT.copy()

        second_event = SAMPLE_EVENT.copy()
        second_event["client_ip"] = "127.0.0.2"
        second_event["resource"] = "/api/login"

        records = [
            build_kinesis_record(first_event),
            build_kinesis_record(second_event),
        ]

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

        self.assertEqual(successful, 2)
        self.assertEqual(failed, 0)
        self.assertEqual(len(client.calls), 2)

        first_request = client.calls[0]["Records"]
        retry_request = client.calls[1]["Records"]

        self.assertEqual(len(first_request), 2)
        self.assertEqual(len(retry_request), 1)

        self.assertEqual(
            retry_request[0]["PartitionKey"],
            "127.0.0.2",
        )


if __name__ == "__main__":
    unittest.main()
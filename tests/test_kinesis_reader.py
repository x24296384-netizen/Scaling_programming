"""Tests for the reusable Kinesis reader."""

import unittest

from speed.kinesis_reader import (
    get_first_shard_id,
    read_from_stream,
)


class FakeKinesisClient:
    """Simulate the Kinesis API without contacting AWS."""

    def __init__(self):
        self.read_calls = 0
        self.iterator_type = None

    def list_shards(self, StreamName):
        return {
            "Shards": [
                {
                    "ShardId": "shardId-000000000000"
                }
            ]
        }

    def get_shard_iterator(
        self,
        StreamName,
        ShardId,
        ShardIteratorType,
    ):
        self.iterator_type = ShardIteratorType

        return {
            "ShardIterator": "iterator-0"
        }

    def get_records(
        self,
        ShardIterator,
        Limit,
    ):
        self.read_calls += 1

        # Simulate an initially empty Kinesis response.
        if self.read_calls == 1:
            return {
                "Records": [],
                "NextShardIterator": "iterator-1",
            }

        return {
            "Records": [
                {
                    "Data": b'{"event_id": "1"}',
                    "SequenceNumber": "1",
                },
                {
                    "Data": b'{"event_id": "2"}',
                    "SequenceNumber": "2",
                },
            ],
            "NextShardIterator": "iterator-2",
        }


class EmptyKinesisClient:
    """Simulate a stream containing no available shards."""

    def list_shards(self, StreamName):
        return {
            "Shards": []
        }


class TestKinesisReader(unittest.TestCase):

    def test_reader_retries_empty_response_and_returns_records(self):
        """The reader should continue after an empty response."""

        client = FakeKinesisClient()
        sleep_calls = []

        results = read_from_stream(
            client=client,
            stream_name="test-stream",
            expected_records=2,
            iterator_type="LATEST",
            max_attempts=3,
            sleep_seconds=0.5,
            sleeper=sleep_calls.append,
        )

        self.assertEqual(
            results["records_received"],
            2,
        )
        self.assertEqual(
            results["read_attempts"],
            2,
        )
        self.assertEqual(
            results["shard_id"],
            "shardId-000000000000",
        )
        self.assertEqual(
            results["iterator_type"],
            "LATEST",
        )
        self.assertEqual(
            client.iterator_type,
            "LATEST",
        )
        self.assertEqual(
            sleep_calls,
            [0.5],
        )

    def test_missing_shard_is_reported(self):
        """A stream without shards should raise a clear error."""

        with self.assertRaises(ValueError):
            get_first_shard_id(
                client=EmptyKinesisClient(),
                stream_name="empty-stream",
            )


if __name__ == "__main__":
    unittest.main()

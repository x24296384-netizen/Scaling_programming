import json
import unittest
from datetime import datetime, timezone

from speed.stream_metrics import benchmark_stream_records
from speed.window_analytics import SlidingWindowAnalytics


def make_record(
    event_id: str,
    endpoint: str,
    ingested_at: str,
    status_code: int = 200,
) -> dict:
    """Create one simulated Kinesis record for metrics testing."""

    event = {
        "client_ip": "127.0.0.1",
        "timestamp": "2026-07-30T00:00:00+00:00",
        "method": "GET",
        "endpoint": endpoint,
        "protocol": "HTTP/1.1",
        "status_code": status_code,
        "response_bytes": 100,
        "referrer": "-",
        "user_agent": "unit-test",
        "event_id": event_id,
        "ingested_at": ingested_at,
        "source": "nginx_access_log",
    }

    return {
        "Data": json.dumps(event).encode("utf-8"),
        "PartitionKey": event["client_ip"],
        "SequenceNumber": event_id,
    }


class FakeTimer:
    """Return fixed values so runtime and throughput are predictable."""

    def __init__(self):
        self.values = iter([100.0, 102.0])

    def __call__(self):
        return next(self.values)


class FakeClock:
    """Return fixed processing times for latency calculations."""

    def __init__(self):
        self.values = iter(
            [
                datetime(
                    2026, 7, 30, 0, 0, 0, 100000,
                    tzinfo=timezone.utc,
                ),
                datetime(
                    2026, 7, 30, 0, 0, 0, 200000,
                    tzinfo=timezone.utc,
                ),
                datetime(
                    2026, 7, 30, 0, 0, 0, 300000,
                    tzinfo=timezone.utc,
                ),
            ]
        )

    def __call__(self):
        return next(self.values)


class TestStreamMetrics(unittest.TestCase):

    def test_benchmark_records_calculates_counts_and_performance(self):
        """Counts, throughput and latency should be calculated correctly."""

        records = [
            make_record(
                event_id="1",
                endpoint="/home",
                ingested_at="2026-07-30T00:00:00+00:00",
            ),
            make_record(
                event_id="2",
                endpoint="/api/login",
                ingested_at="2026-07-30T00:00:00+00:00",
                status_code=500,
            ),
            make_record(
                event_id="3",
                endpoint="/images/logo.png",
                ingested_at="2026-07-30T00:00:00+00:00",
            ),
            {
                "Data": b"not-valid-json",
                "SequenceNumber": "invalid",
            },
        ]

        analytics = SlidingWindowAnalytics(
            window_seconds=60,
        )

        results = benchmark_stream_records(
            records=records,
            analytics=analytics,
            timer=FakeTimer(),
            clock=FakeClock(),
        )

        self.assertEqual(
            results["records_attempted"],
            4,
        )
        self.assertEqual(
            results["records_processed"],
            3,
        )
        self.assertEqual(
            results["invalid_records"],
            1,
        )

        # The fake runtime is exactly two seconds.
        self.assertAlmostEqual(
            results["runtime_seconds"],
            2.0,
        )

        # Three successfully processed records over two seconds.
        self.assertAlmostEqual(
            results["throughput_records_per_second"],
            1.5,
        )

        # Fixed latency samples: 100 ms, 200 ms and 300 ms.
        self.assertAlmostEqual(
            results["latency_min_ms"],
            100.0,
        )
        self.assertAlmostEqual(
            results["latency_mean_ms"],
            200.0,
        )
        self.assertAlmostEqual(
            results["latency_p50_ms"],
            200.0,
        )
        self.assertAlmostEqual(
            results["latency_p95_ms"],
            290.0,
        )
        self.assertAlmostEqual(
            results["latency_max_ms"],
            300.0,
        )
        self.assertEqual(
            results["latency_samples"],
            3,
        )

        snapshot = analytics.snapshot()

        self.assertEqual(
            snapshot["window_event_count"],
            3,
        )


if __name__ == "__main__":
    unittest.main()

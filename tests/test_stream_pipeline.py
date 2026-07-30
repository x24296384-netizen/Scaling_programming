"""Local integration test for the complete streaming path."""

import unittest
from pathlib import Path

from producer.log_parser import parse_log_line
from producer.replay_logs import build_kinesis_record
from speed.stream_consumer import process_kinesis_record
from speed.window_analytics import SlidingWindowAnalytics


FIXTURE_PATH = (
    Path(__file__).parent
    / "fixtures"
    / "sample_access.log"
)


class TestLocalStreamPipeline(unittest.TestCase):

    def test_fixture_records_reach_sliding_window(self):
        """
        Real fixture lines should pass through the complete local path.
        """

        # Use a large window so all three sample records remain available.
        analytics = SlidingWindowAnalytics(
            window_seconds=10 * 365 * 24 * 60 * 60,
        )

        processed_records = 0

        with FIXTURE_PATH.open(
            mode="r",
            encoding="utf-8",
            errors="replace",
        ) as log_file:

            for line in log_file:
                event = parse_log_line(line)

                # The fixture contains three valid Nginx records.
                self.assertIsNotNone(event)

                kinesis_record = build_kinesis_record(
                    event
                )

                processed = process_kinesis_record(
                    record=kinesis_record,
                    analytics=analytics,
                )

                if processed:
                    processed_records += 1

        snapshot = analytics.snapshot()

        self.assertEqual(
            processed_records,
            3,
        )
        self.assertEqual(
            snapshot["window_event_count"],
            3,
        )
        self.assertEqual(
            sum(
                snapshot[
                    "requests_per_endpoint"
                ].values()
            ),
            3,
        )
        self.assertEqual(
            sum(
                snapshot[
                    "status_code_distribution"
                ].values()
            ),
            3,
        )


if __name__ == "__main__":
    unittest.main()

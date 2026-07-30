import unittest

from speed.window_analytics import SlidingWindowAnalytics


def make_event(
    timestamp: str,
    endpoint: str,
    status_code: int,
) -> dict:
    """Create a minimal valid streaming event for testing."""

    return {
        "client_ip": "127.0.0.1",
        "timestamp": timestamp,
        "method": "GET",
        "endpoint": endpoint,
        "protocol": "HTTP/1.1",
        "status_code": status_code,
        "response_bytes": 100,
        "referrer": "-",
        "user_agent": "unit-test",
    }


class TestSlidingWindowAnalytics(unittest.TestCase):

    def test_incremental_metrics_and_window_eviction(self):
        """Metrics should update and expired events should be removed."""

        analytics = SlidingWindowAnalytics(
            window_seconds=60,
        )

        analytics.add_event(
            make_event(
                "2026-07-29T12:59:00+00:00",
                "/old",
                200,
            )
        )
        analytics.add_event(
            make_event(
                "2026-07-29T12:59:20+00:00",
                "/api/login",
                500,
            )
        )
        analytics.add_event(
            make_event(
                "2026-07-29T12:59:50+00:00",
                "/api/login",
                404,
            )
        )

        first_snapshot = analytics.snapshot()

        self.assertEqual(
            first_snapshot["window_event_count"],
            3,
        )
        self.assertEqual(
            first_snapshot["requests_per_endpoint"],
            {
                "/old": 1,
                "/api/login": 2,
            },
        )
        self.assertEqual(
            first_snapshot["status_code_distribution"],
            {
                200: 1,
                500: 1,
                404: 1,
            },
        )

        # The latest timestamp moves the window start to 12:59:10.
        # Therefore, the event at 12:59:00 must be removed.
        analytics.add_event(
            make_event(
                "2026-07-29T13:00:10+00:00",
                "/home",
                200,
            )
        )

        second_snapshot = analytics.snapshot()

        self.assertEqual(
            second_snapshot["window_event_count"],
            3,
        )
        self.assertEqual(
            second_snapshot["requests_per_endpoint"],
            {
                "/api/login": 2,
                "/home": 1,
            },
        )
        self.assertEqual(
            second_snapshot["traffic_by_hour"],
            {
                12: 2,
                13: 1,
            },
        )
        self.assertEqual(
            second_snapshot["status_code_distribution"],
            {
                500: 1,
                404: 1,
                200: 1,
            },
        )

        api_errors = second_snapshot["error_rates"]["/api/login"]
        home_errors = second_snapshot["error_rates"]["/home"]

        self.assertEqual(api_errors["total_requests"], 2)
        self.assertEqual(api_errors["error_count"], 2)
        self.assertAlmostEqual(api_errors["error_rate"], 1.0)

        self.assertEqual(home_errors["total_requests"], 1)
        self.assertEqual(home_errors["error_count"], 0)
        self.assertAlmostEqual(home_errors["error_rate"], 0.0)

        self.assertNotIn(
            "/old",
            second_snapshot["requests_per_endpoint"],
        )

    def test_response_byte_metrics_follow_window_eviction(self):
        """Response-byte totals should update when events expire."""

        analytics = SlidingWindowAnalytics(
            window_seconds=60,
        )

        analytics.add_event(
            {
                "timestamp": "2026-07-30T10:00:00+00:00",
                "endpoint": "/api/users",
                "status_code": 200,
                "response_bytes": 100,
            }
        )

        analytics.add_event(
            {
                "timestamp": "2026-07-30T10:00:30+00:00",
                "endpoint": "/api/users",
                "status_code": 200,
                "response_bytes": 50,
            }
        )

        first_snapshot = analytics.snapshot()

        self.assertEqual(
            first_snapshot["total_response_bytes"],
            150,
        )

        self.assertEqual(
            first_snapshot["response_byte_totals"],
            {
                "/api/users": 150,
            },
        )

        # Moving the watermark to 10:02 means that the
        # first two events are outside the 60-second window.
        analytics.add_event(
            {
                "timestamp": "2026-07-30T10:02:00+00:00",
                "endpoint": "/health",
                "status_code": 200,
                "response_bytes": 25,
            }
        )

        second_snapshot = analytics.snapshot()

        self.assertEqual(
            second_snapshot["total_response_bytes"],
            25,
        )

        self.assertEqual(
            second_snapshot["response_byte_totals"],
            {
                "/health": 25,
            },
        )

    def test_empty_window_snapshot(self):
        """A new window should contain no events or time boundaries."""

        analytics = SlidingWindowAnalytics(
            window_seconds=60,
        )

        snapshot = analytics.snapshot()

        self.assertEqual(
            snapshot["window_event_count"],
            0,
        )
        self.assertIsNone(
            snapshot["window_start"]
        )
        self.assertIsNone(
            snapshot["window_end"]
        )
        self.assertEqual(
            snapshot["requests_per_endpoint"],
            {},
        )
        self.assertEqual(
            snapshot["error_rates"],
            {},
        )
        self.assertEqual(
            snapshot["traffic_by_hour"],
            {},
        )
        self.assertEqual(
            snapshot["status_code_distribution"],
            {},
        )

    def test_out_of_order_event_inside_window_is_retained(self):
        """A late event should remain when it is still inside the window."""

        analytics = SlidingWindowAnalytics(
            window_seconds=60,
        )

        # The newest event arrives first.
        analytics.add_event(
            make_event(
                "2026-07-29T12:00:50+00:00",
                "/newest",
                200,
            )
        )

        # This event arrives later, but its event time is earlier.
        analytics.add_event(
            make_event(
                "2026-07-29T12:00:30+00:00",
                "/late",
                404,
            )
        )

        snapshot = analytics.snapshot()

        self.assertEqual(
            snapshot["window_event_count"],
            2,
        )
        self.assertEqual(
            snapshot["requests_per_endpoint"],
            {
                "/newest": 1,
                "/late": 1,
            },
        )
        self.assertEqual(
            snapshot["status_code_distribution"],
            {
                200: 1,
                404: 1,
            },
        )
        self.assertEqual(
            snapshot["window_end"],
            "2026-07-29T12:00:50+00:00",
        )

    def test_event_older_than_window_is_discarded(self):
        """An event older than the current cutoff should not remain."""

        analytics = SlidingWindowAnalytics(
            window_seconds=60,
        )

        analytics.add_event(
            make_event(
                "2026-07-29T13:01:10+00:00",
                "/current",
                200,
            )
        )

        # The current cutoff is 13:00:10, so this event is expired.
        analytics.add_event(
            make_event(
                "2026-07-29T13:00:00+00:00",
                "/expired",
                500,
            )
        )

        snapshot = analytics.snapshot()

        self.assertEqual(
            snapshot["window_event_count"],
            1,
        )
        self.assertEqual(
            snapshot["requests_per_endpoint"],
            {
                "/current": 1,
            },
        )
        self.assertNotIn(
            "/expired",
            snapshot["requests_per_endpoint"],
        )
        self.assertNotIn(
            500,
            snapshot["status_code_distribution"],
        )

    def test_window_lower_boundary_is_inclusive(self):
        """An event exactly at the cutoff should remain in the window."""

        analytics = SlidingWindowAnalytics(
            window_seconds=60,
        )

        analytics.add_event(
            make_event(
                "2026-07-29T14:01:00+00:00",
                "/latest",
                200,
            )
        )
        analytics.add_event(
            make_event(
                "2026-07-29T14:00:00+00:00",
                "/boundary",
                200,
            )
        )

        snapshot = analytics.snapshot()

        self.assertEqual(
            snapshot["window_event_count"],
            2,
        )
        self.assertEqual(
            snapshot["requests_per_endpoint"],
            {
                "/latest": 1,
                "/boundary": 1,
            },
        )

    def test_invalid_window_and_event_values_are_rejected(self):
        """Invalid configuration or event fields should raise ValueError."""

        with self.assertRaises(ValueError):
            SlidingWindowAnalytics(
                window_seconds=0,
            )

        analytics = SlidingWindowAnalytics(
            window_seconds=60,
        )

        invalid_timestamp = make_event(
            "not-a-timestamp",
            "/invalid-time",
            200,
        )

        with self.assertRaises(ValueError):
            analytics.add_event(
                invalid_timestamp
            )

        timestamp_without_timezone = make_event(
            "2026-07-29T12:00:00",
            "/missing-timezone",
            200,
        )

        with self.assertRaises(ValueError):
            analytics.add_event(
                timestamp_without_timezone
            )

        missing_endpoint = make_event(
            "2026-07-29T12:00:00+00:00",
            "",
            200,
        )

        with self.assertRaises(ValueError):
            analytics.add_event(
                missing_endpoint
            )

        invalid_status = make_event(
            "2026-07-29T12:00:00+00:00",
            "/invalid-status",
            200,
        )
        invalid_status["status_code"] = "invalid"

        with self.assertRaises(ValueError):
            analytics.add_event(
                invalid_status
            )

if __name__ == "__main__":
    unittest.main()


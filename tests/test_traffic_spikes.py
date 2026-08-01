"""Tests for endpoint RPM baseline comparison and spike detection."""

from __future__ import annotations

import unittest

from serving.traffic_spikes import (
    build_endpoint_traffic_view,
    compare_endpoint_traffic,
    extract_baseline_rpm,
)


class TestTrafficSpikes(unittest.TestCase):
    """Validate traffic-spike thresholds and supported baseline formats."""

    def test_extracts_mapping_nested_and_row_baselines(self) -> None:
        mapping = extract_baseline_rpm(
            {
                "/api/login": 3.5,
                "/api/users": {
                    "avg_requests_per_minute": 2.0,
                },
                "/invalid": -1,
            }
        )

        rows = extract_baseline_rpm(
            [
                {
                    "endpoint": "/images/logo.png",
                    "avg_requests_per_minute": 0.5,
                },
                {
                    "endpoint": "/health",
                    "baseline_rpm": 1.0,
                },
            ]
        )

        self.assertEqual(
            mapping,
            {
                "/api/login": 3.5,
                "/api/users": 2.0,
            },
        )
        self.assertEqual(
            rows,
            {
                "/images/logo.png": 0.5,
                "/health": 1.0,
            },
        )

    def test_detects_significant_increase(self) -> None:
        result = compare_endpoint_traffic(
            baseline_rpm=0.4,
            recent_requests=10,
            window_seconds=300,
            traffic_spike_ratio=2.0,
            minimum_requests=10,
        )

        self.assertEqual(result["recent_rpm"], 2.0)
        self.assertEqual(result["rpm_difference"], 1.6)
        self.assertEqual(result["recent_to_baseline_ratio"], 5.0)
        self.assertTrue(result["minimum_requests_met"])
        self.assertTrue(result["significant_increase"])
        self.assertEqual(result["traffic_status"], "significant increase")

    def test_minimum_request_guard_prevents_false_spike(self) -> None:
        result = compare_endpoint_traffic(
            baseline_rpm=0.1,
            recent_requests=5,
            window_seconds=300,
            traffic_spike_ratio=2.0,
            minimum_requests=10,
        )

        self.assertEqual(result["recent_to_baseline_ratio"], 10.0)
        self.assertFalse(result["minimum_requests_met"])
        self.assertFalse(result["significant_increase"])
        self.assertEqual(result["traffic_status"], "above baseline")

    def test_zero_baseline_new_traffic_can_be_significant(self) -> None:
        result = compare_endpoint_traffic(
            baseline_rpm=0.0,
            recent_requests=10,
            window_seconds=300,
            minimum_requests=10,
        )

        self.assertIsNone(result["recent_to_baseline_ratio"])
        self.assertTrue(result["significant_increase"])
        self.assertEqual(result["traffic_status"], "significant increase")

    def test_missing_baseline_is_explicit(self) -> None:
        result = compare_endpoint_traffic(
            baseline_rpm=None,
            recent_requests=10,
            window_seconds=300,
        )

        self.assertFalse(result["comparison_available"])
        self.assertFalse(result["significant_increase"])
        self.assertEqual(result["traffic_status"], "baseline unavailable")

    def test_builds_ranked_spike_summary(self) -> None:
        result = build_endpoint_traffic_view(
            baseline_rpm_by_endpoint={
                "/api/login": 0.5,
                "/api/users": 1.0,
                "/new": 0.0,
            },
            recent_requests_by_endpoint={
                "/api/login": 20,
                "/api/users": 5,
                "/new": 10,
            },
            window_seconds=300,
            traffic_spike_ratio=2.0,
            minimum_requests=10,
        )

        self.assertEqual(
            result["summary"]["significant_increase_count"],
            2,
        )
        self.assertEqual(
            result["summary"]["significant_increase_endpoints"],
            ["/new", "/api/login"],
        )
        self.assertEqual(
            [item["endpoint"] for item in result["spikes"]],
            ["/new", "/api/login"],
        )
        self.assertFalse(
            result["comparisons"]["/api/users"]["significant_increase"]
        )

    def test_rejects_invalid_thresholds(self) -> None:
        with self.assertRaises(ValueError):
            compare_endpoint_traffic(
                baseline_rpm=1.0,
                recent_requests=10,
                window_seconds=300,
                traffic_spike_ratio=1.0,
            )

        with self.assertRaises(ValueError):
            compare_endpoint_traffic(
                baseline_rpm=1.0,
                recent_requests=10,
                window_seconds=300,
                minimum_requests=0,
            )


if __name__ == "__main__":
    unittest.main()

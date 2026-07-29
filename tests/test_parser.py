"""Tests for the web access-log parser."""

import unittest

from producer.log_parser import parse_log_line


class TestLogParser(unittest.TestCase):
    """Automated tests for the log parser function."""

    def test_valid_log_line(self):
        """Test parsing a valid log line."""

        line = (
            '54.63.149.41 - - [10/Oct/2000:13:55:36 -0700] '
            '"GET /filter/test HTTP/1.1" 200 1234 "-" '
            '"Mozilla/5.0" "-"'
        )

        # Pass the raw Nginx log line to the parser.
        result = parse_log_line(line)

        # A valid line should produce a dictionary.
        self.assertIsInstance(result, dict)

        # Confirm that each field was extracted correctly.
        self.assertEqual(
            result["client_ip"],
            "54.63.149.41",
        )
        self.assertEqual(
            result["timestamp"],
            "2000-10-10T13:55:36-07:00",
        )
        self.assertEqual(
            result["method"],
            "GET",
        )
        self.assertEqual(
            result["endpoint"],
            "/filter/test",
        )
        self.assertEqual(
            result["protocol"],
            "HTTP/1.1",
        )
        self.assertEqual(
            result["status_code"],
            200,
        )
        self.assertEqual(
            result["response_bytes"],
            1234,
        )
        self.assertEqual(
            result["referrer"],
            "-",
        )
        self.assertEqual(
            result["user_agent"],
            "Mozilla/5.0",
        )

        # The parser must return only the frozen base schema.
        self.assertEqual(
            set(result.keys()),
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
            },
        )

        # Numeric fields must remain integers.
        self.assertIsInstance(
            result["status_code"],
            int,
        )
        self.assertIsInstance(
            result["response_bytes"],
            int,
        )

    def test_invalid_log_line(self):
        """An invalid line should return None without stopping processing."""

        invalid_line = (
            "Invalid log line that does not match the expected format"
        )

        result = parse_log_line(invalid_line)

        self.assertIsNone(result)

    def test_invalid_timestamp_returns_none(self):
        """A malformed timestamp should be rejected without stopping replay."""

        line = (
            '54.63.149.41 - - [99/Oct/2000:13:55:36 -0700] '
            '"GET /filter/test HTTP/1.1" 200 1234 "-" '
            '"Mozilla/5.0" "-"'
        )

        result = parse_log_line(line)

        self.assertIsNone(result)


if __name__ == "__main__":
    unittest.main()
"""Tests for the PySpark batch log parser."""

import subprocess
import tempfile
import unittest
from pathlib import Path

from pyspark import SparkContext
from pyspark.sql import SparkSession
from pyspark.sql import functions as F

from batch.batch_job import (
    compute_batch_metrics,
    read_raw_data,
    read_raw_data_with_quality,
)

SAMPLE_LOG = """\
54.63.149.41 - - [10/Oct/2000:13:55:36 -0700] "GET /filter/test HTTP/1.1" 200 1234 "-" "Mozilla/5.0" "-"
192.168.1.10 - - [11/Oct/2000:09:15:22 +0000] "POST /api/login HTTP/1.1" 500 450 "https://example.com" "TestAgent/1.0" "-"
10.0.0.25 - - [11/Oct/2000:09:16:00 +0000] "GET /images/logo.png HTTP/1.1" 304 - "-" "Mozilla/5.0" "-"
"""

QUALITY_LOG = SAMPLE_LOG + """\
This is not a valid access-log line
127.0.0.1 - - [not-a-valid-timestamp] "GET /bad-time HTTP/1.1" 200 10 "-" "TestAgent/1.0" "-"
"""


class TestBatchLogParser(unittest.TestCase):
    """Verify that the batch parser matches the producer schema."""

    @classmethod
    def setUpClass(cls):
        cls.spark = (
            SparkSession.builder
            .master("local[2]")
            .appName("batch-parser-tests")
            .getOrCreate()
        )
        cls.spark.sparkContext.setLogLevel("ERROR")

    @classmethod
    def tearDownClass(cls):
        """Stop Spark and close its Java gateway on Windows."""
        gateway = SparkContext._gateway
        process = getattr(gateway, "proc", None) if gateway else None

        cls.spark.stop()
        cls.spark = None

        if gateway is not None:
            try:
                gateway.shutdown()
            finally:
                if process is not None and process.poll() is None:
                    process.terminate()

                    try:
                        process.wait(timeout=5)
                    except subprocess.TimeoutExpired:
                        process.kill()

                SparkContext._gateway = None
                SparkContext._jvm = None

    def test_parser_uses_official_schema_and_utc(self):
        """Batch parsing should match the producer and normalise time to UTC."""

        with tempfile.TemporaryDirectory() as temp_directory:
            sample_path = Path(temp_directory) / "sample_access.log"
            sample_path.write_text(SAMPLE_LOG, encoding="utf-8")

            # Start from a non-UTC timezone to prove that the parser
            # produces deterministic UTC results.
            self.spark.conf.set(
                "spark.sql.session.timeZone",
                "Europe/Dublin",
            )

            df = read_raw_data(self.spark, str(sample_path))

            expected_columns = [
                "client_ip",
                "timestamp",
                "method",
                "endpoint",
                "protocol",
                "status_code",
                "response_bytes",
                "referrer",
                "user_agent",
            ]

            self.assertEqual(df.columns, expected_columns)
            self.assertEqual(df.count(), 3)

            displayed_times = {
                row["endpoint"]: row["timestamp_text"]
                for row in (
                    df.select(
                        "endpoint",
                        F.date_format(
                            "timestamp",
                            "yyyy-MM-dd HH:mm:ss",
                        ).alias("timestamp_text"),
                    ).collect()
                )
            }

            self.assertEqual(
                displayed_times["/filter/test"],
                "2000-10-10 20:55:36",
            )
            self.assertEqual(
                displayed_times["/api/login"],
                "2000-10-11 09:15:22",
            )

            logo = (
                df.filter(F.col("endpoint") == "/images/logo.png")
                .first()
            )

            self.assertEqual(logo["response_bytes"], 0)

    def test_batch_metrics_are_correct(self):
        """Historical metrics should match the sample log contents."""

        with tempfile.TemporaryDirectory() as temp_directory:
            sample_path = Path(temp_directory) / "sample_access.log"
            sample_path.write_text(SAMPLE_LOG, encoding="utf-8")

            df = read_raw_data(self.spark, str(sample_path))
            results = compute_batch_metrics(df)

            requests = {
                row["endpoint"]: row["total_requests"]
                for row in results["requests_per_endpoint"].collect()
            }

            self.assertEqual(
                requests,
                {
                    "/filter/test": 1,
                    "/api/login": 1,
                    "/images/logo.png": 1,
                },
            )

            traffic = {
                row["hour"]: row["request_count"]
                for row in results["traffic_by_hour"].collect()
            }

            self.assertEqual(
                traffic,
                {
                    9: 2,
                    20: 1,
                },
            )

            errors = {
                row["endpoint"]: {
                    "total_requests": row["total_requests"],
                    "error_count": row["error_count"],
                    "error_rate": row["error_rate"],
                }
                for row in results["error_rates"].collect()
            }

            self.assertEqual(
                errors["/api/login"]["total_requests"],
                1,
            )
            self.assertEqual(
                errors["/api/login"]["error_count"],
                1,
            )
            self.assertAlmostEqual(
                errors["/api/login"]["error_rate"],
                1.0,
            )

            self.assertEqual(
                errors["/filter/test"]["error_count"],
                0,
            )
            self.assertAlmostEqual(
                errors["/filter/test"]["error_rate"],
                0.0,
            )

            baseline = {
                row["endpoint"]: row["avg_requests_per_minute"]
                for row in results["baseline_rpm"].collect()
            }

            self.assertEqual(
                baseline,
                {
                    "/filter/test": 1.0,
                    "/api/login": 1.0,
                    "/images/logo.png": 1.0,
                },
            )


    def test_data_quality_counts_valid_and_invalid_records(self):
        """Data-quality metrics should explain rejected log records."""

        with tempfile.TemporaryDirectory() as temp_directory:
            sample_path = Path(temp_directory) / "quality_access.log"
            sample_path.write_text(QUALITY_LOG, encoding="utf-8")

            df, quality = read_raw_data_with_quality(
                self.spark,
                str(sample_path),
            )

            self.assertEqual(df.count(), 3)

            self.assertEqual(quality["total_raw_lines"], 5)
            self.assertEqual(quality["valid_records"], 3)
            self.assertEqual(quality["invalid_records"], 2)
            self.assertEqual(quality["invalid_format_records"], 1)
            self.assertEqual(
                quality["invalid_timestamp_records"],
                1,
            )
            self.assertAlmostEqual(
                quality["invalid_percentage"],
                40.0,
            )

if __name__ == "__main__":
    unittest.main()

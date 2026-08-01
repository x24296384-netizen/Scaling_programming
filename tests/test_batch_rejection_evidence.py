"""Compatibility and output tests for batch rejection evidence."""

import subprocess
import tempfile
import unittest
from pathlib import Path
from unittest.mock import patch

from pyspark import SparkContext
from pyspark.sql import SparkSession

from batch.batch_job import (
    build_rejected_evidence,
    read_raw_data_with_quality,
    read_raw_data_with_quality_and_rejected,
    write_rejected_evidence,
)

REJECTION_LOG = """\
54.63.149.41 - - [10/Oct/2000:13:55:36 -0700] "GET /filter/test HTTP/1.1" 200 1234 "-" "Mozilla/5.0" "-"
192.168.1.10 - - [11/Oct/2000:09:15:22 +0000] "POST /api/login HTTP/1.1" 500 450 "https://example.com" "TestAgent/1.0" "-"
10.0.0.25 - - [11/Oct/2000:09:16:00 +0000] "GET /images/logo.png HTTP/1.1" 304 - "-" "Mozilla/5.0" "-"
This is not a valid access-log line
127.0.0.1 - - [not-a-valid-timestamp] "GET /bad-time HTTP/1.1" 200 10 "-" "TestAgent/1.0" "-"
127.0.0.1 - - [11/Oct/2000:09:17:00 +0000] "BROKEN" 400 0 "-" "TestAgent/1.0" "-"
"""


class TestBatchRejectionEvidence(unittest.TestCase):
    """Verify compatibility and precise rejected-record outputs."""

    @classmethod
    def setUpClass(cls):
        cls.spark = (
            SparkSession.builder
            .master("local[2]")
            .appName("batch-rejection-evidence-tests")
            .getOrCreate()
        )
        cls.spark.sparkContext.setLogLevel("ERROR")

    @classmethod
    def tearDownClass(cls):
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

    def _write_fixture(self, directory):
        sample_path = Path(directory) / "rejection_access.log"
        sample_path.write_text(REJECTION_LOG, encoding="utf-8")
        return sample_path

    def test_public_quality_reader_preserves_two_value_interface(self):
        """Existing callers must still receive only records and quality."""
        with tempfile.TemporaryDirectory() as temp_directory:
            sample_path = self._write_fixture(temp_directory)
            result = read_raw_data_with_quality(
                self.spark,
                str(sample_path),
            )

            self.assertEqual(len(result), 2)
            df, quality = result
            self.assertEqual(df.count(), 3)
            self.assertEqual(quality["total_raw_lines"], 6)
            self.assertEqual(quality["valid_records"], 3)
            self.assertEqual(quality["invalid_records"], 3)
            df.unpersist()

    def test_rejection_breakdown_and_sample_outputs(self):
        """Rejected records must have one reason and bounded evidence."""
        with tempfile.TemporaryDirectory() as temp_directory:
            sample_path = self._write_fixture(temp_directory)
            output_path = Path(temp_directory) / "rejection-output"

            df, quality, rejected_df = (
                read_raw_data_with_quality_and_rejected(
                    self.spark,
                    str(sample_path),
                )
            )

            breakdown = {
                row["rejection_reason"]: row["count"]
                for row in (
                    rejected_df.groupBy("rejection_reason")
                    .count()
                    .collect()
                )
            }

            self.assertEqual(
                quality["total_raw_lines"],
                quality["valid_records"] + quality["invalid_records"],
            )
            self.assertEqual(
                breakdown,
                {
                    "invalid_format": 1,
                    "invalid_timestamp": 1,
                    "invalid_request": 1,
                },
            )
            self.assertEqual(
                sum(breakdown.values()),
                quality["invalid_records"],
            )

            breakdown_df, sample_df = build_rejected_evidence(rejected_df)
            generated_breakdown = {
                row["rejection_reason"]: row["count"]
                for row in breakdown_df.collect()
            }
            self.assertEqual(generated_breakdown, breakdown)
            self.assertLessEqual(sample_df.count(), 20)

            # Local Spark CSV writes on Windows require winutils.exe.
            # Mock only the filesystem boundary while retaining all Spark
            # transformations and output-path assertions.
            with patch(
                "pyspark.sql.readwriter.DataFrameWriter.csv"
            ) as csv_mock:
                write_rejected_evidence(rejected_df, str(output_path))

            self.assertEqual(csv_mock.call_count, 2)

            written_paths = [
                str(call.args[0]).replace("\\", "/")
                for call in csv_mock.call_args_list
            ]
            expected_root = str(output_path).replace("\\", "/")
            self.assertEqual(
                written_paths,
                [
                    f"{expected_root}/rejected_breakdown",
                    f"{expected_root}/rejected_sample",
                ],
            )

            rejected_df.unpersist()
            df.unpersist()


if __name__ == "__main__":
    unittest.main()

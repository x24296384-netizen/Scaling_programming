"""Tests for the batch data-quality CSV output."""

import unittest

from batch.batch_job import write_data_quality


class _FakeWriter:
    def __init__(self):
        self.mode_value = None
        self.options = {}
        self.csv_path = None

    def mode(self, value):
        self.mode_value = value
        return self

    def option(self, key, value):
        self.options[key] = value
        return self

    def csv(self, path):
        self.csv_path = path


class _FakeDataFrame:
    def __init__(self):
        self.partition_count = None
        self.writer = _FakeWriter()

    def coalesce(self, partition_count):
        self.partition_count = partition_count
        return self

    @property
    def write(self):
        return self.writer


class _FakeSpark:
    def __init__(self):
        self.rows = None
        self.schema = None
        self.dataframe = _FakeDataFrame()

    def createDataFrame(self, rows, schema):
        self.rows = rows
        self.schema = schema
        return self.dataframe


class TestBatchDataQualityOutput(unittest.TestCase):
    """Verify the complete data-quality output schema and write options."""

    def test_write_data_quality_uses_complete_schema_and_path(self):
        spark = _FakeSpark()

        quality = {
            "total_raw_lines": 100,
            "valid_records": 95,
            "invalid_records": 5,
            "invalid_format_records": 1,
            "invalid_timestamp_records": 1,
            "invalid_request_records": 3,
            "invalid_percentage": 5.0,
        }

        write_data_quality(
            spark,
            quality,
            "s3://example-bucket/batch",
        )

        self.assertEqual(
            spark.rows,
            [(100, 95, 5, 1, 1, 3, 5.0)],
        )

        self.assertEqual(
            spark.schema,
            [
                "total_raw_lines",
                "valid_records",
                "invalid_records",
                "invalid_format_records",
                "invalid_timestamp_records",
                "invalid_request_records",
                "invalid_percentage",
            ],
        )

        self.assertEqual(
            spark.dataframe.partition_count,
            1,
        )

        self.assertEqual(
            spark.dataframe.writer.mode_value,
            "overwrite",
        )

        self.assertEqual(
            spark.dataframe.writer.options,
            {"header": "true"},
        )

        self.assertEqual(
            spark.dataframe.writer.csv_path,
            "s3://example-bucket/batch/data_quality",
        )


if __name__ == "__main__":
    unittest.main()

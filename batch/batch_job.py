"""
Batch layer for the Lambda Architecture (Scalable Cloud Programming CA).

Dataset confirmed: Kaggle "Web Server Access Logs" (eliasdabbas). This is a
RAW nginx-style access log, not a clean CSV — each line looks like:

  192.168.1.10 - - [30/Jul/2025:15:57:19 +0000] "GET /index.html HTTP/1.1" 200 1024 "http://example.com/start" "Mozilla/5.0..."

That's why Mary Helen built a log parser for the producer side. This batch
job parses the same raw format with a regex (standard nginx "combined" log
format) so both sides agree on field names. IMPORTANT: once you've looked at
her actual parser in producer/, double check the field names/regex below
match hers exactly — the serving layer needs both sides speaking the same
schema.

Run on EMR with:
  spark-submit --deploy-mode cluster batch/batch_job.py --input s3://.../raw-data/ --output s3://.../batch-results/
"""

import argparse
import time

from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import IntegerType

# standard nginx "combined" log format regex — covers:
# remote_addr - remote_user [time_local] "method path protocol" status bytes_sent "referer" "user_agent"
LOG_PATTERN = (
    r'^(\S+) \S+ \S+ '
    r'\[([^\]]+)\] '
    r'"([^"]*)" '
    r'(\d{3}) '
    r'(\S+) '
    r'"([^"]*)" '
    r'"([^"]*)" '
    r'"([^"]*)"$'
)


def build_spark_session(app_name="scp-batch-layer"):
    return (
        SparkSession.builder
        .appName(app_name)
        .getOrCreate()
    )


def read_raw_data(spark, input_path):
    """
    Read and parse raw Nginx access-log lines.

    The returned DataFrame uses the same field names as the producer layer.
    Timestamps are normalised to UTC so results are consistent across local
    machines and the Amazon EMR environment.
    """

    # All timestamp operations use UTC, independently of the machine timezone.
    spark.conf.set("spark.sql.session.timeZone", "UTC")

    raw = spark.read.text(input_path)

    parsed = raw.select(
        F.regexp_extract("value", LOG_PATTERN, 1).alias("client_ip"),
        F.regexp_extract("value", LOG_PATTERN, 2).alias("timestamp_raw"),
        F.regexp_extract("value", LOG_PATTERN, 3).alias("request_raw"),
        F.regexp_extract("value", LOG_PATTERN, 4).alias("status_code_raw"),
        F.regexp_extract("value", LOG_PATTERN, 5).alias("response_bytes_raw"),
        F.regexp_extract("value", LOG_PATTERN, 6).alias("referrer"),
        F.regexp_extract("value", LOG_PATTERN, 7).alias("user_agent"),
        F.regexp_extract("value", LOG_PATTERN, 8).alias("extra"),
    )

    # Match the producer behaviour: split the request into three components.
    request_parts = F.split(F.col("request_raw"), " ", 3)
    valid_request = F.size(request_parts) == 3

    df = (
        parsed

        # An empty client_ip means the complete regex did not match.
        .filter(F.col("client_ip") != "")

        .withColumn(
            "timestamp",
            F.to_timestamp(
                "timestamp_raw",
                "dd/MMM/yyyy:HH:mm:ss Z",
            ),
        )
        .withColumn(
            "method",
            F.when(valid_request, request_parts.getItem(0))
            .otherwise(F.lit(None).cast("string")),
        )
        .withColumn(
            "resource",
            F.when(valid_request, request_parts.getItem(1))
            .otherwise(F.col("request_raw")),
        )
        .withColumn(
            "protocol",
            F.when(valid_request, request_parts.getItem(2))
            .otherwise(F.lit(None).cast("string")),
        )
        .withColumn(
            "status_code",
            F.col("status_code_raw").cast(IntegerType()),
        )
        .withColumn(
            "response_bytes",
            F.when(F.col("response_bytes_raw") == "-", 0)
            .otherwise(
                F.col("response_bytes_raw").cast(IntegerType())
            ),
        )
        .select(
            "client_ip",
            "timestamp",
            "method",
            "resource",
            "protocol",
            "status_code",
            "response_bytes",
            "referrer",
            "user_agent",
            "extra",
        )
    )

    return df


def compute_batch_metrics(df):
    """
    Historical / "complete and accurate" view required by Lambda Architecture.
    This is my (Nalini's) part of the split — batch layer + benchmarking.
    """

    # total requests per resource — the baseline the speed layer compares against
    requests_per_resource = (
        df.groupBy("resource")
        .agg(F.count("*").alias("total_requests"))
        .orderBy(F.desc("total_requests"))
    )

    # average traffic by hour of day — useful for the "normal vs abnormal" story
    traffic_by_hour = (
        df.withColumn("hour", F.hour("timestamp"))
        .groupBy("hour")
        .agg(F.count("*").alias("request_count"))
        .orderBy("hour")
    )

    # historical error rate per endpoint (status codes >= 400)
    error_rates = (
        df.withColumn("is_error", (F.col("status_code") >= 400).cast("int"))
        .groupBy("resource")
        .agg(
            F.count("*").alias("total_requests"),
            F.sum("is_error").alias("error_count"),
        )
        .withColumn("error_rate", F.col("error_count") / F.col("total_requests"))
        .orderBy(F.desc("error_rate"))
    )

    # baseline requests-per-minute per endpoint — this is the number the
    # serving layer will compare live speed-layer numbers against to flag anomalies
    baseline_rpm = (
        df.withColumn("minute_bucket", F.date_trunc("minute", "timestamp"))
        .groupBy("resource", "minute_bucket")
        .agg(F.count("*").alias("requests_in_minute"))
        .groupBy("resource")
        .agg(F.avg("requests_in_minute").alias("avg_requests_per_minute"))
    )

    return {
        "requests_per_resource": requests_per_resource,
        "traffic_by_hour": traffic_by_hour,
        "error_rates": error_rates,
        "baseline_rpm": baseline_rpm,
    }


def write_results(results, output_path):
    for name, df in results.items():
        df.coalesce(1).write.mode("overwrite").option("header", "true").csv(
            f"{output_path}/{name}"
        )


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--input", required=True, help="S3 path to raw data")
    parser.add_argument("--output", required=True, help="S3 path for batch results")
    parser.add_argument(
        "--workers", type=int, default=None,
        help="Just for logging in benchmark runs — not enforced here, worker "
             "count comes from the EMR cluster config itself"
    )
    args = parser.parse_args()

    spark = build_spark_session()

    start_time = time.time()

    df = read_raw_data(spark, args.input)
    df.cache()  # reused across all four aggregations below, worth caching
    record_count = df.count()  # forces the read, also useful for benchmarking

    results = compute_batch_metrics(df)
    write_results(results, args.output)

    elapsed = time.time() - start_time

    # Basic benchmark log used by benchmark/run_batch_benchmarks.py.
    # will run this multiple times with different worker counts and collect these
    print(f"BENCHMARK workers={args.workers} records={record_count} elapsed_sec={elapsed:.2f}")

    spark.stop()


if __name__ == "__main__":
    main()
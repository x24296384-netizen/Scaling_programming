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
  spark-submit --deploy-mode cluster batch_job.py --input s3://.../raw-data/ --output s3://.../batch-results/
"""

import argparse
import time

from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import IntegerType

# standard nginx "combined" log format regex — covers:
# remote_addr - remote_user [time_local] "method path protocol" status bytes_sent "referer" "user_agent"
LOG_PATTERN = (
    r'^(\S+) \S+ \S+ \[(.*?)\] "(\S+) (\S+) (\S+)" (\d{3}) (\S+) "(.*?)" "(.*?)"'
)


def build_spark_session(app_name="scp-batch-layer"):
    return (
        SparkSession.builder
        .appName(app_name)
        .getOrCreate()
    )


def read_raw_data(spark, input_path):
    """
    Reads the raw nginx log as plain text lines, then parses each line with
    regexp_extract into named columns. Malformed lines (regex doesn't match)
    end up with an empty ip, which we filter out below — worth counting
    those for the report's "data cleaning" section.
    """
    raw = spark.read.text(input_path)

    df = raw.select(
        F.regexp_extract("value", LOG_PATTERN, 1).alias("ip"),
        F.regexp_extract("value", LOG_PATTERN, 2).alias("timestamp_raw"),
        F.regexp_extract("value", LOG_PATTERN, 3).alias("method"),
        F.regexp_extract("value", LOG_PATTERN, 4).alias("endpoint"),
        F.regexp_extract("value", LOG_PATTERN, 5).alias("protocol"),
        F.regexp_extract("value", LOG_PATTERN, 6).alias("status_code_raw"),
        F.regexp_extract("value", LOG_PATTERN, 7).alias("bytes_sent_raw"),
        F.regexp_extract("value", LOG_PATTERN, 8).alias("referrer"),
        F.regexp_extract("value", LOG_PATTERN, 9).alias("user_agent"),
    )

    # nginx time format: 30/Jul/2025:15:57:19 +0000
    df = df.withColumn(
        "timestamp", F.to_timestamp("timestamp_raw", "dd/MMM/yyyy:HH:mm:ss Z")
    ).withColumn(
        "status_code", F.col("status_code_raw").cast(IntegerType())
    ).withColumn(
        # bytes_sent is sometimes "-" for zero-byte responses
        "bytes_sent",
        F.when(F.col("bytes_sent_raw") == "-", 0)
         .otherwise(F.col("bytes_sent_raw").cast(IntegerType())),
    )

    # drop rows where the regex didn't match at all (empty ip = no match)
    df = df.filter(F.col("ip") != "")

    return df


def compute_batch_metrics(df):
    """
    Historical / "complete and accurate" view required by Lambda Architecture.
    This is my (Nalini's) part of the split — batch layer + benchmarking.
    """

    # total requests per endpoint — the baseline the speed layer compares against
    requests_per_endpoint = (
        df.groupBy("endpoint")
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
        .groupBy("endpoint")
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
        .groupBy("endpoint", "minute_bucket")
        .agg(F.count("*").alias("requests_in_minute"))
        .groupBy("endpoint")
        .agg(F.avg("requests_in_minute").alias("avg_requests_per_minute"))
    )

    return {
        "requests_per_endpoint": requests_per_endpoint,
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

    # crude benchmarking log — the real benchmarking harness (benchmark.py)
    # will run this multiple times with different worker counts and collect these
    print(f"BENCHMARK workers={args.workers} records={record_count} elapsed_sec={elapsed:.2f}")

    spark.stop()


if __name__ == "__main__":
    main()
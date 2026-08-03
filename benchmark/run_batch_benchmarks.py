"""Benchmark harness for the PySpark batch layer.

The script executes the batch job against the same input while the EMR core
capacity is changed between benchmark runs. It records elapsed execution time
and calculates speedup and parallel efficiency relative to the one-worker
baseline.

The EMR instance-group capacity must be adjusted separately before each run.
S3 input and output paths must also be configured before execution.
"""

import csv
import re
import subprocess
import sys

INPUT_PATH = "s3://<data-bucket>/raw-data/"
OUTPUT_PATH = "s3://<results-bucket>/batch-results/"
RESULTS_CSV = "benchmark_results.csv"

# Worker counts used in the controlled EMR benchmark.
WORKER_COUNTS_TO_TEST = [1, 2, 4]


def run_batch_job(workers):
    """
    Assumes the EMR cluster's core node count has ALREADY been resized to
    `workers` before calling this (do that manually, or extend this script
    with an aws emr modify-instance-groups call once we're comfortable
    automating it).
    """
    cmd = [
        "spark-submit",
        "--deploy-mode", "cluster",
        "batch/batch_job.py",
        "--input", INPUT_PATH,
        "--output", OUTPUT_PATH,
        "--workers", str(workers),
    ]
    result = subprocess.run(cmd, capture_output=True, text=True)

    match = re.search(r"elapsed_sec=([\d.]+)", result.stdout)
    if not match:
        print(f"Couldn't find benchmark line in output for workers={workers}")
        print(result.stdout)
        print(result.stderr, file=sys.stderr)
        return None

    return float(match.group(1))


def main():
    rows = []
    baseline_time = None

    for workers in WORKER_COUNTS_TO_TEST:
        print(f"Running with {workers} worker(s)... (make sure cluster is resized first)")
        elapsed = run_batch_job(workers)
        if elapsed is None:
            continue

        if workers == 1:
            baseline_time = elapsed

        speedup = (baseline_time / elapsed) if baseline_time else 1.0
        efficiency = speedup / workers

        rows.append({
            "workers": workers,
            "elapsed_sec": elapsed,
            "speedup": round(speedup, 3),
            "efficiency": round(efficiency, 3),
        })
        print(f"  -> {elapsed:.2f}s, speedup={speedup:.2f}, efficiency={efficiency:.2f}")

    with open(RESULTS_CSV, "w", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=["workers", "elapsed_sec", "speedup", "efficiency"])
        writer.writeheader()
        writer.writerows(rows)

    print(f"\nSaved results to {RESULTS_CSV} — plot speedup/efficiency vs workers from this for the report.")


if __name__ == "__main__":
    main()
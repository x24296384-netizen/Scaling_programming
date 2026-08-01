# Contribution Statement (draft — Nalini's portion)

## Nalini Panneerselvam — Batch Layer, EMR, Auto-Scaling, Benchmarking

- Designed and implemented the PySpark batch layer, parsing raw nginx-format
  access logs and computing all four core historical aggregates (requests
  per endpoint, traffic by hour, error rates, baseline requests-per-minute),
  plus status code distribution, response byte totals, and summary metrics.
- Set up and configured the Amazon EMR cluster (Spark 3.5.0, EMR 7.1.0),
  including an EMR-managed auto-scaling policy (scale out below 15% YARN
  memory, scale in above 75%, 300s cooldown, 1-5 core nodes).
- Ran and validated benchmarking across 1, 2, and 4 worker configurations
  on the full ~3.5GB dataset, producing genuine isolated timing measurements
  and calculating speedup/efficiency, with critical analysis of the
  sub-linear scaling pattern observed.
- Investigated and root-caused the batch layer's malformed-record count,
  identifying that rejected records corresponded to attack, probe, and
  non-HTTP traffic in the source dataset (IoT botnet exploit attempts,
  TLS/BitTorrent protocol bytes) rather than genuine data-quality defects.
- Authored data-quality tracking, rejection-reason categorization, and a
  rejected-record sampling capability, and wrote automated tests validating
  these properties.
- Produced supporting evidence: EMR console screenshots, benchmark charts
  (speedup/efficiency/execution-time vs workers), the batch-layer report
  section, and the batch-layer README/setup documentation.
- Collaborated on aligning the shared event schema between the batch and
  streaming pipelines, and on integrating batch-layer work into the shared
  repository structure.

## [Mary Helen's portion — to be completed together]

- Producer/log parser, Kinesis replay pipeline, speed-layer sliding-window
  analytics, Lambda-based stream processing, S3 snapshot persistence,
  integration testing and validation, repository restructuring.

## Shared work

- Event schema design and alignment
- Serving-layer design (batch baseline + speed-layer comparison)
- Report writing, review, and final assembly
- Architecture diagram, presentation slides, demo script
- GitHub repository organization and evidence collection

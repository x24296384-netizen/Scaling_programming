# Project Status

## Project

- **Title:** Scalable Web Log Analytics
- **Architecture:** Lambda Architecture
- **Cloud platform:** Amazon Web Services
- **Primary region:** `us-east-1`
- **Deadline:** 4 August 2026 at 5:00 pm
- **Final repository branch:** `main`
- **Merged integration branch:** `maryhelen-integration`

The project analyses a large Nginx access-log dataset through two processing
paths:

- PySpark batch processing on Amazon EMR;
- near-real-time processing using Amazon Kinesis Data Streams and AWS Lambda.

The batch layer provides complete historical analytics. The speed layer
provides recent event-time analytics. The serving layer combines both paths to
compare recent endpoint RPM with a historical baseline.

## Overall Status

- **Technical implementation:** Complete
- **Final EMR batch delivery:** Complete
- **Athena deployment and reconciliation:** Complete
- **Real Batch-Speed serving validation:** Complete
- **Architecture diagram:** Complete
- **Automated tests:** 64 passed
- **Performance analysis:** Complete
- **Repository review and merge into `main`:** Complete
- **Technical report:** Final review in progress
- **Presentation, demonstration and submission:** Pending

No additional architecture or repository integration work is required. The
remaining work is limited to final report checks, the presentation and
demonstration, submission and preservation of the submission receipt.

## Team Responsibilities

### Maryhelen

- Shared schema and repository integration
- Nginx parser and reliable Kinesis replay
- Sliding-window speed analytics
- Kinesis-triggered Lambda processing
- S3 snapshot and immutable-delta persistence
- Global speed-layer aggregation
- Combined serving view and traffic-spike comparison
- Athena deployment, reconciliation and evidence
- Final architecture, integration evidence and documentation

### Nalini

- PySpark batch-processing implementation
- Amazon EMR execution and worker benchmarks
- EMR auto-scaling configuration and evidence
- Final batch outputs and data-quality evidence
- Batch benchmark documentation

The batch contribution has been received, validated and integrated. No further
batch implementation or evidence dependency remains open. The final EMR
auto-scaling trigger attempt has been completed and documented.

## Shared Event Schema

The batch and real-time paths use the same base schema:

```text
client_ip
timestamp
method
endpoint
protocol
status_code
response_bytes
referrer
user_agent
```

The Kinesis path additionally includes:

```text
event_id
ingested_at
source
```

Schema rules:

- `endpoint` is the official shared request-path field;
- `client_ip` replaces the earlier batch field `ip`;
- `response_bytes` replaces the earlier batch field `bytes_sent`;
- timestamps are normalised to timezone-aware ISO 8601 UTC values;
- `status_code` and `response_bytes` are integers;
- a response-byte value of `-` is converted to zero;
- HTTP requests must contain method, endpoint and protocol;
- `event_id` is generated as a unique identifier;
- `source` is set to `nginx_access_log`.

## Day 1 - Repository and Reliability Foundation

**Status:** Complete

Completed work:

- Integrated the initial PySpark batch implementation.
- Created the `maryhelen-integration` branch.
- Reorganised the repository into batch, speed, producer, benchmark, serving
  and infrastructure modules.
- Added Kinesis record construction.
- Used `client_ip` as the Kinesis partition key.
- Added `PutRecords` batching.
- Added partial-failure handling.
- Retried only failed Kinesis records.
- Added data-quality reporting.
- Added automated tests for parsing, batch processing and replay reliability.
- Fixed Spark test shutdown behaviour on Windows.
- Added project status and task documentation.

Important commits:

- `7379d7d` - Reorganise project into Lambda architecture modules
- `8fd0e10` - Add batch data-quality reporting
- `2bbbca9` - Fix Spark test shutdown on Windows
- `367d3ae` - Add Kinesis record batching with retry support
- `849e0b1` - Add project status and task list

## Day 2 - Shared Schema and Kinesis Replay

**Status:** Complete

Completed work:

- Froze `endpoint` as the official shared field.
- Replaced previous `resource` references.
- Aligned `client_ip` and `response_bytes`.
- Normalised batch timestamps to UTC.
- Added `event_id`, `ingested_at` and `source`.
- Connected the Nginx parser to the Kinesis replay path.
- Added progressive file processing.
- Counted and skipped malformed log lines.
- Enforced Kinesis batch sizes between 1 and 500 records.
- Supported complete and final partial batches.
- Added configurable retries and retry delays.
- Added command-line replay support.
- Added tests for parser, replay, batching and permanent failures.

Important commits:

- `f0ca5c0` - Freeze endpoint schema and complete Kinesis replay
- `ce752c4` - Update batch progress and shared schema documentation

## Day 3 - Speed Layer and Benchmarks

**Status:** Complete

### Implementation

Completed work:

- Added event-time sliding-window analytics.
- Added incremental event eviction.
- Added support for out-of-order events inside the active window.
- Kept the lower window boundary inclusive.
- Added requests per endpoint.
- Added traffic by hour.
- Added error rates per endpoint.
- Added HTTP status-code distribution.
- Added response-byte totals.
- Added a local Kinesis-style consumer.
- Added invalid JSON handling.
- Added reusable Kinesis shard discovery and record reading.
- Added throughput and latency measurements.
- Added local and AWS benchmark scripts.

### Full Local Benchmark

Dataset:

- Kaggle Web Server Access Logs
- Dataset size: 3,502,440,823 bytes
- Total lines: 10,365,152
- Valid records: 10,365,077
- Invalid lines: 75
- Failed records: 0

Measured results:

- Runtime: 352.8049 seconds
- Throughput: 29,379.27 lines per second
- Valid-record throughput: 29,379.06 records per second
- Sampled local latency mean: 0.0351 ms
- Sampled local latency p95: 0.06968 ms

The local latency covers parsing, Kinesis-style JSON creation, consumer
decoding and sliding-window analytics. It does not include AWS network latency.

### Real Amazon Kinesis Smoke Test

Stream configuration:

- Mode: provisioned
- Open shards: 1
- Retention: 24 hours

Observed result:

- Records sent: 3
- Records received: 3
- Records processed: 3
- Invalid records: 0
- Failed records: 0
- Read attempts: 1
- Complete runtime: 0.30739 seconds
- Observed end-to-end latency: 300.444 ms

This was a controlled functional smoke test, not a maximum-throughput test.

Important commits:

- `72b5800` - Add sliding-window speed-layer analytics
- `cb267e6` - Add local stream consumer and integration tests
- `bfcfef2` - Complete Kinesis speed layer and benchmarks

## Day 4 - Batch and Streaming Integration

**Status:** Complete

### Controlled Input

Both processing paths were validated with:

```text
tests/fixtures/integration_window.log
```

Controlled input result:

- Raw lines: 9
- Valid records: 7
- Rejected records: 2
- Total response bytes: 600

### Batch and Streaming Alignment

The producer parser rejected a malformed HTTP request, but the initial batch
parser accepted it because the batch validity rule checked only the complete
log format and timestamp.

The batch parser was updated to validate the HTTP request structure.

The two paths now reject the same malformed records and calculate the same
comparable metrics:

- total valid records;
- total response bytes;
- requests per endpoint;
- error rates per endpoint;
- traffic by hour;
- status-code distribution;
- response-byte totals per endpoint.

### Automated Comparison

Command:

```powershell
python -m serving.compare_controlled_window
```

Final result:

```text
PASS - total valid records
PASS - total response bytes
PASS - requests per endpoint
PASS - error rates
PASS - traffic by hour
PASS - status-code distribution
PASS - response-byte totals per endpoint

ALL COMPARABLE METRICS MATCH
```

### Kinesis-Triggered AWS Lambda

AWS function:

```text
scp-speed-processor-25186396
```

Implemented and validated:

- Connected Amazon Kinesis to AWS Lambda through an event-source mapping.
- Enabled the trigger with starting position `LATEST`.
- Configured a batch size of 100 records.
- Enabled `ReportBatchItemFailures`.
- Enabled batch splitting on function errors.
- Configured three retry attempts.
- Decoded real Lambda Kinesis records.
- Updated recent sliding-window analytics.
- Detected high error-rate anomalies.
- Logged processing summaries and anomalies in CloudWatch.

Controlled AWS result:

- Records sent: 2
- Failed records: 0
- Records received by Lambda: 2
- Records processed: 2
- Invalid records: 0
- Anomalies detected: 1
- Event-source mapping state: Enabled
- Last processing result: OK

### Speed-Layer Snapshot Persistence

The Lambda persists its latest local analytics view in:

```text
s3://scp-speed-results-25186396/speed/latest_snapshot.json
```

The stored document contains:

- schema version;
- generation timestamp;
- processing summary;
- recent anomalies;
- requests per endpoint;
- error rates;
- traffic by hour;
- status-code distribution;
- response-byte totals;
- sliding-window boundaries.

Controlled persistence result:

- Records processed: 2
- Window event count: 2
- Total response bytes: 400
- HTTP 200 responses: 1
- HTTP 500 responses: 1
- Endpoint error rate: 0.50
- Anomalies detected: 1
- S3 persistence result: stored
- Content type: `application/json`
- Server-side encryption: AES256

A temporary S3 failure is logged without incorrectly marking already
processed Kinesis records as invalid.

### Historical Automated-Test Milestone

At the end of Day 4:

- Total tests: 36
- Result: all tests passed
- Execution time: 15.934 seconds

The suite later grew to 64 tests.

### Evidence

Controlled comparison evidence:

```text
docs/evidence/integration/01_producer_fixture_validation.txt
docs/evidence/integration/02_batch_fixture_validation.txt
docs/evidence/integration/03_batch_alignment_confirmation.txt
docs/evidence/integration/04_batch_metrics_validation.txt
docs/evidence/integration/05_stream_metrics_validation.txt
docs/evidence/integration/06_batch_stream_comparison.txt
```

Lambda, persistence and test evidence:

```text
docs/evidence/speed/01_real_kinesis_lambda_e2e.txt
docs/evidence/speed/02_s3_snapshot_persistence_e2e.txt
docs/evidence/speed/03_lambda_distributed_state_limitation.txt
docs/evidence/speed/screenshots/
docs/evidence/tests/01_lambda_handler_tests.txt
docs/evidence/tests/02_full_suite_64_tests.txt
```

Machine-readable evidence:

```text
results/integration/batch_stream_comparison.json
```

Important commits:

- `e8bcb14` - Validate batch and streaming analytics parity
- `67aa834` - Add and validate Kinesis-triggered Lambda processing
- `9bb4324` - Persist speed-layer snapshots in Amazon S3

## Day 5 - Global Speed Aggregation and Load Validation

**Status:** Complete

Completed work:

- Added immutable per-invocation Lambda delta documents under
  `speed/batches/`.
- Added `speed/global_aggregator.py`.
- Rebuilt the complete event-time window across concurrent Lambda
  environments.
- Added event deduplication and invalid-document accounting.
- Wrote the consolidated result to `speed/global_snapshot.json`.
- Updated the serving layer to use the global snapshot instead of treating one
  Lambda environment as global state.
- Added automated tests for global aggregation.

AWS validation results:

| Load | Successful | Failed | Local snapshot | Global endpoint count |
|---:|---:|---:|---:|---:|
| 100 | 100 | 0 | 100 | 100 |
| 500 | 500 | 0 | 500 | 500 |
| 1,000 | 1,000 | 0 | 600 | 1,000 |

The 1,000-event result demonstrates why the local Lambda snapshot cannot be
treated as globally complete. Multiple Lambda execution environments processed
the stream, but the immutable deltas allowed the global aggregator to recover
all 1,000 events for the benchmark endpoint.

Important commits:

- `33135c8` - Add global speed-layer aggregation
- `7c8aacf` - Validate global aggregation under Lambda concurrency

## Day 6 - Athena and Endpoint RPM Comparison

**Status:** Complete

Completed work:

- Added Athena database, external-table, view, validation and demonstration SQL.
- Deployed database `scp_web_logs_25186396` in `us-east-1`.
- Configured the `primary` workgroup result location as
  `s3://scp-speed-results-25186396/athena-results/`.
- Created seven external tables and two views.
- Validated Athena queries against the final real EMR outputs.
- Reconciled the main batch aggregates through Athena.
- Added endpoint-level historical baseline versus recent RPM comparison.
- Added configurable traffic-spike ratio and minimum-request thresholds.
- Added ranked traffic-spike summaries.
- Validated the comparison with a controlled baseline.
- Completed a real Batch-Speed comparison for `/settings/logo`.
- Completed a full suite of 64 automated tests.

Athena configuration:

- Database: `scp_web_logs_25186396`
- Workgroup: `primary`
- Query output: `s3://scp-speed-results-25186396/athena-results/`
- External tables: 7
- Views: 2

Athena reconciliation confirmed:

- 893,048 endpoint rows;
- 10,365,077 requests across each main aggregate;
- 128,870,996,472 response bytes;
- 177,634 error responses;
- 15 HTTP status-code categories;
- no empty baseline endpoints;
- no null RPM values.

Important commits:

- `5268911` - Add Athena tables and deployment documentation
- `75e00da` - Add endpoint RPM baseline comparison
- `157cf74` - Add final Athena batch validation evidence

## Final Batch Evidence

**Status:** Complete and validated

Received and validated from the batch-layer owner:

- EMR benchmark documents and report material;
- execution-time, speedup and efficiency graphs;
- EMR step, cluster and S3 evidence;
- auto-scaling policy evidence;
- the final single-run delivery containing all ten CSV outputs;
- complete data-quality and rejected-record evidence.

Confirmed final batch results:

- Total raw lines: 10,365,152
- Valid records: 10,365,077
- Rejected records: 75
- Invalid format records: 0
- Invalid timestamp records: 0
- Invalid request records: 75
- Endpoint aggregate rows: 893,048
- Total error responses: 177,634
- Total response bytes: 128,870,996,472
- Final CSV outputs: 10

Final execution provenance:

- Nalini final-run source commit: `120dda8` on `origin/nalini-batch-layer`
- Equivalent and extended data-quality functionality is integrated in `main`
- EMR cluster: `j-2KIK1VQPJT200`
- EMR step: `s-09947463792EMAWECP0P`
- Final delivery archive: `final_v2_delivery.zip`
- Archive SHA-256:
  `EAE8C84DE4727271AC1EFCE103D3B1E5EA99D83C998E280F32D924A9EBB98474`

Batch benchmark results:

| Workers | Execution time | Speedup | Efficiency |
|---:|---:|---:|---:|
| 1 | 410.4 s | 1.00x | 100% |
| 2 | 380.1 s | 1.08x | 54% |
| 4 | 294.1 s | 1.40x | 35% |

All ten final CSV outputs came from the same EMR execution. Earlier mixed-run
deliveries were marked as superseded and were not used in the final analysis.

Important commits:

- `c980599` - Add data-quality output test
- `12d9749` - Add final EMR batch delivery evidence
- `4b2123f` - Update status with final EMR and Athena results

## Real Batch-Speed Serving Validation

**Status:** Complete

Validation endpoint:

```text
/settings/logo
```

Historical input from the final EMR results through Athena:

- Historical dataset records: 10,365,077
- Historical endpoint requests: 352,047
- Historical endpoint errors: 971
- Historical error rate: 0.0027581544509681947
- Historical response bytes: 1,444,977,294
- Historical baseline RPM: 52.25575181831676

Recent speed-layer input:

- Events requested: 600
- Events successfully sent to Kinesis: 600
- Failed events: 0
- Kinesis producer batches: 2
- S3 delta objects before: 16
- S3 delta objects after: 22

Global aggregation result:

- Window seconds: 300
- Window event count: 600
- Recent endpoint requests: 600
- Recent RPM: 120.0
- Recent error count: 30
- Recent error rate: 0.05
- Duplicate events: 0
- Invalid documents: 0
- Invalid events: 0

Combined serving result:

- RPM difference: 67.744248
- Recent-to-baseline ratio: 2.296398
- Traffic status: `significant increase`
- Significant increase: `true`
- Error-rate anomalies: 0

The traffic-spike rule was satisfied because the recent rate was more than
twice the historical baseline and the recent request count exceeded the
minimum of ten. The separate error-rate anomaly rule was not satisfied because
the recent error rate of `0.05` was below the configured threshold of `0.50`.

Evidence:

```text
results/serving/real_batch_metrics.json
results/serving/real_global_aggregation_run.json
results/serving/real_global_snapshot.json
results/serving/real_combined_serving_view.json
docs/evidence/serving/01_real_batch_speed_validation.txt
docs/evidence/tests/02_full_suite_64_tests.txt
```

Important commit:

- `3157f48` - Validate real batch and speed serving integration

## Automated Tests

**Status:** Complete

The latest verification after the fast-forward merge into `main` produced:

```text
Ran 64 tests in 24.606s

OK
```

Exit code: `0`

The committed concise evidence at
`docs/evidence/tests/02_full_suite_64_tests.txt` records an earlier complete
passing execution in 30.343 seconds. Both runs completed successfully. The
Windows-specific Spark shutdown warning occurred after the unittest result and
did not invalidate the suite.

## Architecture and Evidence

**Status:** Complete

Final architecture files:

```text
docs/architecture/lambda_architecture_final.drawio
docs/architecture/lambda_architecture_final.png
docs/architecture/lambda_architecture_final.svg
```

The diagram includes:

- Nginx data ingestion;
- historical S3 storage;
- EMR and PySpark batch processing;
- Kinesis and Lambda speed processing;
- S3 local snapshots and immutable deltas;
- the separate global window aggregator;
- the global serving snapshot;
- the combined serving view;
- Athena external tables, views and SQL queries;
- EMR auto-scaling and CloudWatch monitoring.

Important commits:

- `fad2283` - Add final Lambda architecture diagram
- `8114df6` - Remove obsolete architecture placeholder

## Final Git Status

**Status:** Complete

The final technical integration was verified as follows:

- `maryhelen-integration` was committed and pushed through `b0fbf1b`;
- `main` was updated by fast-forward from `82ddf3c` to `b0fbf1b`;
- `origin/main` was successfully updated to `b0fbf1b`;
- the final working tree was clean at the merge verification point;
- the complete 64-test suite passed from the updated `main` worktree;
- no oversized staged files were found during the final repository review.

Final technical integration commit:

```text
b0fbf1b - Finalize project evidence, analysis, and repository structure
```

The repository no longer depends on `maryhelen-integration` for submission.
The submission link should point to the `main` branch.

## Local Spark Warnings

Local Spark execution on Windows may show warnings relating to:

- `winutils.exe`;
- `HADOOP_HOME`;
- native Hadoop libraries;
- local socket cleanup;
- Spark temporary-directory deletion.

These warnings do not invalidate a successful test run when the unittest
summary ends with `OK` and the process exit code is `0`.

## Current Limitations

- AWS Academy Learner Lab sessions can expire and terminate temporary
  resources.
- The EMR auto-scaling policy was verified under four concurrent Steps. The
  workload produced 28 pending containers, but the complete post-test instance
  inventory contained only two core instances and one master instance; no
  verified scale-out occurred.
- The most recent local Lambda snapshot is not globally complete under
  concurrency; the global aggregator is therefore required.
- Global aggregation currently runs explicitly rather than through a scheduled
  production workflow.
- The benchmark dataset and AWS Academy environment do not represent a
  production service-level agreement.
- Batch scaling was sub-linear because Spark startup, scheduling,
  communication and shuffle overheads remained significant.

## Completed Technical Tasks

- [x] Shared schema alignment
- [x] Reliable Kinesis replay with retries
- [x] Sliding-window speed analytics
- [x] Kinesis-triggered Lambda
- [x] S3 snapshot persistence
- [x] Immutable Lambda batch deltas
- [x] Global speed-layer aggregation
- [x] Controlled 100, 500 and 1,000-event AWS validation
- [x] Performance graphs
- [x] Final single-run EMR batch delivery
- [x] Ten final batch CSV outputs
- [x] Batch data-quality and rejection evidence
- [x] Upload final batch outputs to the project S3 bucket
- [x] Athena database, seven tables and two views
- [x] Athena reconciliation checks with real data
- [x] Athena demonstration queries and screenshots
- [x] Endpoint-level RPM baseline comparison
- [x] Real historical batch serving document
- [x] Real global speed snapshot
- [x] Final combined serving view
- [x] Real traffic-spike validation
- [x] Final architecture diagram
- [x] Organised serving-layer evidence
- [x] Final 64-test automated suite
- [x] Final performance-analysis discussion
- [x] Technical report updated with validated results
- [x] Review for accidental and oversized files
- [x] Merge `maryhelen-integration` into `main`
- [x] Run the final test suite from updated `main`
- [x] Confirm that `main` was clean and synchronised
- [x] Commit and push the final repository structure and evidence
- [x] Integrate the final concurrent EMR auto-scaling trigger evidence

## Remaining Tasks

- [ ] Insert the final presentation URL and signatures in the report
- [ ] Export and visually verify the final PDF
- [ ] Prepare presentation slides, demonstration notes and backup video
- [ ] Submit the report and `main` repository link
- [ ] Save the submission receipt

### Final EMR auto-scaling attempt

- Step Concurrency Level was confirmed as 4.
- Four EMR Steps started concurrently at approximately 21:14 UTC.
- The monitoring dashboard recorded 28 pending containers.
- The complete unfiltered post-test instance inventory listed two core
  instances and one master instance.
- No additional core instance was present.
- No verified scale-out event occurred.
- Evidence is stored in `docs/evidence/batch/12_*` through `16_*`.

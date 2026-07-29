# Batch Layer Progress

**Project:** Scalable Cloud Programming  
**Architecture:** Lambda Architecture for Web Server Log Analytics  
**Batch layer owner:** Nalini  
**Integration support:** Maryhelen  
**Last updated:** 29 July 2026

---

## 1. Current Status

The batch layer is implemented using PySpark and Amazon EMR.

The batch pipeline:

1. Reads the raw Nginx access log dataset.
2. Parses each raw log line using a regular expression.
3. Validates and converts the required fields.
4. Removes malformed records from the valid analytical dataset.
5. Produces historical aggregate outputs.
6. Writes the results to Amazon S3.
7. Supports benchmark execution with different EMR worker configurations.

Nalini has confirmed that the benchmark runs with **1, 2 and 4 workers** completed successfully.

The EMR setup script was restored and corrected in:

```text
Commit: f42e155
Branch: nalini-batch-layer
Description: Restore the complete emr_setup.sh script with the auto-scaling-role fix
```

---

## 2. Raw Nginx Input

The source dataset contains raw Nginx access log lines.

The parser extracts the following information from each line:

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

The original Nginx field commonly described as `bytes_sent` is exposed in the shared project schema as:

```text
response_bytes
```

The original batch field `ip` is exposed in the shared project schema as:

```text
client_ip
```

The request path is consistently named:

```text
endpoint
```

The field name `resource` is not part of the final shared schema.

---

## 3. Agreed Shared Schema

Both the batch and real-time paths should use the same base event schema:

| Field | Type | Description |
|---|---|---|
| `client_ip` | string | Client IP address from the Nginx log |
| `timestamp` | ISO 8601 timestamp | Time when the request was received |
| `method` | string | HTTP request method |
| `endpoint` | string | Requested path or endpoint |
| `protocol` | string | HTTP protocol version |
| `status_code` | integer | HTTP response status code |
| `response_bytes` | integer | Number of bytes returned |
| `referrer` | string or null | HTTP referrer |
| `user_agent` | string or null | Client user-agent value |

The real-time Kinesis path may also include the following metadata:

| Field | Type | Description |
|---|---|---|
| `event_id` | string | Unique identifier for the streaming event |
| `ingested_at` | ISO 8601 timestamp | Time when the event entered the streaming pipeline |
| `source` | string | Source identifier, for example `nginx_access_log` |

These streaming metadata fields are not required for the historical batch parser unless they are later needed by the serving layer.

---

## 4. Parsing and Data Quality

The PySpark parser performs the following checks:

- Extracts fields from the raw Nginx line.
- Verifies that the request contains `method`, `endpoint` and `protocol`.
- Converts the timestamp into a Spark timestamp.
- Converts `status_code` to an integer.
- Converts `response_bytes` to an integer.
- Treats a missing or `-` byte value safely.
- Separates valid and invalid records.
- Excludes parsing-only fields from the final shared event.

The regular expression may still capture an internal field called `extra` to recognise the complete raw Nginx line. However, `extra` is not selected into the final valid event schema.

---

## 5. Batch Aggregate Outputs

The batch job produces four historical outputs.

### 5.1 Requests per Endpoint

**Output name:**

```text
requests_per_endpoint
```

**Columns:**

```text
endpoint
total_requests
```

**Purpose:**  
Shows the total number of requests received by each endpoint across the complete dataset.

### 5.2 Traffic by Hour

**Output name:**

```text
traffic_by_hour
```

**Columns:**

```text
hour
request_count
```

**Purpose:**  
Shows the total request volume for each hour of the day.

### 5.3 Error Rates

**Output name:**

```text
error_rates
```

**Columns:**

```text
endpoint
total_requests
error_count
error_rate
```

**Purpose:**  
Calculates the historical error rate for each endpoint. HTTP status codes greater than or equal to 400 are treated as errors.

### 5.4 Baseline Requests per Minute

**Output name:**

```text
baseline_rpm
```

**Columns:**

```text
endpoint
avg_requests_per_minute
```

**Purpose:**  
Provides a historical average requests-per-minute baseline for each endpoint. This output can later be compared with recent speed-layer results.

---

## 6. EMR and Scalability Work

The batch layer uses Amazon EMR to execute the PySpark job.

Completed benchmark configurations:

| Configuration | Status |
|---|---|
| 1 worker | Completed successfully |
| 2 workers | Completed successfully |
| 4 workers | Completed successfully |

The final report should include the real values for:

- worker count;
- instance type;
- input dataset size;
- execution time;
- throughput;
- speedup;
- parallel efficiency;
- S3 output location;
- EMR cluster configuration;
- scaling policy;
- CloudWatch or EMR monitoring evidence.

The calculations should use:

```text
Speedup = single-worker execution time / parallel execution time
```

```text
Efficiency = speedup / number of workers
```

No benchmark value should be added to the report until it is supported by execution output, logs or screenshots.

---

## 7. Required Benchmark Evidence

The following evidence should be stored in the shared OneDrive evidence folder:

- EMR cluster configuration.
- EMR step completed successfully.
- 1-worker benchmark result.
- 2-worker benchmark result.
- 4-worker benchmark result.
- Auto-scaling or managed-scaling configuration.
- Relevant CloudWatch or EMR metrics.
- S3 folders containing each aggregate output.
- Terminal output showing benchmark commands and measured execution times.
- A final comparison table or chart.

Suggested screenshot names:

```text
Batch_EMR_Cluster_Configuration.png
Batch_EMR_1_Worker_Result.png
Batch_EMR_2_Workers_Result.png
Batch_EMR_4_Workers_Result.png
Batch_EMR_Scaling_Policy.png
Batch_EMR_CloudWatch_Metrics.png
Batch_S3_Aggregate_Outputs.png
Batch_Benchmark_Comparison.png
```

---

## 8. Integration with the Real-Time Layer

The batch and real-time pipelines are being aligned around the same event field names.

Confirmed shared naming:

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

Maryhelen's integration branch includes the schema alignment and Kinesis replay work in:

```text
Commit: f0ca5c0
Branch: maryhelen-integration
Description: Freeze endpoint schema and complete Kinesis replay
```

The real-time layer should produce recent-window results that can be compared with the batch aggregates, starting with:

```text
requests_per_endpoint
```

Later comparable metrics may include:

```text
traffic_by_hour
error_rates
requests_per_minute
```

The serving layer should preserve consistent column names so that historical and recent results can be queried or visualised together.

---

## 9. Git Collaboration Rules

To avoid losing work:

- Do not force-push shared branches.
- Keep Nalini's batch work on `nalini-batch-layer`.
- Keep Maryhelen's integration work on `maryhelen-integration`.
- Fetch before merging.
- Review commits before integration.
- Do not overwrite another team member's branch.
- Use clear commit messages.
- Keep benchmark evidence outside the Git repository when files are large.
- Store only code, configuration, documentation and small evidence summaries in Git.

Useful commands:

```powershell
git fetch origin
git status -sb
git log --oneline --decorate -10
```

To review Nalini's EMR fix:

```powershell
git show --stat f42e155
```

To review Maryhelen's schema and replay work:

```powershell
git show --stat f0ca5c0
```

---

## 10. Remaining Batch Tasks

- [x] Parse the raw Nginx log format.
- [x] Produce typed batch records.
- [x] Use `endpoint` as the shared request-path field.
- [x] Use `client_ip` as the shared client field.
- [x] Use `response_bytes` as the shared byte field.
- [x] Produce `requests_per_endpoint`.
- [x] Produce `traffic_by_hour`.
- [x] Produce `error_rates`.
- [x] Produce `baseline_rpm`.
- [x] Complete the 1-worker benchmark.
- [x] Complete the 2-worker benchmark.
- [x] Complete the 4-worker benchmark.
- [x] Restore the complete EMR setup script.
- [ ] Confirm the final timestamp output is ISO 8601 in exported results.
- [ ] Share exact benchmark timings and worker configurations.
- [ ] Organise benchmark screenshots in OneDrive.
- [ ] Confirm the final S3 paths for all four aggregate outputs.
- [ ] Create the final speedup and efficiency comparison.
- [ ] Validate batch outputs against comparable speed-layer results.
- [ ] Add final benchmark evidence and analysis to the report.

---

## 11. Next Coordination Point

Before starting the serving-layer integration, both team members should confirm:

1. The schema changes are committed in both active branches.
2. The same endpoint names and aggregate names are used.
3. Batch benchmark values and evidence are available.
4. Real-time sliding-window outputs are available.
5. The serving-layer storage and query approach is agreed.
6. The final visualisation requirements are assigned.

The immediate next development task on Maryhelen's side is the speed layer, beginning with requests per endpoint over recent sliding windows.

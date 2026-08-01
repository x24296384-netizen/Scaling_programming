# Amazon Athena SQL

These scripts register the final PySpark/EMR CSV outputs in Amazon Athena and
provide validation and demonstration queries.

## Expected S3 layout

Upload the result folders produced by `batch/batch_job.py` to:

```text
s3://scp-speed-results-25186396/batch/
├── requests_per_endpoint/
├── traffic_by_hour/
├── error_rates/
├── status_code_distribution/
├── response_byte_totals/
├── summary/
└── baseline_rpm/
```

Each folder should contain the Spark `part-*.csv` file. `_SUCCESS` files may
remain in the folders.

Do not place these historical EMR results inside `speed/batches/`. That prefix
contains immutable Lambda invocation deltas for the Speed Layer.

## Athena query-result location

Before running the SQL for the first time, configure the Athena query-result
location as:

```text
s3://scp-speed-results-25186396/athena-results/
```

In the Athena console, use the `primary` workgroup and set this path under the
query-result settings.

## Execution order

Open the files in this order:

```text
01_create_database.sql
02_create_external_tables.sql
03_create_views.sql
04_validation_queries.sql
05_demo_queries.sql
```

In the Athena query editor, execute **one SQL statement at a time**. Select a
complete statement (from its first keyword to its semicolon) and choose **Run**.
The scripts use database-qualified object names, so they do not depend on a
`USE` statement or on the database currently selected in the console.

The table script deliberately drops and recreates only the Glue Data Catalog
metadata. It does not delete any CSV object in Amazon S3.

## Expected schemas

| Table | Columns |
|---|---|
| `requests_per_endpoint` | `endpoint`, `total_requests` |
| `traffic_by_hour` | `hour`, `request_count` |
| `error_rates` | `endpoint`, `total_requests`, `error_count`, `error_rate` |
| `status_code_distribution` | `status_code`, `request_count` |
| `response_byte_totals` | `endpoint`, `total_response_bytes` |
| `batch_summary` | `total_valid_records`, `total_response_bytes` |
| `baseline_rpm` | `endpoint`, `avg_requests_per_minute` |

## Validation

`04_validation_queries.sql` checks that:

- endpoint totals equal `total_valid_records`;
- hourly totals equal `total_valid_records`;
- status-code totals equal `total_valid_records`;
- endpoint response-byte totals equal the batch summary byte total.

Every validation row should return:

```text
PASS
```

## Recommended evidence

Capture screenshots showing:

1. the database, seven external tables and two views;
2. the four reconciliation checks returning `PASS`;
3. the top endpoints query;
4. the error-rate query;
5. the baseline RPM query;
6. the `athena-results/` objects in Amazon S3.

Suggested filenames:

```text
Athena_Tables_And_Views_Created.png
Athena_Batch_Validation_PASS.png
Athena_Top_Endpoints_Query.png
Athena_Error_Rates_Query.png
Athena_Baseline_RPM_Query.png
S3_Athena_Query_Results.png
```

## AWS Deployment Status

The Athena infrastructure was created and validated in `us-east-1` on 31 July 2026.

- Database: `scp_web_logs_25186396`
- Workgroup: `primary`
- Query-results location: `s3://scp-speed-results-25186396/athena-results/`
- External tables created: 7
- Views created: 2
- Basic Athena query execution validated successfully.

The deployed views are:

- `endpoint_historical_summary`
- `batch_health_summary`

The external tables currently contain no historical batch rows because the
final EMR CSV outputs have not yet been copied to:

```text
s3://scp-speed-results-25186396/batch/
```

After the EMR outputs become available, the final validation and demonstration
queries will be executed to confirm that the aggregate totals reconcile and to
provide the final Athena evidence.

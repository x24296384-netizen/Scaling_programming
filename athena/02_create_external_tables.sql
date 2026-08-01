-- Scalable Cloud Programming
-- External Athena tables over the CSV folders produced by batch/batch_job.py.
--
-- Expected S3 layout:
-- s3://scp-speed-results-25186396/batch/
--   requests_per_endpoint/
--   traffic_by_hour/
--   error_rates/
--   status_code_distribution/
--   response_byte_totals/
--   summary/
--   baseline_rpm/
--
-- Spark writes a header to every part CSV, so each table skips one header line.


DROP TABLE IF EXISTS scp_web_logs_25186396.requests_per_endpoint;
CREATE EXTERNAL TABLE scp_web_logs_25186396.requests_per_endpoint (
    endpoint STRING,
    total_requests BIGINT
)
ROW FORMAT SERDE 'org.apache.hadoop.hive.serde2.OpenCSVSerde'
WITH SERDEPROPERTIES (
    'separatorChar' = ',',
    'quoteChar' = '"',
    'escapeChar' = '\\'
)
STORED AS TEXTFILE
LOCATION 's3://scp-speed-results-25186396/batch/requests_per_endpoint/'
TBLPROPERTIES (
    'skip.header.line.count' = '1',
    'classification' = 'csv'
);

DROP TABLE IF EXISTS scp_web_logs_25186396.traffic_by_hour;
CREATE EXTERNAL TABLE scp_web_logs_25186396.traffic_by_hour (
    hour INTEGER,
    request_count BIGINT
)
ROW FORMAT SERDE 'org.apache.hadoop.hive.serde2.OpenCSVSerde'
WITH SERDEPROPERTIES (
    'separatorChar' = ',',
    'quoteChar' = '"',
    'escapeChar' = '\\'
)
STORED AS TEXTFILE
LOCATION 's3://scp-speed-results-25186396/batch/traffic_by_hour/'
TBLPROPERTIES (
    'skip.header.line.count' = '1',
    'classification' = 'csv'
);

DROP TABLE IF EXISTS scp_web_logs_25186396.error_rates;
CREATE EXTERNAL TABLE scp_web_logs_25186396.error_rates (
    endpoint STRING,
    total_requests BIGINT,
    error_count BIGINT,
    error_rate DOUBLE
)
ROW FORMAT SERDE 'org.apache.hadoop.hive.serde2.OpenCSVSerde'
WITH SERDEPROPERTIES (
    'separatorChar' = ',',
    'quoteChar' = '"',
    'escapeChar' = '\\'
)
STORED AS TEXTFILE
LOCATION 's3://scp-speed-results-25186396/batch/error_rates/'
TBLPROPERTIES (
    'skip.header.line.count' = '1',
    'classification' = 'csv'
);

DROP TABLE IF EXISTS scp_web_logs_25186396.status_code_distribution;
CREATE EXTERNAL TABLE scp_web_logs_25186396.status_code_distribution (
    status_code INTEGER,
    request_count BIGINT
)
ROW FORMAT SERDE 'org.apache.hadoop.hive.serde2.OpenCSVSerde'
WITH SERDEPROPERTIES (
    'separatorChar' = ',',
    'quoteChar' = '"',
    'escapeChar' = '\\'
)
STORED AS TEXTFILE
LOCATION 's3://scp-speed-results-25186396/batch/status_code_distribution/'
TBLPROPERTIES (
    'skip.header.line.count' = '1',
    'classification' = 'csv'
);

DROP TABLE IF EXISTS scp_web_logs_25186396.response_byte_totals;
CREATE EXTERNAL TABLE scp_web_logs_25186396.response_byte_totals (
    endpoint STRING,
    total_response_bytes BIGINT
)
ROW FORMAT SERDE 'org.apache.hadoop.hive.serde2.OpenCSVSerde'
WITH SERDEPROPERTIES (
    'separatorChar' = ',',
    'quoteChar' = '"',
    'escapeChar' = '\\'
)
STORED AS TEXTFILE
LOCATION 's3://scp-speed-results-25186396/batch/response_byte_totals/'
TBLPROPERTIES (
    'skip.header.line.count' = '1',
    'classification' = 'csv'
);

DROP TABLE IF EXISTS scp_web_logs_25186396.batch_summary;
CREATE EXTERNAL TABLE scp_web_logs_25186396.batch_summary (
    total_valid_records BIGINT,
    total_response_bytes BIGINT
)
ROW FORMAT SERDE 'org.apache.hadoop.hive.serde2.OpenCSVSerde'
WITH SERDEPROPERTIES (
    'separatorChar' = ',',
    'quoteChar' = '"',
    'escapeChar' = '\\'
)
STORED AS TEXTFILE
LOCATION 's3://scp-speed-results-25186396/batch/summary/'
TBLPROPERTIES (
    'skip.header.line.count' = '1',
    'classification' = 'csv'
);

DROP TABLE IF EXISTS scp_web_logs_25186396.baseline_rpm;
CREATE EXTERNAL TABLE scp_web_logs_25186396.baseline_rpm (
    endpoint STRING,
    avg_requests_per_minute DOUBLE
)
ROW FORMAT SERDE 'org.apache.hadoop.hive.serde2.OpenCSVSerde'
WITH SERDEPROPERTIES (
    'separatorChar' = ',',
    'quoteChar' = '"',
    'escapeChar' = '\\'
)
STORED AS TEXTFILE
LOCATION 's3://scp-speed-results-25186396/batch/baseline_rpm/'
TBLPROPERTIES (
    'skip.header.line.count' = '1',
    'classification' = 'csv'
);

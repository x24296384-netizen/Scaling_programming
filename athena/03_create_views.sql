-- Reusable Athena views for the serving and demonstration layers.


CREATE OR REPLACE VIEW scp_web_logs_25186396.endpoint_historical_summary AS
SELECT
    requests.endpoint,
    requests.total_requests,
    errors.error_count,
    errors.error_rate,
    bytes.total_response_bytes,
    baseline.avg_requests_per_minute
FROM scp_web_logs_25186396.requests_per_endpoint AS requests
LEFT JOIN scp_web_logs_25186396.error_rates AS errors
    ON requests.endpoint = errors.endpoint
LEFT JOIN scp_web_logs_25186396.response_byte_totals AS bytes
    ON requests.endpoint = bytes.endpoint
LEFT JOIN scp_web_logs_25186396.baseline_rpm AS baseline
    ON requests.endpoint = baseline.endpoint;

CREATE OR REPLACE VIEW scp_web_logs_25186396.batch_health_summary AS
WITH status_totals AS (
    SELECT
        SUM(request_count) AS status_record_count,
        SUM(
            CASE
                WHEN status_code >= 400 THEN request_count
                ELSE 0
            END
        ) AS total_error_responses
    FROM scp_web_logs_25186396.status_code_distribution
)
SELECT
    summary.total_valid_records,
    summary.total_response_bytes,
    status.status_record_count,
    status.total_error_responses,
    CASE
        WHEN summary.total_valid_records = 0 THEN 0.0
        ELSE CAST(status.total_error_responses AS DOUBLE)
             / CAST(summary.total_valid_records AS DOUBLE)
    END AS overall_error_rate
FROM scp_web_logs_25186396.batch_summary AS summary
CROSS JOIN status_totals AS status;

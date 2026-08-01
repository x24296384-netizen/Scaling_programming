-- Run after creating the database, tables and views.
-- These checks prove that the independent Spark aggregates reconcile with
-- the authoritative batch summary.


SHOW TABLES IN scp_web_logs_25186396;

SELECT *
FROM scp_web_logs_25186396.batch_summary;

SELECT *
FROM scp_web_logs_25186396.batch_health_summary;

WITH expected AS (
    SELECT
        total_valid_records,
        total_response_bytes
    FROM scp_web_logs_25186396.batch_summary
),
checks AS (
    SELECT
        'requests_per_endpoint record total' AS validation_check,
        expected.total_valid_records AS expected_value,
        SUM(requests.total_requests) AS actual_value
    FROM expected
    CROSS JOIN scp_web_logs_25186396.requests_per_endpoint AS requests
    GROUP BY expected.total_valid_records

    UNION ALL

    SELECT
        'traffic_by_hour record total' AS validation_check,
        expected.total_valid_records AS expected_value,
        SUM(traffic.request_count) AS actual_value
    FROM expected
    CROSS JOIN scp_web_logs_25186396.traffic_by_hour AS traffic
    GROUP BY expected.total_valid_records

    UNION ALL

    SELECT
        'status_code_distribution record total' AS validation_check,
        expected.total_valid_records AS expected_value,
        SUM(statuses.request_count) AS actual_value
    FROM expected
    CROSS JOIN scp_web_logs_25186396.status_code_distribution AS statuses
    GROUP BY expected.total_valid_records

    UNION ALL

    SELECT
        'response_byte_totals byte total' AS validation_check,
        expected.total_response_bytes AS expected_value,
        SUM(bytes.total_response_bytes) AS actual_value
    FROM expected
    CROSS JOIN scp_web_logs_25186396.response_byte_totals AS bytes
    GROUP BY expected.total_response_bytes
)
SELECT
    validation_check,
    expected_value,
    actual_value,
    CASE
        WHEN expected_value = actual_value THEN 'PASS'
        ELSE 'FAIL'
    END AS validation_status
FROM checks
ORDER BY validation_check;

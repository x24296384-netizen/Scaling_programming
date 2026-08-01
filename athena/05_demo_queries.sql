-- Queries suitable for the final demonstration and evidence screenshots.


-- 1. Most frequently requested endpoints.
SELECT
    endpoint,
    total_requests
FROM scp_web_logs_25186396.requests_per_endpoint
ORDER BY total_requests DESC
LIMIT 10;

-- 2. Historical traffic by hour of day.
SELECT
    hour,
    request_count
FROM scp_web_logs_25186396.traffic_by_hour
ORDER BY hour;

-- 3. Endpoints with the greatest error volume and their error rate.
SELECT
    endpoint,
    total_requests,
    error_count,
    ROUND(error_rate * 100, 4) AS error_percentage
FROM scp_web_logs_25186396.error_rates
ORDER BY error_count DESC, error_rate DESC, total_requests DESC
LIMIT 10;

-- 4. HTTP status-code distribution.
SELECT
    status_code,
    request_count,
    ROUND(
        100.0 * request_count
        / SUM(request_count) OVER (),
        4
    ) AS percentage_of_requests
FROM scp_web_logs_25186396.status_code_distribution
ORDER BY status_code;

-- 5. Endpoints responsible for the largest response-byte volume.
SELECT
    endpoint,
    total_response_bytes
FROM scp_web_logs_25186396.response_byte_totals
ORDER BY total_response_bytes DESC
LIMIT 10;

-- 6. Historical requests-per-minute baselines used by the serving layer.
SELECT
    endpoint,
    ROUND(avg_requests_per_minute, 4) AS avg_requests_per_minute
FROM scp_web_logs_25186396.baseline_rpm
ORDER BY avg_requests_per_minute DESC
LIMIT 10;

-- 7. Combined historical endpoint view.
SELECT
    endpoint,
    total_requests,
    error_count,
    ROUND(error_rate * 100, 4) AS error_percentage,
    total_response_bytes,
    ROUND(avg_requests_per_minute, 4) AS avg_requests_per_minute
FROM scp_web_logs_25186396.endpoint_historical_summary
ORDER BY total_requests DESC
LIMIT 10;

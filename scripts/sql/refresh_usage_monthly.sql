-- Reference template only; the phase-1 usage-worker is a planned implementation.
-- Render only validated ISO dates, partition names, UUID refresh IDs and hex SHA-256.
-- Preparation only: serialize jobs for this period, load and validate the temporary partition.
-- Ordinary reads continue using the previous formal partition.
-- Publish separately after validation; see docs/daily-monthly-summary-design.md.
SET time_zone = '+00:00';
SET enable_insert_strict = true;

-- All dates from month_start through min(next_month, tomorrow) must have published state,
-- including zero-row days. Lock the month's daily inputs until monthly publication.
ALTER TABLE metering.usage_monthly ADD PARTITION IF NOT EXISTS pm{{yyyymm}}
VALUES [('{{month_start}}'), ('{{next_month}}'));
ALTER TABLE metering.usage_monthly ADD TEMPORARY PARTITION tp{{refresh_token}}
VALUES [('{{month_start}}'), ('{{next_month}}'));
INSERT INTO metering.usage_monthly TEMPORARY PARTITION (tp{{refresh_token}})
(stat_month, customer_id, application_id, api_key_id, service_id, model, usage_source, request_count, started_request_count, usage_observed_request_count, usage_missing_request_count, eligible_usage_request_count, ineligible_usage_request_count, http_2xx_request_count, http_error_request_count, terminal_missing_request_count, input_tokens, output_tokens, total_tokens, cached_read_tokens, cached_write_tokens, reasoning_tokens, reported_total_tokens, cached_read_known_requests, cached_write_known_requests, reasoning_known_requests, reported_total_known_requests, refresh_id, source_watermark, refreshed_at)
SELECT CAST('{{month_start}}' AS DATE), customer_id, application_id, api_key_id, service_id, model, usage_source,
       SUM(request_count) AS request_count,
       SUM(started_request_count) AS started_request_count,
       SUM(usage_observed_request_count) AS usage_observed_request_count,
       SUM(usage_missing_request_count) AS usage_missing_request_count,
       SUM(eligible_usage_request_count) AS eligible_usage_request_count,
       SUM(ineligible_usage_request_count) AS ineligible_usage_request_count,
       SUM(http_2xx_request_count) AS http_2xx_request_count,
       SUM(http_error_request_count) AS http_error_request_count,
       SUM(terminal_missing_request_count) AS terminal_missing_request_count,
       SUM(input_tokens) AS input_tokens,
       SUM(output_tokens) AS output_tokens,
       SUM(total_tokens) AS total_tokens,
       SUM(cached_read_tokens) AS cached_read_tokens,
       SUM(cached_write_tokens) AS cached_write_tokens,
       SUM(reasoning_tokens) AS reasoning_tokens,
       SUM(reported_total_tokens) AS reported_total_tokens,
       SUM(cached_read_known_requests) AS cached_read_known_requests,
       SUM(cached_write_known_requests) AS cached_write_known_requests,
       SUM(reasoning_known_requests) AS reasoning_known_requests,
       SUM(reported_total_known_requests) AS reported_total_known_requests,
       '{{refresh_id}}', '{{source_watermark}}', NOW(3)
FROM metering.usage_daily
WHERE stat_date >= '{{month_start}}' AND stat_date < '{{next_month}}'
GROUP BY customer_id, application_id, api_key_id, service_id, model, usage_source;

-- Reference template only; the phase-1 usage-worker is a planned implementation.
-- Render only validated ISO dates, partition names, UUID refresh IDs and hex SHA-256.
-- Preparation only: one protected Go publisher serializes all periods.
-- Persist captured inputs in Doris, then load and validate the temporary partition.
-- Ordinary reads continue using the previous formal partition.
-- Publish separately after validation; see docs/daily-monthly-summary-design.md.
SET time_zone = '+00:00';
SET enable_insert_strict = true;

ALTER TABLE metering.usage_daily ADD PARTITION IF NOT EXISTS pd{{yyyymmdd}}
VALUES [('{{stat_date}}'), ('{{next_date}}'));
ALTER TABLE metering.usage_daily ADD TEMPORARY PARTITION tp{{refresh_token}}
VALUES [('{{stat_date}}'), ('{{next_date}}'));
INSERT INTO metering.usage_daily TEMPORARY PARTITION (tp{{refresh_token}})
(stat_date, customer_id, application_id, api_key_id, service_id, model, usage_source, request_count, started_request_count, usage_observed_request_count, usage_missing_request_count, eligible_usage_request_count, ineligible_usage_request_count, http_2xx_request_count, http_error_request_count, terminal_missing_request_count, input_tokens, output_tokens, total_tokens, cached_read_tokens, cached_write_tokens, reasoning_tokens, reported_total_tokens, cached_read_known_requests, cached_write_known_requests, reasoning_known_requests, reported_total_known_requests, refresh_id, source_watermark, refreshed_at)
SELECT request_date, customer_id, application_id, api_key_id, service_id, model, usage_source,
       COUNT(*) AS request_count,
       SUM(CASE WHEN start_variants = 1 THEN 1 ELSE 0 END) AS started_request_count,
       SUM(CASE WHEN usage_variants = 1 THEN 1 ELSE 0 END) AS usage_observed_request_count,
       SUM(CASE WHEN usage_variants = 0 THEN 1 ELSE 0 END) AS usage_missing_request_count,
       SUM(CASE WHEN usage_variants = 1 AND usage_eligible = 1 AND input_tokens IS NOT NULL AND output_tokens IS NOT NULL THEN 1 ELSE 0 END) AS eligible_usage_request_count,
       SUM(CASE WHEN usage_variants = 1 AND usage_eligible = 1 AND input_tokens IS NOT NULL AND output_tokens IS NOT NULL THEN 0 ELSE 1 END) AS ineligible_usage_request_count,
       SUM(CASE WHEN http_status BETWEEN 200 AND 299 THEN 1 ELSE 0 END) AS http_2xx_request_count,
       SUM(CASE WHEN http_status >= 400 THEN 1 ELSE 0 END) AS http_error_request_count,
       SUM(CASE WHEN terminal_observed IS NULL OR terminal_observed = 0 THEN 1 ELSE 0 END) AS terminal_missing_request_count,
       SUM(CASE WHEN usage_variants = 1 AND usage_eligible = 1 AND input_tokens IS NOT NULL AND output_tokens IS NOT NULL THEN CAST(input_tokens AS DECIMAL(38, 0)) END) AS input_tokens,
       SUM(CASE WHEN usage_variants = 1 AND usage_eligible = 1 AND input_tokens IS NOT NULL AND output_tokens IS NOT NULL THEN CAST(output_tokens AS DECIMAL(38, 0)) END) AS output_tokens,
       SUM(CASE WHEN usage_variants = 1 AND usage_eligible = 1 AND input_tokens IS NOT NULL AND output_tokens IS NOT NULL THEN CAST(input_tokens AS DECIMAL(38, 0)) + CAST(output_tokens AS DECIMAL(38, 0)) END) AS total_tokens,
       SUM(CASE WHEN usage_variants = 1 AND usage_eligible = 1 AND input_tokens IS NOT NULL AND output_tokens IS NOT NULL THEN CAST(cached_read_tokens AS DECIMAL(38, 0)) END) AS cached_read_tokens,
       SUM(CASE WHEN usage_variants = 1 AND usage_eligible = 1 AND input_tokens IS NOT NULL AND output_tokens IS NOT NULL THEN CAST(cached_write_tokens AS DECIMAL(38, 0)) END) AS cached_write_tokens,
       SUM(CASE WHEN usage_variants = 1 AND usage_eligible = 1 AND input_tokens IS NOT NULL AND output_tokens IS NOT NULL THEN CAST(reasoning_tokens AS DECIMAL(38, 0)) END) AS reasoning_tokens,
       SUM(CASE WHEN usage_variants = 1 AND usage_eligible = 1 AND input_tokens IS NOT NULL AND output_tokens IS NOT NULL THEN CAST(reported_total_tokens AS DECIMAL(38, 0)) END) AS reported_total_tokens,
       SUM(CASE WHEN usage_variants = 1 AND usage_eligible = 1 AND input_tokens IS NOT NULL AND output_tokens IS NOT NULL AND cached_read_tokens IS NOT NULL THEN 1 ELSE 0 END) AS cached_read_known_requests,
       SUM(CASE WHEN usage_variants = 1 AND usage_eligible = 1 AND input_tokens IS NOT NULL AND output_tokens IS NOT NULL AND cached_write_tokens IS NOT NULL THEN 1 ELSE 0 END) AS cached_write_known_requests,
       SUM(CASE WHEN usage_variants = 1 AND usage_eligible = 1 AND input_tokens IS NOT NULL AND output_tokens IS NOT NULL AND reasoning_tokens IS NOT NULL THEN 1 ELSE 0 END) AS reasoning_known_requests,
       SUM(CASE WHEN usage_variants = 1 AND usage_eligible = 1 AND input_tokens IS NOT NULL AND output_tokens IS NOT NULL AND reported_total_tokens IS NOT NULL THEN 1 ELSE 0 END) AS reported_total_known_requests,
       '{{refresh_id}}', '{{source_watermark}}', NOW(3)
FROM metering.request_statistics
WHERE request_date = '{{stat_date}}' AND has_conflict = 0 AND identity_status = 'known'
  AND customer_id IS NOT NULL AND customer_id <> ''
  AND application_id IS NOT NULL AND application_id <> ''
  AND api_key_id IS NOT NULL AND api_key_id <> '' AND service_id <> ''
GROUP BY request_date, customer_id, application_id, api_key_id, service_id, model, usage_source;

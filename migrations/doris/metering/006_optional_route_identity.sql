-- Optional diagnostic route names are not stable request identity.
-- Apply after 001/002 and before running the upgraded Consumer.
-- Derive equality keys from typed fields for ALL rows, including legacy hashes.
-- No raw payloads/hashes, immutable conflict marks, or summaries are rewritten.
CREATE OR REPLACE VIEW metering.requests AS
SELECT a.request_date, a.logical_request_id, a.start_variants, a.usage_variants,
       a.request_started_at_ms, a.x_request_id, a.customer_id, a.service_id,
       CASE WHEN a.usage_variants = 1 THEN a.response_model END AS response_model,
       CASE WHEN a.usage_variants = 0 THEN 'not_observed'
            WHEN a.usage_variants = 1 THEN a.usage_status ELSE 'conflict' END AS usage_status,
       CASE WHEN a.usage_variants = 0 THEN 'not_observed'
            WHEN a.usage_variants = 1 THEN a.observation_status ELSE 'conflict' END AS observation_status,
       CASE WHEN a.usage_variants = 1 THEN a.input_tokens END AS input_tokens,
       CASE WHEN a.usage_variants = 1 THEN a.output_tokens END AS output_tokens,
       CASE WHEN a.usage_variants = 1 THEN a.request_finished_at_ms END AS request_finished_at_ms,
       CASE WHEN a.usage_variants = 1 THEN a.terminal_observed ELSE 0 END AS terminal_observed,
       CASE WHEN a.usage_variants = 1 THEN a.business_outcome END AS business_outcome,
       CASE WHEN a.start_variants > 1 OR a.usage_variants > 1
                 OR a.started_count > 1 OR a.date_count > 1 OR a.identity_count > 1
                 OR m.logical_request_id IS NOT NULL THEN 1 ELSE 0 END AS has_conflict
FROM (
    SELECT logical_request_id, MIN(request_date) AS request_date,
           SUM(CASE WHEN event_type = 'request_started' THEN 1 ELSE 0 END) AS start_variants,
           SUM(CASE WHEN event_type = 'usage_observed' THEN 1 ELSE 0 END) AS usage_variants,
           COUNT(DISTINCT request_started_at_ms) AS started_count,
           COUNT(DISTINCT request_date) AS date_count,
           COUNT(DISTINCT CAST(JSON_ARRAY(service_id, api_key_id, customer_id, application_id, identity_status, config_version, gateway_instance_id, adapter_id, x_request_id) AS STRING)) AS identity_count,
           MIN(request_started_at_ms) AS request_started_at_ms,
           MAX(x_request_id) AS x_request_id,
           MAX(customer_id) AS customer_id,
           MAX(service_id) AS service_id,
           MAX(CASE WHEN event_type = 'usage_observed' THEN response_model END) AS response_model,
           MAX(CASE WHEN event_type = 'usage_observed' THEN usage_status END) AS usage_status,
           MAX(CASE WHEN event_type = 'usage_observed' THEN observation_status END) AS observation_status,
           MAX(CASE WHEN event_type = 'usage_observed' THEN input_tokens END) AS input_tokens,
           MAX(CASE WHEN event_type = 'usage_observed' THEN output_tokens END) AS output_tokens,
           MAX(CASE WHEN event_type = 'usage_observed' THEN request_finished_at_ms END) AS request_finished_at_ms,
           MAX(CASE WHEN event_type = 'usage_observed' THEN terminal_observed END) AS terminal_observed,
           MAX(CASE WHEN event_type = 'usage_observed' THEN business_outcome END) AS business_outcome
    FROM metering.event_variants
    GROUP BY logical_request_id
) a
LEFT JOIN metering.request_conflict_ids m ON a.logical_request_id = m.logical_request_id;

CREATE OR REPLACE VIEW metering.request_statistics AS
SELECT a.*,
       CASE WHEN a.start_variants > 1 OR a.usage_variants > 1
                 OR a.started_count > 1 OR a.identity_count > 1
                 OR c.logical_request_id IS NOT NULL THEN 1 ELSE 0 END AS has_conflict,
       COALESCE(NULLIF(a.response_model, ''), NULLIF(a.request_model, ''), '') AS model,
       CASE WHEN a.usage_variants = 0 THEN 'missing'
            WHEN a.input_tokens IS NULL OR a.output_tokens IS NULL THEN 'unknown'
            WHEN a.input_tokens_source = 'reported' AND a.output_tokens_source = 'reported' THEN 'reported'
            WHEN a.input_tokens_source = 'estimated_model' AND a.output_tokens_source = 'estimated_model' THEN 'estimated_model'
            WHEN a.input_tokens_source = 'estimated_generic' AND a.output_tokens_source = 'estimated_generic' THEN 'estimated_generic'
            ELSE 'mixed' END AS usage_source
FROM (
    SELECT request_date, logical_request_id,
           SUM(CASE WHEN event_type = 'request_started' THEN 1 ELSE 0 END) AS start_variants,
           SUM(CASE WHEN event_type = 'usage_observed' THEN 1 ELSE 0 END) AS usage_variants,
           COUNT(DISTINCT request_started_at_ms) AS started_count,
           COUNT(DISTINCT CAST(JSON_ARRAY(service_id, api_key_id, customer_id, application_id, identity_status, config_version, gateway_instance_id, adapter_id, x_request_id) AS STRING)) AS identity_count,
           MAX(customer_id) AS customer_id, MAX(application_id) AS application_id,
           MAX(api_key_id) AS api_key_id, MAX(service_id) AS service_id,
           MAX(identity_status) AS identity_status,
           MAX(request_model) AS request_model,
           MAX(CASE WHEN event_type = 'usage_observed' THEN response_model END) AS response_model,
           MAX(CASE WHEN event_type = 'usage_observed' THEN usage_eligible END) AS usage_eligible,
           MAX(CASE WHEN event_type = 'usage_observed' THEN http_status END) AS http_status,
           MAX(CASE WHEN event_type = 'usage_observed' THEN terminal_observed END) AS terminal_observed,
           MAX(CASE WHEN event_type = 'usage_observed' THEN input_tokens END) AS input_tokens,
           MAX(CASE WHEN event_type = 'usage_observed' THEN output_tokens END) AS output_tokens,
           MAX(CASE WHEN event_type = 'usage_observed' THEN input_tokens_source END) AS input_tokens_source,
           MAX(CASE WHEN event_type = 'usage_observed' THEN output_tokens_source END) AS output_tokens_source,
           MAX(CASE WHEN event_type = 'usage_observed' THEN cached_read_tokens END) AS cached_read_tokens,
           MAX(CASE WHEN event_type = 'usage_observed' THEN cached_write_tokens END) AS cached_write_tokens,
           MAX(CASE WHEN event_type = 'usage_observed' THEN reasoning_tokens END) AS reasoning_tokens,
           MAX(CASE WHEN event_type = 'usage_observed' THEN reported_total_tokens END) AS reported_total_tokens
    FROM metering.events_raw
    GROUP BY request_date, logical_request_id
) a
LEFT JOIN metering.request_conflict_ids c ON a.logical_request_id = c.logical_request_id;

SELECT period_type, period_start, status,
       JSON_EXTRACT_STRING(record_json, '$.success_refresh_id') AS success_refresh_id,
       JSON_EXTRACT_STRING(record_json, '$.refreshed_at') AS refreshed_at,
       JSON_EXTRACT_STRING(record_json, '$.coverage_start') AS coverage_start,
       JSON_EXTRACT_STRING(record_json, '$.coverage_end') AS coverage_end,
       JSON_EXTRACT_STRING(record_json, '$.error_code') AS error_code
FROM metering.summary_mv_refresh_state
ORDER BY period_type, period_start;
SELECT period_type, period_start, refresh_id, status, updated_at
FROM metering.summary_mv_refresh_runs
WHERE status IN ('PENDING', 'RUNNING', 'UNCERTAIN', 'FAILED')
ORDER BY updated_at;

-- Source-ingestion gaps are independent of successful summary calculations.
SELECT group_id,kafka_topic,kafka_partition,COUNT(*) AS ingestion_gap_count
FROM metering.consumer_partition_gaps
GROUP BY group_id,kafka_topic,kafka_partition;

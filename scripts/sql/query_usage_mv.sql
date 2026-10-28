-- Internal examples only. Bind authenticated customer IDs in a future query API.
SET time_zone = '+00:00';
-- Provider totals, including unknown customers; current day is never included.
SELECT stat_date, cluster_name, deployment_environment, provider_id,
       SUM(request_count) AS request_count, SUM(total_tokens) AS total_tokens,
       SUM(eligible_usage_request_count) AS known_usage_requests
FROM metering.usage_daily_mv
WHERE provider_id IS NOT NULL AND stat_date < CURRENT_DATE()
  AND stat_date >= DATE_SUB(DATE_TRUNC(CURRENT_DATE(), 'month'), INTERVAL 12 MONTH)
GROUP BY stat_date, cluster_name, deployment_environment, provider_id;
-- API Key monthly totals exclude unknown customer/key attribution.
SELECT stat_month, cluster_name, deployment_environment, customer_id, api_key_id,
       SUM(request_count) AS request_count, SUM(total_tokens) AS total_tokens
FROM metering.usage_monthly_mv
WHERE customer_id IS NOT NULL AND api_key_id IS NOT NULL
  AND stat_month >= DATE_SUB(DATE_TRUNC(CURRENT_DATE(), 'month'), INTERVAL 12 MONTH)
GROUP BY stat_month, cluster_name, deployment_environment, customer_id, api_key_id;
-- Check period status alongside results. RUNNING/GAP are not confirmed zero.

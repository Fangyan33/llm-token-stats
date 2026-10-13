-- Existing deployments: stop any archive worker / external partition cleanup first.
-- CREATE TABLE IF NOT EXISTS does not update an existing table's retention.
-- Enable FE dynamic_partition_enable on every FE; persist it in fe.conf.
-- Rolling year = 365 UTC dates, including today; already deleted data is not restored.
ALTER TABLE metering.events_raw SET (
    "dynamic_partition.enable" = "true",
    "dynamic_partition.time_unit" = "DAY",
    "dynamic_partition.start" = "-364",
    "dynamic_partition.end" = "2",
    "dynamic_partition.prefix" = "p",
    "dynamic_partition.buckets" = "16",
    "dynamic_partition.create_history_partition" = "true",
    "dynamic_partition.time_zone" = "UTC"
);
ALTER TABLE metering.ingest_rejections SET (
    "dynamic_partition.enable" = "true",
    "dynamic_partition.time_unit" = "DAY",
    "dynamic_partition.start" = "-364",
    "dynamic_partition.end" = "2",
    "dynamic_partition.prefix" = "p",
    "dynamic_partition.buckets" = "8",
    "dynamic_partition.create_history_partition" = "true",
    "dynamic_partition.time_zone" = "UTC"
);

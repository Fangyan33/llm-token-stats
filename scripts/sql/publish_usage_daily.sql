-- Execute only after validation and acquiring the short publication lock.
-- Mark PostgreSQL publication_uncertain=true before this statement.
-- Empty results must also replace the old partition, removing obsolete groups.
ALTER TABLE metering.usage_daily REPLACE PARTITION (pd{{yyyymmdd}})
WITH TEMPORARY PARTITION (tp{{refresh_token}})
PROPERTIES ("strict_range" = "true", "use_temp_partition_name" = "false");
-- Then commit the state and, for a day, mark the month dirty in one PG transaction.

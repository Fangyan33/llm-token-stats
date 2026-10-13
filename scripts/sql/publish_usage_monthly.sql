-- Execute only after validation under the protected single Go publisher.
-- Persist PREPARED/PUBLISHING and Doris state publication_uncertain=true;
-- confirm visibility and publisher ownership before executing this DDL.
-- DDL and state writes are separate recoverable operations, not one transaction.
-- See docs/doris-summary-coordination.md for the required recovery protocol.
-- Empty results must also replace the old partition, removing obsolete groups.
ALTER TABLE metering.usage_monthly REPLACE PARTITION (pm{{yyyymm}})
WITH TEMPORARY PARTITION (tp{{refresh_token}})
PROPERTIES ("strict_range" = "true", "use_temp_partition_name" = "false");
-- Verify formal PartitionId, refresh_id, row count and validation digest, even if empty.
-- Publish Doris state/version/coverage first, then mark this run PUBLISHED.
-- Recover any interruption before processing new work; do not guess DDL success.
-- Monthly refresh is detected from the persisted daily publication-version vector.

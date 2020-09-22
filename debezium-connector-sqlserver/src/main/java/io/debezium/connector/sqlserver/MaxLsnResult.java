package io.debezium.connector.sqlserver;

/**
 * Stores the result from querying the lsn_time_mapping for the highest LSNs.
 * Not all LSNs are associated with a transaction; they can also be a record for the completion of LSN processing in periods of low or no change activity.
 * In the case of being a record for completion, there's no need to query the cdc tables as there will be no changes.
 */
public class MaxLsnResult {

    /**
     * The highest l
     */
    private final Lsn maxLsn;
    private final Lsn maxTransactionalLsn;

    public MaxLsnResult(Lsn maxLsn, Lsn maxTransactionalLsn) {
        this.maxLsn = maxLsn;
        this.maxTransactionalLsn = maxTransactionalLsn;
    }

    public Lsn getMaxLsn() {
        return maxLsn;
    }

    public Lsn getMaxTransactionalLsn() {
        return maxTransactionalLsn;
    }
}

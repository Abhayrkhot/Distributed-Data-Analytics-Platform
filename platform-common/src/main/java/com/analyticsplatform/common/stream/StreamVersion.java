package com.analyticsplatform.common.stream;

/**
 * Version ordering for the {@code ReplacingMergeTree} streaming sink.
 *
 * <p>ClickHouse resolves duplicate rows by keeping the one with the highest version, so the
 * version must be <em>deterministic</em>: replaying a microbatch has to produce the same value it
 * produced the first time. An earlier design used {@code updated_at DateTime DEFAULT now()},
 * which is not — a replay stamps a different version and which physical row survives the merge
 * becomes arbitrary.
 *
 * <p>Spark's {@code foreachBatch} batch id <em>is</em> deterministic on replay from a checkpoint,
 * but it is scoped to a single checkpoint lineage: a fresh checkpoint restarts numbering at 0.
 * On its own that is actively dangerous — a new query writing {@code batch_id=0} would compare
 * lower than an existing {@code batch_id=42} and silently fail to replace it. So a monotonic
 * epoch, allocated transactionally in Postgres, dominates the ordering:
 *
 * <pre>version = epoch * 2^32 + batchId</pre>
 *
 * <p>The layout packs the epoch into the high 32 bits and the batch id into the low 32, giving
 * lexicographic ordering by (epoch, batchId) with a single integer comparison.
 *
 * <p>All values are treated as signed Java longs and kept strictly positive, so the version is
 * safe to store in a ClickHouse {@code UInt64} and to compare in Java without unsigned tricks.
 */
public final class StreamVersion {

    /** Batch ids occupy the low 32 bits: 0 .. 2^32-1. */
    public static final long MAX_BATCH_ID = 0xFFFF_FFFFL;

    /**
     * Epochs occupy the high 31 bits. Capping at 2^31-1 (rather than 2^32-1) keeps the packed
     * value inside the positive range of a signed 64-bit long, so ordering never depends on
     * unsigned comparison.
     */
    public static final long MAX_EPOCH = 0x7FFF_FFFFL;

    /** Epochs come from a Postgres sequence starting at 1; 0 signals "unallocated". */
    public static final long MIN_EPOCH = 1L;

    private static final int EPOCH_SHIFT = 32;

    private StreamVersion() {
    }

    /**
     * Packs an epoch and batch id into a single ordered version.
     *
     * @throws IllegalArgumentException if either component is outside its supported range. This
     *         fails closed on purpose: silently truncating an out-of-range epoch would corrupt
     *         the ordering that replacement correctness depends on.
     */
    public static long of(long epoch, long batchId) {
        if (epoch < MIN_EPOCH || epoch > MAX_EPOCH) {
            throw new IllegalArgumentException(
                    "epoch out of range [" + MIN_EPOCH + ", " + MAX_EPOCH + "]: " + epoch);
        }
        if (batchId < 0 || batchId > MAX_BATCH_ID) {
            throw new IllegalArgumentException(
                    "batchId out of range [0, " + MAX_BATCH_ID + "]: " + batchId);
        }
        return (epoch << EPOCH_SHIFT) | batchId;
    }

    /** Recovers the epoch from a packed version. */
    public static long epochOf(long version) {
        requirePositive(version);
        return version >>> EPOCH_SHIFT;
    }

    /** Recovers the batch id from a packed version. */
    public static long batchIdOf(long version) {
        requirePositive(version);
        return version & MAX_BATCH_ID;
    }

    /**
     * True when {@code candidate} should replace {@code existing} in the sink.
     *
     * <p>Equality returns false: a replayed microbatch produces an identical version, and treating
     * that as a replacement would be a no-op that merely looks like progress.
     */
    public static boolean supersedes(long candidate, long existing) {
        return candidate > existing;
    }

    private static void requirePositive(long version) {
        if (version <= 0) {
            throw new IllegalArgumentException("version must be positive: " + version);
        }
    }
}

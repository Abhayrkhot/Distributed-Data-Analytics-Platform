package com.analyticsplatform.common.stream;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import net.jqwik.api.Arbitraries;
import net.jqwik.api.Arbitrary;
import net.jqwik.api.Assume;
import net.jqwik.api.ForAll;
import net.jqwik.api.Property;
import net.jqwik.api.Provide;
import net.jqwik.api.Label;

/**
 * Generated coverage for {@link StreamVersion} (§17).
 *
 * <p>Hand-written examples confirm the cases we already thought of; these properties look for the
 * ones we did not. jqwik reports the seed and a minimized counterexample on failure, so anything
 * found here is reproducible rather than a one-off.
 *
 * <p>Generators deliberately mix uniform sampling with concentrated sampling at the packing
 * boundaries (0, 1, 2^32-2, 2^32-1, MAX_EPOCH), because a 32-bit shift is overwhelmingly most
 * likely to be wrong exactly there and uniform longs would almost never land on them.
 */
@Label("StreamVersion")
class StreamVersionProperties {

    @Provide
    Arbitrary<Long> epochs() {
        Arbitrary<Long> boundaries = Arbitraries.of(
                StreamVersion.MIN_EPOCH,
                StreamVersion.MIN_EPOCH + 1,
                StreamVersion.MAX_EPOCH - 1,
                StreamVersion.MAX_EPOCH);
        Arbitrary<Long> uniform =
                Arbitraries.longs().between(StreamVersion.MIN_EPOCH, StreamVersion.MAX_EPOCH);
        return Arbitraries.oneOf(boundaries, uniform);
    }

    @Provide
    Arbitrary<Long> batchIds() {
        Arbitrary<Long> boundaries = Arbitraries.of(
                0L, 1L, 2L,
                StreamVersion.MAX_BATCH_ID - 1,
                StreamVersion.MAX_BATCH_ID);
        Arbitrary<Long> uniform = Arbitraries.longs().between(0L, StreamVersion.MAX_BATCH_ID);
        return Arbitraries.oneOf(boundaries, uniform);
    }

    /**
     * The invariant the whole epoch mechanism exists to guarantee: a newer epoch outranks an older
     * one regardless of batch ids, so a fresh checkpoint restarting at batch 0 still wins.
     */
    @Property
    void aHigherEpochAlwaysOutranksALowerOne(
            @ForAll("epochs") long epochA, @ForAll("epochs") long epochB,
            @ForAll("batchIds") long batchA, @ForAll("batchIds") long batchB) {

        Assume.that(epochA > epochB);

        assertThat(StreamVersion.of(epochA, batchA))
                .isGreaterThan(StreamVersion.of(epochB, batchB));
    }

    @Property
    void withinAnEpochHigherBatchIdsOutrank(
            @ForAll("epochs") long epoch,
            @ForAll("batchIds") long batchA, @ForAll("batchIds") long batchB) {

        Assume.that(batchA > batchB);

        assertThat(StreamVersion.of(epoch, batchA))
                .isGreaterThan(StreamVersion.of(epoch, batchB));
    }

    /** Replay produces an identical version, which is what makes the merge a no-op. */
    @Property
    void packingIsDeterministic(@ForAll("epochs") long epoch, @ForAll("batchIds") long batchId) {
        assertThat(StreamVersion.of(epoch, batchId))
                .isEqualTo(StreamVersion.of(epoch, batchId));
        assertThat(StreamVersion.supersedes(
                StreamVersion.of(epoch, batchId), StreamVersion.of(epoch, batchId))).isFalse();
    }

    @Property
    void componentsAlwaysSurviveRoundTrip(
            @ForAll("epochs") long epoch, @ForAll("batchIds") long batchId) {

        long version = StreamVersion.of(epoch, batchId);

        assertThat(StreamVersion.epochOf(version)).isEqualTo(epoch);
        assertThat(StreamVersion.batchIdOf(version)).isEqualTo(batchId);
    }

    /** Every representable version stays inside the positive signed range. */
    @Property
    void versionsAreAlwaysPositive(
            @ForAll("epochs") long epoch, @ForAll("batchIds") long batchId) {
        assertThat(StreamVersion.of(epoch, batchId)).isPositive();
    }

    @Property
    void rejectsOutOfRangeEpochs(@ForAll long epoch, @ForAll("batchIds") long batchId) {
        Assume.that(epoch < StreamVersion.MIN_EPOCH || epoch > StreamVersion.MAX_EPOCH);

        assertThatThrownBy(() -> StreamVersion.of(epoch, batchId))
                .isInstanceOf(IllegalArgumentException.class);
    }

    @Property
    void rejectsOutOfRangeBatchIds(@ForAll("epochs") long epoch, @ForAll long batchId) {
        Assume.that(batchId < 0 || batchId > StreamVersion.MAX_BATCH_ID);

        assertThatThrownBy(() -> StreamVersion.of(epoch, batchId))
                .isInstanceOf(IllegalArgumentException.class);
    }
}

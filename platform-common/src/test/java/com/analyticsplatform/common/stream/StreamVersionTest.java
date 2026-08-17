package com.analyticsplatform.common.stream;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;
import org.junit.jupiter.params.provider.ValueSource;

/**
 * Enumerated cases for {@link StreamVersion}, with heavy emphasis on the boundaries where the
 * 32-bit packing is most likely to be wrong. Generated coverage lives in
 * {@code StreamVersionProperties}.
 */
class StreamVersionTest {

    @Nested
    @DisplayName("ordering")
    class Ordering {

        /**
         * The case the epoch exists for. Without it, a fresh checkpoint restarting at batch 0
         * would compare lower than an established batch 42 and the replacement would silently
         * not happen.
         */
        @Test
        @DisplayName("a newer epoch beats any batch id from an older epoch")
        void newerEpochDominatesAnyBatchId() {
            long oldEpochHighestBatch = StreamVersion.of(1, StreamVersion.MAX_BATCH_ID);
            long newEpochLowestBatch = StreamVersion.of(2, 0);

            assertThat(newEpochLowestBatch).isGreaterThan(oldEpochHighestBatch);
            assertThat(StreamVersion.supersedes(newEpochLowestBatch, oldEpochHighestBatch)).isTrue();
        }

        @Test
        @DisplayName("within an epoch, a higher batch id wins")
        void higherBatchWinsWithinEpoch() {
            assertThat(StreamVersion.of(5, 11)).isGreaterThan(StreamVersion.of(5, 10));
        }

        /**
         * Replay determinism: the same microbatch must produce the same version, and must not be
         * treated as superseding itself.
         */
        @Test
        @DisplayName("a replayed batch produces an equal version and does not supersede itself")
        void replayIsIdempotent() {
            long first = StreamVersion.of(5, 10);
            long replay = StreamVersion.of(5, 10);

            assertThat(replay).isEqualTo(first);
            assertThat(StreamVersion.supersedes(replay, first)).isFalse();
        }
    }

    @Nested
    @DisplayName("round trip")
    class RoundTrip {

        @ParameterizedTest(name = "epoch={0} batch={1}")
        @CsvSource({
            "1, 0",
            "1, 1",
            "1, 4294967294",   // 2^32 - 2
            "1, 4294967295",   // 2^32 - 1
            "2, 0",
            "2147483647, 0",             // MAX_EPOCH
            "2147483647, 4294967295",    // MAX_EPOCH, MAX_BATCH_ID
        })
        void componentsSurviveRoundTrip(long epoch, long batchId) {
            long version = StreamVersion.of(epoch, batchId);

            assertThat(StreamVersion.epochOf(version)).isEqualTo(epoch);
            assertThat(StreamVersion.batchIdOf(version)).isEqualTo(batchId);
        }

        /** The largest representable version must stay positive in a signed long. */
        @Test
        @DisplayName("maximum version does not overflow into negative")
        void maximumVersionStaysPositive() {
            long max = StreamVersion.of(StreamVersion.MAX_EPOCH, StreamVersion.MAX_BATCH_ID);

            assertThat(max).isPositive();
            assertThat(max).isEqualTo(Long.MAX_VALUE);
        }
    }

    @Nested
    @DisplayName("range rejection")
    class RangeRejection {

        @ParameterizedTest(name = "epoch={0}")
        @ValueSource(longs = {0L, -1L, Long.MIN_VALUE, 2147483648L, Long.MAX_VALUE})
        void rejectsEpochOutsideRange(long epoch) {
            assertThatThrownBy(() -> StreamVersion.of(epoch, 0))
                    .isInstanceOf(IllegalArgumentException.class)
                    .hasMessageContaining("epoch out of range");
        }

        @ParameterizedTest(name = "batchId={0}")
        @ValueSource(longs = {-1L, Long.MIN_VALUE, 4294967296L, Long.MAX_VALUE})
        void rejectsBatchIdOutsideRange(long batchId) {
            assertThatThrownBy(() -> StreamVersion.of(1, batchId))
                    .isInstanceOf(IllegalArgumentException.class)
                    .hasMessageContaining("batchId out of range");
        }

        /**
         * Truncating instead of throwing would corrupt ordering in the worst possible way:
         * quietly, and only under replay.
         */
        @Test
        @DisplayName("an out-of-range epoch throws rather than silently truncating")
        void doesNotSilentlyTruncate() {
            assertThatThrownBy(() -> StreamVersion.of(StreamVersion.MAX_EPOCH + 1, 0))
                    .isInstanceOf(IllegalArgumentException.class);
        }

        @ParameterizedTest(name = "version={0}")
        @ValueSource(longs = {0L, -1L, Long.MIN_VALUE})
        void rejectsNonPositiveVersionOnUnpack(long version) {
            assertThatThrownBy(() -> StreamVersion.epochOf(version))
                    .isInstanceOf(IllegalArgumentException.class);
            assertThatThrownBy(() -> StreamVersion.batchIdOf(version))
                    .isInstanceOf(IllegalArgumentException.class);
        }
    }
}

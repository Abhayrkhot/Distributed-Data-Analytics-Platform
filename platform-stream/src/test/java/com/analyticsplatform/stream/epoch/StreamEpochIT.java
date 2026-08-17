package com.analyticsplatform.stream.epoch;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import com.analyticsplatform.common.config.PlatformConfig;
import com.analyticsplatform.common.dao.ConnectionSource;
import com.analyticsplatform.common.stream.StreamVersion;
import com.analyticsplatform.stream.epoch.StreamEpochStore.Allocation;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.util.ArrayList;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;

/**
 * Epoch allocation (§23).
 *
 * <p>The epoch exists to stop a fresh checkpoint's {@code batch_id = 0} losing the version comparison
 * against an existing {@code batch_id = 42}. If allocation is not transactional and monotonic, that
 * protection silently does not hold — the row looks updated and is not.
 */
class StreamEpochIT {

    private static ConnectionSource connections;

    private StreamEpochStore store;
    private String checkpointPrefix;

    @BeforeAll
    static void connect() {
        connections = ConnectionSource.postgres(PlatformConfig.fromEnvironment());
    }

    @BeforeEach
    void setUp() {
        store = new StreamEpochStore(connections);
        checkpointPrefix = "it-ckpt-" + UUID.randomUUID();
    }

    @AfterEach
    void cleanUp() throws Exception {
        try (Connection connection = connections.open();
             PreparedStatement statement = connection.prepareStatement(
                     "DELETE FROM control.stream_epoch WHERE checkpoint_id LIKE ?")) {
            statement.setString(1, checkpointPrefix + "%");
            statement.executeUpdate();
        }
    }

    @Nested
    @DisplayName("allocation")
    class AllocationSemantics {

        @Test
        @DisplayName("a fresh checkpoint gets exactly one epoch")
        void freshCheckpointAllocatesOnce() {
            Allocation first = store.allocate(checkpointPrefix + "/a", "query-1");

            assertThat(first.fresh()).isTrue();
            assertThat(first.epoch()).isPositive();
            assertThat(store.epochOf(checkpointPrefix + "/a")).contains(first.epoch());
        }

        /** A restart against the same checkpoint continues the same lineage. */
        @Test
        @DisplayName("restarting on the same checkpoint reuses its epoch")
        void restartReusesEpoch() {
            Allocation first = store.allocate(checkpointPrefix + "/a", "query-1");
            Allocation second = store.allocate(checkpointPrefix + "/a", "query-2");

            assertThat(second.epoch()).isEqualTo(first.epoch());
            assertThat(second.fresh()).as("not a fresh allocation").isFalse();
        }

        @Test
        @DisplayName("independent checkpoints get distinct epochs")
        void independentCheckpointsDiffer() {
            long a = store.allocate(checkpointPrefix + "/a", "q").epoch();
            long b = store.allocate(checkpointPrefix + "/b", "q").epoch();

            assertThat(b).isNotEqualTo(a);
        }

        /** Monotonicity is what makes a later epoch outrank an earlier one's highest batch. */
        @Test
        @DisplayName("each new epoch exceeds every previously allocated epoch")
        void epochsAreMonotonic() {
            long before = store.highestEpoch();

            List<Long> allocated = new ArrayList<>();
            for (int i = 0; i < 5; i++) {
                allocated.add(store.allocate(checkpointPrefix + "/c" + i, "q").epoch());
            }

            assertThat(allocated).isSorted();
            assertThat(allocated.get(0)).isGreaterThan(before);
        }

        @Test
        @DisplayName("an unknown checkpoint has no epoch")
        void unknownCheckpointHasNoEpoch() {
            assertThat(store.epochOf(checkpointPrefix + "/never")).isEmpty();
        }

        @Test
        @DisplayName("a blank checkpoint id is refused")
        void blankCheckpointIsRefused() {
            assertThatThrownBy(() -> store.allocate("  ", "q"))
                    .isInstanceOf(IllegalArgumentException.class);
            assertThatThrownBy(() -> store.allocate(null, "q"))
                    .isInstanceOf(IllegalArgumentException.class);
        }
    }

    @Nested
    @DisplayName("concurrency")
    class Concurrency {

        /**
         * Several consumers racing to start against the same checkpoint must agree on one epoch.
         * If they disagreed, two writers would produce versions from different epochs for the same
         * window and the later-arriving-but-lower version would silently lose.
         */
        @Test
        @DisplayName("concurrent allocation on one checkpoint yields a single epoch")
        void concurrentAllocationAgrees() throws Exception {
            int threads = 8;
            CountDownLatch gate = new CountDownLatch(1);
            ExecutorService pool = Executors.newFixedThreadPool(threads);
            try {
                List<Future<Allocation>> futures = new ArrayList<>();
                for (int i = 0; i < threads; i++) {
                    String queryId = "query-" + i;
                    futures.add(pool.submit(() -> {
                        gate.await();
                        return store.allocate(checkpointPrefix + "/race", queryId);
                    }));
                }
                gate.countDown();

                List<Long> epochs = new ArrayList<>();
                long freshCount = 0;
                for (Future<Allocation> future : futures) {
                    Allocation allocation = future.get(30, TimeUnit.SECONDS);
                    epochs.add(allocation.epoch());
                    if (allocation.fresh()) {
                        freshCount++;
                    }
                }

                assertThat(epochs).as("all threads see one epoch").containsOnly(epochs.get(0));
                assertThat(freshCount).as("exactly one thread allocated it").isEqualTo(1);
            } finally {
                pool.shutdownNow();
            }
        }

        /** Distinct checkpoints allocated concurrently must still get distinct epochs. */
        @Test
        @DisplayName("concurrent allocation on distinct checkpoints yields distinct epochs")
        void concurrentDistinctCheckpointsDiffer() throws Exception {
            int threads = 8;
            CountDownLatch gate = new CountDownLatch(1);
            ExecutorService pool = Executors.newFixedThreadPool(threads);
            try {
                List<Future<Long>> futures = new ArrayList<>();
                for (int i = 0; i < threads; i++) {
                    String checkpoint = checkpointPrefix + "/distinct-" + i;
                    futures.add(pool.submit(() -> {
                        gate.await();
                        return store.allocate(checkpoint, "q").epoch();
                    }));
                }
                gate.countDown();

                List<Long> epochs = new ArrayList<>();
                for (Future<Long> future : futures) {
                    epochs.add(future.get(30, TimeUnit.SECONDS));
                }

                assertThat(epochs).doesNotHaveDuplicates();
            } finally {
                pool.shutdownNow();
            }
        }
    }

    @Nested
    @DisplayName("version ordering across a checkpoint reset")
    class VersionOrdering {

        /**
         * The whole reason the epoch exists. A fresh checkpoint restarts batch numbering at 0; that
         * batch must still outrank the old lineage's highest batch, or the replacement silently does
         * not happen.
         */
        @Test
        @DisplayName("a new epoch's batch 0 outranks an old epoch's highest batch")
        void freshCheckpointOutranksOldLineage() {
            Allocation old = store.allocate(checkpointPrefix + "/v1", "q1");
            Allocation fresh = store.allocate(checkpointPrefix + "/v2", "q2");

            long oldHighest = old.versionFor(4_294_967_295L);   // 2^32 - 1
            long freshLowest = fresh.versionFor(0L);

            assertThat(freshLowest)
                    .as("batch 0 of a later epoch must still win")
                    .isGreaterThan(oldHighest);
        }

        @Test
        @DisplayName("within one epoch, a later batch outranks an earlier one")
        void laterBatchWinsWithinEpoch() {
            Allocation allocation = store.allocate(checkpointPrefix + "/v", "q");

            assertThat(allocation.versionFor(42)).isGreaterThan(allocation.versionFor(41));
        }

        /** Replaying a batch must produce the identical version, not merely a comparable one. */
        @Test
        @DisplayName("replaying a batch yields the identical version")
        void replayYieldsIdenticalVersion() {
            Allocation allocation = store.allocate(checkpointPrefix + "/v", "q");

            assertThat(allocation.versionFor(42)).isEqualTo(allocation.versionFor(42));
            assertThat(StreamVersion.epochOf(allocation.versionFor(42)))
                    .isEqualTo(allocation.epoch());
            assertThat(StreamVersion.batchIdOf(allocation.versionFor(42))).isEqualTo(42);
        }
    }
}

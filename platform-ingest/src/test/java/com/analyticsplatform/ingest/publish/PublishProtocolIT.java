package com.analyticsplatform.ingest.publish;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.catchThrowable;

import com.analyticsplatform.common.config.PlatformConfig;
import com.analyticsplatform.common.dao.ConnectionSource;
import com.analyticsplatform.common.dao.JdbcControlPlane;
import com.analyticsplatform.common.run.ControlPlane.RunOutcome;
import com.analyticsplatform.common.run.ControlPlane.RunSpec;
import com.analyticsplatform.common.unit.ProcessingUnitState.Status;
import com.analyticsplatform.ingest.publish.FailPoint.Site;
import com.analyticsplatform.ingest.publish.ProcessingUnitStore.UnitKey;
import com.analyticsplatform.ingest.publish.StagedPublisher.Outcome;
import com.analyticsplatform.ingest.publish.StagedPublisher.Recovery;
import com.analyticsplatform.ingest.publish.StagedPublisher.WriteResult;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

/**
 * The publish-protocol failure matrix — Tier 1 evidence for the idempotent/restart-safe claim.
 *
 * <p>Every case crashes the protocol at a named boundary and asserts what the next run does. The
 * assertions are deliberately about <em>committed state</em>, not about whether an exception was
 * thrown: a protocol that throws politely and leaves a half-published target is exactly the failure
 * this is meant to catch.
 */
class PublishProtocolIT {

    private static ConnectionSource connections;
    private static JdbcControlPlane controlPlane;

    private ProcessingUnitStore store;
    private StagedPublisher publisher;
    private UnitKey key;
    private long runId;
    private String suffix;
    private AtomicInteger writeCount;

    @TempDir
    Path root;

    private Path target;

    @BeforeAll
    static void connect() {
        connections = ConnectionSource.postgres(PlatformConfig.fromEnvironment());
        controlPlane = new JdbcControlPlane(connections);
    }

    @BeforeEach
    void setUp() {
        suffix = UUID.randomUUID().toString();
        key = new UnitKey("it.bronze." + suffix, "raw_to_bronze", "yellow/2024-01");
        store = new ProcessingUnitStore(connections);
        publisher = new StagedPublisher(store, root.resolve("staging"), 900);
        target = root.resolve("warehouse/source=yellow/month=01");
        runId = controlPlane.startRun(RunSpec.of("IT-" + suffix));
        writeCount = new AtomicInteger();
        FailPoint.disarm();
    }

    @AfterEach
    void cleanUp() throws Exception {
        FailPoint.disarm();
        try (Connection connection = connections.open()) {
            for (String sql : List.of(
                    "DELETE FROM control.unit_manifest WHERE dataset_name = ?",
                    "DELETE FROM control.processing_unit WHERE dataset_name = ?")) {
                try (PreparedStatement statement = connection.prepareStatement(sql)) {
                    statement.setString(1, key.datasetName());
                    statement.executeUpdate();
                }
            }
        }
        controlPlane.finishRun(runId, new RunOutcome(RunOutcome.Status.SUCCESS, 0, 0, 0, null, null));
        try (Connection connection = connections.open();
             PreparedStatement statement = connection.prepareStatement(
                     "DELETE FROM control.etl_run WHERE run_id = ?")) {
            statement.setLong(1, runId);
            statement.executeUpdate();
        }
    }

    /** Writes a deterministic payload and counts invocations, so "did it recompute?" is testable. */
    private StagedPublisher.StagedWrite payload(String content, long rows) {
        return staging -> {
            writeCount.incrementAndGet();
            Files.createDirectories(staging);
            Files.writeString(staging.resolve("part-00000.parquet"), content);
            return new WriteResult(rows, "schema-hash-v1");
        };
    }

    private StagedPublisher.StagedWrite payload() {
        return payload("the-data", 100);
    }

    private Outcome publish(String owner) {
        return publisher.publish(key, runId, owner, target, payload());
    }

    private long targetBytes() throws IOException {
        if (!Files.isDirectory(target)) {
            return -1;
        }
        try (var walk = Files.walk(target)) {
            return walk.filter(Files::isRegularFile).mapToLong(p -> {
                try {
                    return Files.size(p);
                } catch (IOException e) {
                    return 0;
                }
            }).sum();
        }
    }

    @Nested
    @DisplayName("happy path")
    class HappyPath {

        @Test
        @DisplayName("publishes, commits, and marks complete")
        void publishesAndCommits() throws IOException {
            Outcome outcome = publish("run-1");

            assertThat(outcome.published()).isTrue();
            assertThat(outcome.rowCount()).isEqualTo(100);
            assertThat(store.findManifest(key)).isPresent();
            assertThat(store.status(key)).contains(Status.COMPLETE);
            assertThat(targetBytes()).isEqualTo(8);
        }

        /** Idempotency: a rerun must skip, not republish. */
        @Test
        @DisplayName("a rerun skips without recomputing")
        void rerunSkips() {
            publish("run-1");
            int afterFirst = writeCount.get();

            Outcome second = publish("run-2");

            assertThat(second.skipped()).isTrue();
            assertThat(writeCount.get()).as("must not recompute").isEqualTo(afterFirst);
        }

        @Test
        @DisplayName("the target fingerprint is unchanged by a rerun")
        void rerunLeavesTargetUntouched() throws IOException {
            publish("run-1");
            long before = targetBytes();

            publish("run-2");

            assertThat(targetBytes()).isEqualTo(before);
        }
    }

    @Nested
    @DisplayName("crash before the commit point: nothing is committed")
    class BeforeCommit {

        /**
         * Every site before the manifest insert must leave the unit uncommitted. The target may or
         * may not have files depending on where it died; what matters is that nothing claims it is
         * done, and that a retry produces correct output.
         */
        @Test
        @DisplayName("crash after staging write leaves no commit and no target")
        void crashAfterStagingWrite() throws IOException {
            FailPoint.arm(Site.AFTER_STAGING_WRITE);
            assertThat(catchThrowable(() -> publish("run-1"))).isNotNull();

            assertThat(store.findManifest(key)).as("not committed").isEmpty();
            assertThat(store.status(key)).contains(Status.FAILED);
            assertThat(targetBytes()).as("target untouched").isEqualTo(-1);

            FailPoint.disarm();
            assertThat(publish("run-2").published()).isTrue();
            assertThat(targetBytes()).isEqualTo(8);
        }

        @Test
        @DisplayName("crash after staging validation leaves no commit and no target")
        void crashAfterStagingValidation() throws IOException {
            FailPoint.arm(Site.AFTER_STAGING_VALIDATION);
            assertThat(catchThrowable(() -> publish("run-1"))).isNotNull();

            assertThat(store.findManifest(key)).isEmpty();
            assertThat(targetBytes()).isEqualTo(-1);

            FailPoint.disarm();
            assertThat(publish("run-2").published()).isTrue();
        }

        @Test
        @DisplayName("crash during promotion leaves no commit; retry rebuilds")
        void crashDuringPromotion() throws IOException {
            FailPoint.arm(Site.DURING_PROMOTION);
            assertThat(catchThrowable(() -> publish("run-1"))).isNotNull();

            assertThat(store.findManifest(key)).isEmpty();

            FailPoint.disarm();
            assertThat(publish("run-2").published()).isTrue();
            assertThat(targetBytes()).isEqualTo(8);
        }

        /**
         * The most dangerous case. Files are fully present in the target, but there is no commit
         * record. An implementation that adopted them would be treating "bytes exist" as "the
         * write finished" — the assumption that turns a crash into silent data loss.
         */
        @Test
        @DisplayName("crash after promotion: a populated target with no manifest is discarded")
        void crashAfterPromotionDiscardsUncommittedTarget() throws IOException {
            FailPoint.arm(Site.AFTER_PROMOTION);
            assertThat(catchThrowable(() -> publish("run-1"))).isNotNull();

            assertThat(targetBytes()).as("files were promoted").isEqualTo(8);
            assertThat(store.findManifest(key)).as("but nothing committed them").isEmpty();

            FailPoint.disarm();
            assertThat(publisher.reconcile(key, target))
                    .isEqualTo(Recovery.UNCOMMITTED_TARGET_DISCARDED);
            assertThat(targetBytes()).as("discarded, not adopted").isEqualTo(-1);

            int before = writeCount.get();
            assertThat(publish("run-2").published()).isTrue();
            assertThat(writeCount.get()).as("rebuilt from source").isGreaterThan(before);
            assertThat(targetBytes()).isEqualTo(8);
        }

        @Test
        @DisplayName("crash after target verification is still uncommitted")
        void crashAfterVerificationIsUncommitted() {
            FailPoint.arm(Site.AFTER_TARGET_VERIFICATION);
            assertThat(catchThrowable(() -> publish("run-1"))).isNotNull();

            assertThat(store.findManifest(key)).isEmpty();
            assertThat(publisher.reconcile(key, target))
                    .isEqualTo(Recovery.UNCOMMITTED_TARGET_DISCARDED);
        }
    }

    @Nested
    @DisplayName("crash after the commit point: committed, only bookkeeping lost")
    class AfterCommit {

        /**
         * The manifest is the commit record, so a crash after it must recover as a status repair —
         * not as a reprocess. Reprocessing here would be wasted work at best and, for a
         * non-deterministic source, divergent output at worst.
         */
        @Test
        @DisplayName("crash after the manifest write repairs status without recomputing")
        void crashAfterManifestRepairsStatus() throws IOException {
            FailPoint.arm(Site.AFTER_MANIFEST_WRITE);
            assertThat(catchThrowable(() -> publish("run-1"))).isNotNull();

            assertThat(store.findManifest(key)).as("committed").isPresent();
            assertThat(store.status(key)).as("bookkeeping lost").isNotEqualTo(java.util.Optional.of(Status.COMPLETE));
            int afterCrash = writeCount.get();

            FailPoint.disarm();
            assertThat(publisher.reconcile(key, target))
                    .isEqualTo(Recovery.COMMITTED_STATUS_REPAIRED);

            assertThat(store.status(key)).contains(Status.COMPLETE);
            assertThat(writeCount.get()).as("must not recompute").isEqualTo(afterCrash);
            assertThat(targetBytes()).isEqualTo(8);
        }

        @Test
        @DisplayName("crash before the status write recovers the same way")
        void crashBeforeCompleteRepairsStatus() {
            FailPoint.arm(Site.BEFORE_COMPLETE);
            assertThat(catchThrowable(() -> publish("run-1"))).isNotNull();

            assertThat(store.findManifest(key)).isPresent();

            FailPoint.disarm();
            int afterCrash = writeCount.get();
            Outcome outcome = publish("run-2");

            assertThat(outcome.skipped()).isTrue();
            assertThat(store.status(key)).contains(Status.COMPLETE);
            assertThat(writeCount.get()).isEqualTo(afterCrash);
        }
    }

    @Nested
    @DisplayName("writer failures")
    class WriterFailures {

        /** A checked exception from the writer must be wrapped, not lost, and must fail the unit. */
        @Test
        @DisplayName("a checked exception from the writer fails the unit")
        void checkedExceptionIsWrapped() {
            Throwable thrown = catchThrowable(() -> publisher.publish(key, runId, "run-1", target,
                    staging -> {
                        throw new java.io.IOException("source file unreadable");
                    }));

            assertThat(thrown).isInstanceOf(ProcessingUnitStore.PublishException.class);
            assertThat(thrown).hasRootCauseMessage("source file unreadable");
            assertThat(store.findManifest(key)).isEmpty();
            assertThat(store.status(key)).contains(Status.FAILED);
        }

        /** Staged output with no files must be refused before anything reaches the target. */
        @Test
        @DisplayName("a writer that produces no files is refused")
        void emptyOutputIsRefused() throws IOException {
            Throwable thrown = catchThrowable(() -> publisher.publish(key, runId, "run-1", target,
                    staging -> {
                        Files.createDirectories(staging);
                        return new WriteResult(0, "schema-hash-v1");
                    }));

            assertThat(thrown).isInstanceOf(ProcessingUnitStore.PublishException.class);
            assertThat(thrown).hasMessageContaining("no data files");
            assertThat(targetBytes()).as("target never touched").isEqualTo(-1);
            assertThat(store.findManifest(key)).isEmpty();
        }

        /** Rebuilding must not steal a unit from a worker that still holds a valid lease. */
        @Test
        @DisplayName("a live lease blocks a forced rebuild")
        void liveLeaseBlocksRebuild() {
            publish("run-1");

            // Corrupt the target so reconcile wants to rebuild, then hand the unit to a live owner.
            assertThat(store.forceRebuild(key, "test")).isTrue();
            assertThat(store.claim(key, "live-owner", 900, "/tmp/live", runId)).isPresent();

            assertThat(store.forceRebuild(key, "should be blocked"))
                    .as("a valid lease must not be overridden").isFalse();
        }
    }

    @Nested
    @DisplayName("manifest and target disagree")
    class Inconsistent {

        @Test
        @DisplayName("a corrupted target is detected and rebuilt")
        void corruptedTargetIsRebuilt() throws IOException {
            publish("run-1");
            assertThat(store.findManifest(key)).isPresent();

            // Simulate corruption: the file shrinks after commit.
            Files.writeString(target.resolve("part-00000.parquet"), "xx");

            assertThat(publisher.reconcile(key, target)).isEqualTo(Recovery.INCONSISTENT_REBUILT);
            assertThat(store.findManifest(key)).as("bad commit record discarded").isEmpty();

            assertThat(publish("run-2").published()).isTrue();
            assertThat(targetBytes()).isEqualTo(8);
            assertThat(store.findManifest(key)).isPresent();
        }

        @Test
        @DisplayName("a manifest whose target vanished is rebuilt")
        void missingTargetIsRebuilt() throws IOException {
            publish("run-1");

            StagedPublisher.deleteRecursively(target);

            assertThat(publisher.reconcile(key, target)).isEqualTo(Recovery.INCONSISTENT_REBUILT);
            assertThat(store.findManifest(key)).isEmpty();

            assertThat(publish("run-2").published()).isTrue();
            assertThat(targetBytes()).isEqualTo(8);
        }
    }

    @Nested
    @DisplayName("retries and ownership")
    class RetriesAndOwnership {

        /** fail, fail, fail, succeed — one manifest, one output, correct attempt count. */
        @Test
        @DisplayName("repeated failure then success yields exactly one commit")
        void repeatedRetryConverges() throws Exception {
            for (int attempt = 0; attempt < 3; attempt++) {
                FailPoint.arm(Site.AFTER_STAGING_WRITE);
                assertThat(catchThrowable(() -> publish("run-fail"))).isNotNull();
                FailPoint.disarm();
            }

            assertThat(publish("run-success").published()).isTrue();

            assertThat(store.findManifest(key)).isPresent();
            assertThat(store.status(key)).contains(Status.COMPLETE);
            assertThat(targetBytes()).isEqualTo(8);

            try (Connection connection = connections.open();
                 PreparedStatement statement = connection.prepareStatement("""
                         SELECT attempt_count FROM control.processing_unit
                          WHERE dataset_name = ? AND pipeline_stage = ? AND processing_unit = ?
                         """)) {
                statement.setString(1, key.datasetName());
                statement.setString(2, key.pipelineStage());
                statement.setString(3, key.processingUnit());
                try (var rows = statement.executeQuery()) {
                    assertThat(rows.next()).isTrue();
                    assertThat(rows.getInt(1)).as("3 failures + 1 success").isEqualTo(4);
                }
            }
        }

        /** A stale attempt's staging directory must never be reused by a later one. */
        @Test
        @DisplayName("a retry does not inherit the previous attempt's staging")
        void stagingIsAttemptScoped() throws IOException {
            FailPoint.arm(Site.AFTER_STAGING_WRITE);
            assertThat(catchThrowable(() ->
                    publisher.publish(key, runId, "owner-a", target, payload("stale", 1))))
                    .isNotNull();
            FailPoint.disarm();

            publisher.publish(key, runId, "owner-b", target, payload("the-data", 100));

            assertThat(Files.readString(target.resolve("part-00000.parquet")))
                    .as("the new attempt's data, not the stale one")
                    .isEqualTo("the-data");
        }

        /**
         * The race the atomic claim exists for. Read-then-write would let several workers each
         * believe they owned the unit.
         */
        @Test
        @DisplayName("concurrent publishers produce exactly one commit")
        void concurrentPublishersSerialize() throws Exception {
            int workers = 6;
            CountDownLatch gate = new CountDownLatch(1);
            ExecutorService pool = Executors.newFixedThreadPool(workers);
            try {
                java.util.List<Future<Outcome>> futures = new java.util.ArrayList<>();
                for (int i = 0; i < workers; i++) {
                    String owner = "worker-" + i;
                    futures.add(pool.submit(() -> {
                        gate.await();
                        try {
                            return publisher.publish(key, runId, owner, target, payload());
                        } catch (RuntimeException e) {
                            return Outcome.skipped("lost the race: " + e.getMessage());
                        }
                    }));
                }
                gate.countDown();

                long published = 0;
                for (Future<Outcome> future : futures) {
                    if (future.get(60, TimeUnit.SECONDS).published()) {
                        published++;
                    }
                }

                assertThat(published).as("exactly one worker publishes").isEqualTo(1);
                assertThat(store.findManifest(key)).isPresent();
                assertThat(targetBytes()).isEqualTo(8);
            } finally {
                pool.shutdownNow();
            }
        }

        /** A committed unit is protected from casual reprocessing. */
        @Test
        @DisplayName("a committed unit cannot be claimed again")
        void committedUnitIsNotClaimable() {
            publish("run-1");

            assertThat(store.claim(key, "run-2", 900, "/tmp/x", runId)).isEmpty();
        }

        /** An owner whose lease expired must not be able to complete the unit. */
        @Test
        @DisplayName("a lease that expires mid-flight cannot be completed")
        void expiredLeaseCannotComplete() {
            StagedPublisher shortLease = new StagedPublisher(store, root.resolve("staging"), 1);
            assertThat(store.claim(key, "owner-a", 1, "/tmp/a", runId)).isPresent();

            // Another worker reclaims after expiry.
            await(1200);
            assertThat(store.claim(key, "owner-b", 900, "/tmp/b", runId)).isPresent();

            assertThat(catchThrowable(() -> store.markComplete(key, "owner-a", 10)))
                    .isInstanceOf(ProcessingUnitStore.PublishException.class)
                    .hasMessageContaining("no longer RUNNING under owner");
            assertThat(shortLease).isNotNull();
        }

        private void await(long millis) {
            try {
                Thread.sleep(millis);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
            }
        }
    }
}

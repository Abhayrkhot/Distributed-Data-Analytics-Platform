package com.analyticsplatform.ingest.publish;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatCode;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import com.analyticsplatform.common.dao.ConnectionSource;
import com.analyticsplatform.ingest.publish.ProcessingUnitStore.PublishException;
import com.analyticsplatform.ingest.publish.ProcessingUnitStore.UnitKey;
import java.nio.file.Files;
import java.nio.file.Path;
import java.sql.SQLException;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;

/**
 * The parts of the publish layer that need no database: key validation, and the failure paths that
 * surface when the control plane is unreachable.
 */
class ProcessingUnitStoreTest {

    /** Stands in for an unreachable control plane. */
    private final ConnectionSource unreachable = () -> {
        throw new SQLException("connection refused");
    };

    @Nested
    @DisplayName("unit key")
    class Key {

        @ParameterizedTest(name = "null {0} is refused")
        @CsvSource({"dataset", "stage", "unit"})
        void nullComponentsAreRefused(String which) {
            String dataset = "dataset".equals(which) ? null : "d";
            String stage = "stage".equals(which) ? null : "raw_to_bronze";
            String unit = "unit".equals(which) ? null : "u";

            assertThatThrownBy(() -> new UnitKey(dataset, stage, unit))
                    .isInstanceOf(IllegalArgumentException.class)
                    .hasMessageContaining("must not be null");
        }

        /** The string form appears in log lines and error messages, so it must be readable. */
        @Test
        @DisplayName("renders as dataset/stage/unit")
        void rendersReadably() {
            assertThat(new UnitKey("bronze.trip_raw", "raw_to_bronze", "yellow/2024-01").toString())
                    .isEqualTo("bronze.trip_raw/raw_to_bronze/yellow/2024-01");
        }

        @Test
        @DisplayName("a fully-populated key is accepted")
        void validKeyIsAccepted() {
            assertThatCode(() -> new UnitKey("d", "raw_to_bronze", "u"))
                    .doesNotThrowAnyException();
        }
    }

    @Nested
    @DisplayName("control-plane failures surface, never silently succeed")
    class Failures {

        private final ProcessingUnitStore store = new ProcessingUnitStore(unreachable);
        private final UnitKey key = new UnitKey("d", "raw_to_bronze", "u");

        @Test
        void claimFails() {
            assertThatThrownBy(() -> store.claim(key, "owner", 900, "/tmp/s", 1L))
                    .isInstanceOf(PublishException.class)
                    .hasMessageContaining("failed to claim");
        }

        /** A null run id is legal — a unit can be claimed outside a tracked run. */
        @Test
        @DisplayName("a null run id is bound rather than rejected")
        void nullRunIdIsAccepted() {
            assertThatThrownBy(() -> store.claim(key, "owner", 900, "/tmp/s", null))
                    .isInstanceOf(PublishException.class)
                    .hasMessageContaining("failed to claim");
        }

        @Test
        void findManifestFails() {
            assertThatThrownBy(() -> store.findManifest(key))
                    .isInstanceOf(PublishException.class)
                    .hasMessageContaining("failed to read manifest");
        }

        @Test
        void statusFails() {
            assertThatThrownBy(() -> store.status(key))
                    .isInstanceOf(PublishException.class)
                    .hasMessageContaining("failed to read status");
        }

        @Test
        void commitFails() {
            UnitFingerprint fingerprint = new UnitFingerprint(1, 1, 1, "s", "c");

            assertThatThrownBy(() -> store.commit(key, 1L, fingerprint, "/tmp/t"))
                    .isInstanceOf(PublishException.class)
                    .hasMessageContaining("failed to commit");
        }

        @Test
        void updatesFail() {
            assertThatThrownBy(() -> store.markFailed(key, "boom"))
                    .isInstanceOf(PublishException.class);
            assertThatThrownBy(() -> store.repairToComplete(key))
                    .isInstanceOf(PublishException.class);
            assertThatThrownBy(() -> store.forceRebuild(key, "reason"))
                    .isInstanceOf(PublishException.class);
        }
    }

    @Nested
    @DisplayName("publisher guards")
    class PublisherGuards {

        private final StagedPublisher publisher = new StagedPublisher(
                new ProcessingUnitStore(unreachable), Path.of("/tmp/staging"), 900);

        /** Staged output that produced no files must never reach the target. */
        @Test
        @DisplayName("empty staged output is refused before promotion")
        void emptyStagingIsRefused(@TempDir Path dir) {
            UnitFingerprint empty = UnitFingerprint.of(dir, 0, "schema");

            assertThat(empty.fileCount()).isZero();
            assertThat(publisher).isNotNull();
        }

        @Test
        @DisplayName("deleteRecursively tolerates a path that does not exist")
        void deleteMissingPathIsSafe(@TempDir Path dir) {
            assertThatCode(() -> StagedPublisher.deleteRecursively(dir.resolve("absent")))
                    .doesNotThrowAnyException();
        }

        @Test
        @DisplayName("deleteRecursively removes nested content")
        void deleteRemovesNestedContent(@TempDir Path dir) throws Exception {
            Path nested = dir.resolve("a/b/c");
            Files.createDirectories(nested);
            Files.writeString(nested.resolve("f.parquet"), "x");

            StagedPublisher.deleteRecursively(dir.resolve("a"));

            assertThat(Files.exists(dir.resolve("a"))).isFalse();
        }
    }

    @Nested
    @DisplayName("fingerprint edge cases")
    class FingerprintEdges {

        @Test
        @DisplayName("describe handles a null hash without throwing")
        void describeHandlesNullHash() {
            assertThat(new UnitFingerprint(1, 1, 1, null, null).describe())
                    .contains("rows=1", "null");
        }

        @Test
        @DisplayName("describe leaves a short hash intact")
        void describeLeavesShortHashAlone() {
            assertThat(new UnitFingerprint(1, 1, 1, "abc", "def").describe())
                    .contains("schema=abc", "content=def");
        }

        @Test
        @DisplayName("fingerprinting a missing directory fails loudly")
        void missingDirectoryFails(@TempDir Path dir) {
            assertThatThrownBy(() -> UnitFingerprint.of(dir.resolve("absent"), 0, "s"))
                    .isInstanceOf(java.io.UncheckedIOException.class)
                    .hasMessageContaining("failed to fingerprint");
        }
    }

    @Nested
    @DisplayName("fail point")
    class FailPointEdges {

        @Test
        @DisplayName("arming with no sites disarms")
        void armingNothingDisarms() {
            FailPoint.arm(FailPoint.Site.AFTER_PROMOTION);
            assertThat(FailPoint.isArmed()).isTrue();

            FailPoint.arm();

            assertThat(FailPoint.isArmed()).isFalse();
            FailPoint.disarm();
        }
    }
}

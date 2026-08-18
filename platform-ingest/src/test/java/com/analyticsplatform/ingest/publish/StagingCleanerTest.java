package com.analyticsplatform.ingest.publish;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatCode;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import com.analyticsplatform.ingest.publish.StagingCleaner.UnsafeCleanupException;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.attribute.FileTime;
import java.time.Duration;
import java.time.Instant;
import java.util.List;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

/**
 * Filesystem safety (§37).
 *
 * <p>This class deletes directories recursively based on configuration, so the tests are written
 * adversarially: the question is not "does cleanup work" but "can any configuration or filesystem
 * layout make it delete the warehouse". Most of these assert a refusal.
 */
class StagingCleanerTest {

    private static final Duration RETENTION = Duration.ofDays(3);

    private static Path attempt(Path stagingRoot, String stage, String dataset,
                                String unit, String owner) throws IOException {
        Path path = stagingRoot.resolve(stage).resolve(dataset).resolve(unit).resolve(owner);
        Files.createDirectories(path);
        Files.writeString(path.resolve("part-00000.parquet"), "data");
        return path;
    }

    private static void age(Path path, Duration age) throws IOException {
        Files.setLastModifiedTime(path, FileTime.from(Instant.now().minus(age)));
    }

    @Nested
    @DisplayName("refuses dangerous configuration")
    class ConfigurationSafety {

        @Test
        @DisplayName("staging root equal to the data root is refused")
        void sameRootRefused(@TempDir Path root) {
            Path shared = root.resolve("warehouse");

            assertThatThrownBy(() -> new StagingCleaner(shared, shared, RETENTION))
                    .isInstanceOf(UnsafeCleanupException.class)
                    .hasMessageContaining("same directory");
        }

        /** Staging inside the warehouse means cleanup walks warehouse data. */
        @Test
        @DisplayName("staging inside the data root is refused")
        void stagingInsideDataRefused(@TempDir Path root) {
            Path data = root.resolve("warehouse");
            Path staging = data.resolve("staging");

            assertThatThrownBy(() -> new StagingCleaner(staging, data, RETENTION))
                    .isInstanceOf(UnsafeCleanupException.class)
                    .hasMessageContaining("could delete published output");
        }

        /** The warehouse inside staging means cleanup can reach all of it. */
        @Test
        @DisplayName("the data root inside staging is refused")
        void dataInsideStagingRefused(@TempDir Path root) {
            Path staging = root.resolve("staging");
            Path data = staging.resolve("warehouse");

            assertThatThrownBy(() -> new StagingCleaner(staging, data, RETENTION))
                    .isInstanceOf(UnsafeCleanupException.class)
                    .hasMessageContaining("delete the entire warehouse");
        }

        /**
         * The check is on the normalized path, so a configured value cannot disguise where it
         * actually points by routing through {@code ../}.
         */
        @Test
        @DisplayName("a ../ path that resolves inside the data root is still refused")
        void traversalCannotDisguiseNesting(@TempDir Path root) {
            Path data = root.resolve("warehouse");
            Path disguised = root.resolve("warehouse/elsewhere/../staging");

            assertThatThrownBy(() -> new StagingCleaner(disguised, data, RETENTION))
                    .isInstanceOf(UnsafeCleanupException.class);
        }

        @Test
        @DisplayName("a filesystem root as the staging root is refused")
        void filesystemRootRefused(@TempDir Path root) {
            assertThatThrownBy(() -> new StagingCleaner(
                    Path.of("/"), root.resolve("warehouse"), RETENTION))
                    .isInstanceOf(UnsafeCleanupException.class);
        }

        @Test
        @DisplayName("sibling directories are accepted")
        void siblingsAccepted(@TempDir Path root) {
            assertThatCode(() -> new StagingCleaner(
                    root.resolve("staging"), root.resolve("warehouse"), RETENTION))
                    .doesNotThrowAnyException();
        }

        @Test
        @DisplayName("a negative retention is refused")
        void negativeRetentionRefused(@TempDir Path root) {
            assertThatThrownBy(() -> new StagingCleaner(
                    root.resolve("staging"), root.resolve("warehouse"), Duration.ofDays(-1)))
                    .isInstanceOf(IllegalArgumentException.class);
        }
    }

    @Nested
    @DisplayName("retention policy")
    class Retention {

        @Test
        @DisplayName("an aged attempt directory is removed")
        void agedAttemptRemoved(@TempDir Path root) throws IOException {
            Path staging = root.resolve("staging");
            Path old = attempt(staging, "raw_to_bronze", "bronze", "yellow-2024", "run-1");
            age(old, Duration.ofDays(10));

            StagingCleaner.Outcome outcome =
                    new StagingCleaner(staging, root.resolve("warehouse"), RETENTION)
                            .clean(List.of(), false);

            assertThat(outcome.removed()).hasSize(1);
            assertThat(Files.exists(old)).isFalse();
        }

        /**
         * Failed staging is the only evidence of what a failed run produced. Deleting it
         * immediately means debugging the next failure from nothing.
         */
        @Test
        @DisplayName("a recent attempt is retained for debugging")
        void recentAttemptRetained(@TempDir Path root) throws IOException {
            Path staging = root.resolve("staging");
            Path recent = attempt(staging, "raw_to_bronze", "bronze", "yellow-2024", "run-1");
            age(recent, Duration.ofHours(1));

            StagingCleaner.Outcome outcome =
                    new StagingCleaner(staging, root.resolve("warehouse"), RETENTION)
                            .clean(List.of(), false);

            assertThat(outcome.removed()).isEmpty();
            assertThat(Files.exists(recent)).isTrue();
        }

        /** A live job's working directory disappearing is worse than disk pressure. */
        @Test
        @DisplayName("an active attempt is never removed, however old")
        void activeAttemptNeverRemoved(@TempDir Path root) throws IOException {
            Path staging = root.resolve("staging");
            Path active = attempt(staging, "raw_to_bronze", "bronze", "yellow-2024", "run-live");
            age(active, Duration.ofDays(100));

            StagingCleaner.Outcome outcome =
                    new StagingCleaner(staging, root.resolve("warehouse"), RETENTION)
                            .clean(List.of(active), false);

            assertThat(outcome.removed()).isEmpty();
            assertThat(Files.exists(active)).isTrue();
        }

        /** One run's cleanup must not take another run's in-flight attempt with it. */
        @Test
        @DisplayName("cleaning one attempt leaves a sibling run's attempt intact")
        void siblingAttemptSurvives(@TempDir Path root) throws IOException {
            Path staging = root.resolve("staging");
            Path stale = attempt(staging, "raw_to_bronze", "bronze", "yellow-2024", "run-old");
            Path live = attempt(staging, "raw_to_bronze", "bronze", "yellow-2024", "run-live");
            age(stale, Duration.ofDays(10));
            age(live, Duration.ofDays(10));

            new StagingCleaner(staging, root.resolve("warehouse"), RETENTION)
                    .clean(List.of(live), false);

            assertThat(Files.exists(stale)).isFalse();
            assertThat(Files.exists(live)).as("still in use").isTrue();
        }

        @Test
        @DisplayName("cleanup never removes a directory above the attempt level")
        void neverDeletesAboveAttemptLevel(@TempDir Path root) throws IOException {
            Path staging = root.resolve("staging");
            Path old = attempt(staging, "raw_to_bronze", "bronze", "yellow-2024", "run-1");
            age(old, Duration.ofDays(10));

            new StagingCleaner(staging, root.resolve("warehouse"), RETENTION)
                    .clean(List.of(), false);

            assertThat(Files.isDirectory(staging.resolve("raw_to_bronze/bronze/yellow-2024")))
                    .as("the unit directory survives").isTrue();
            assertThat(Files.isDirectory(staging)).as("the staging root survives").isTrue();
        }
    }

    @Nested
    @DisplayName("the warehouse is never touched")
    class WarehouseSafety {

        @Test
        @DisplayName("published output survives a cleanup pass")
        void publishedOutputSurvives(@TempDir Path root) throws IOException {
            Path staging = root.resolve("staging");
            Path warehouse = root.resolve("warehouse");
            Path published = warehouse.resolve("source=yellow/month=01");
            Files.createDirectories(published);
            Files.writeString(published.resolve("part-00000.parquet"), "committed");

            Path old = attempt(staging, "raw_to_bronze", "bronze", "yellow-2024", "run-1");
            age(old, Duration.ofDays(10));

            new StagingCleaner(staging, warehouse, RETENTION).clean(List.of(), false);

            assertThat(Files.exists(published.resolve("part-00000.parquet")))
                    .as("committed data untouched").isTrue();
            assertThat(Files.exists(old)).isFalse();
        }

        /**
         * The actual defence against a symlink escape, asserted directly.
         *
         * <p>{@link Files#walk} does not follow symbolic links unless FOLLOW_LINKS is passed, so
         * the link is deleted and its target survives. That — not the containment re-check — is
         * what protects the warehouse today, and stating it here means a future change adding
         * FOLLOW_LINKS turns this test red.
         */
        @Test
        @DisplayName("a symlink escaping to the warehouse is refused")
        void symlinkEscapeRefused(@TempDir Path root) throws IOException {
            Path staging = root.resolve("staging");
            Path warehouse = root.resolve("warehouse");
            Files.createDirectories(warehouse.resolve("precious"));
            Files.writeString(warehouse.resolve("precious/data.parquet"), "committed");

            Path unitDir = staging.resolve("raw_to_bronze/bronze/yellow-2024");
            Files.createDirectories(unitDir);
            Path escape = unitDir.resolve("run-evil");
            try {
                Files.createSymbolicLink(escape, warehouse.resolve("precious"));
            } catch (UnsupportedOperationException | IOException e) {
                return;   // symlinks unavailable on this filesystem; nothing to assert
            }
            age(unitDir, Duration.ofDays(10));

            StagingCleaner cleaner = new StagingCleaner(staging, warehouse, RETENTION);
            try {
                cleaner.clean(List.of(), false);
            } catch (UnsafeCleanupException expected) {
                // Refusing is the correct outcome.
            }

            assertThat(Files.exists(warehouse.resolve("precious/data.parquet")))
                    .as("the warehouse must survive regardless").isTrue();
        }
    }

    @Nested
    @DisplayName("dry run")
    class DryRun {

        @Test
        @DisplayName("dry run reports what it would remove and deletes nothing")
        void dryRunDeletesNothing(@TempDir Path root) throws IOException {
            Path staging = root.resolve("staging");
            Path old = attempt(staging, "raw_to_bronze", "bronze", "yellow-2024", "run-1");
            age(old, Duration.ofDays(10));

            StagingCleaner.Outcome outcome =
                    new StagingCleaner(staging, root.resolve("warehouse"), RETENTION)
                            .clean(List.of(), true);

            assertThat(outcome.dryRun()).isTrue();
            assertThat(outcome.removed()).hasSize(1);
            assertThat(Files.exists(old)).as("nothing actually deleted").isTrue();
            assertThat(outcome.describe()).contains("[dry-run]");
        }

        @Test
        @DisplayName("dry run and real run agree on what is stale")
        void dryRunMatchesRealRun(@TempDir Path root) throws IOException {
            Path staging = root.resolve("staging");
            Path old = attempt(staging, "raw_to_bronze", "bronze", "yellow-2024", "run-old");
            Path recent = attempt(staging, "raw_to_bronze", "bronze", "yellow-2024", "run-new");
            age(old, Duration.ofDays(10));
            age(recent, Duration.ofHours(1));

            StagingCleaner cleaner =
                    new StagingCleaner(staging, root.resolve("warehouse"), RETENTION);

            List<Path> predicted = cleaner.clean(List.of(), true).removed();
            List<Path> actual = cleaner.clean(List.of(), false).removed();

            assertThat(actual).containsExactlyElementsOf(predicted);
        }
    }

    @Test
    @DisplayName("a missing staging root is not an error")
    void missingStagingRootIsSafe(@TempDir Path root) {
        StagingCleaner.Outcome outcome =
                new StagingCleaner(root.resolve("absent"), root.resolve("warehouse"), RETENTION)
                        .clean(List.of(), false);

        assertThat(outcome.removed()).isEmpty();
        assertThat(outcome.retained()).isEmpty();
    }
}

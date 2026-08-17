package com.analyticsplatform.ingest.publish;

import static org.assertj.core.api.Assertions.assertThat;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

/**
 * Fingerprinting is what lets recovery tell a finished write from a half-finished one, so the
 * cases that matter are the ones where two directories look similar but are not the same.
 */
class UnitFingerprintTest {

    private static final String SCHEMA = "abc123";

    private static void write(Path directory, String name, String content) throws IOException {
        Files.createDirectories(directory);
        Files.writeString(directory.resolve(name), content);
    }

    @Nested
    @DisplayName("what it counts")
    class Counting {

        @Test
        @DisplayName("counts data files and their total size")
        void countsDataFiles(@TempDir Path dir) throws IOException {
            write(dir, "part-00000.parquet", "abcde");
            write(dir, "part-00001.parquet", "fgh");

            UnitFingerprint fingerprint = UnitFingerprint.of(dir, 42, SCHEMA);

            assertThat(fingerprint.fileCount()).isEqualTo(2);
            assertThat(fingerprint.totalBytes()).isEqualTo(8);
            assertThat(fingerprint.rowCount()).isEqualTo(42);
            assertThat(fingerprint.schemaHash()).isEqualTo(SCHEMA);
        }

        /**
         * Parquet writers leave _SUCCESS markers and .crc sidecars. They vary between runs and
         * between local and cluster writes, so counting them would make an identical dataset
         * fingerprint differently depending on where it was produced.
         */
        @Test
        @DisplayName("ignores writer metadata and checksum sidecars")
        void ignoresNonDataFiles(@TempDir Path dir) throws IOException {
            write(dir, "part-00000.parquet", "abcde");
            write(dir, "_SUCCESS", "");
            write(dir, ".part-00000.parquet.crc", "xx");
            write(dir, ".hidden", "yy");

            UnitFingerprint fingerprint = UnitFingerprint.of(dir, 1, SCHEMA);

            assertThat(fingerprint.fileCount()).isEqualTo(1);
            assertThat(fingerprint.totalBytes()).isEqualTo(5);
        }

        @Test
        @DisplayName("descends into partition subdirectories")
        void walksNestedPartitions(@TempDir Path dir) throws IOException {
            write(dir.resolve("year=2024/month=01"), "part-0.parquet", "aaa");
            write(dir.resolve("year=2024/month=02"), "part-0.parquet", "bb");

            UnitFingerprint fingerprint = UnitFingerprint.of(dir, 5, SCHEMA);

            assertThat(fingerprint.fileCount()).isEqualTo(2);
            assertThat(fingerprint.totalBytes()).isEqualTo(5);
        }

        @Test
        @DisplayName("an empty directory fingerprints as empty rather than failing")
        void emptyDirectory(@TempDir Path dir) {
            UnitFingerprint fingerprint = UnitFingerprint.of(dir, 0, SCHEMA);

            assertThat(fingerprint.fileCount()).isZero();
            assertThat(fingerprint.totalBytes()).isZero();
        }
    }

    @Nested
    @DisplayName("matching")
    class Matching {

        /**
         * The core requirement: a fingerprint taken in staging must still match after the files
         * are promoted to a different absolute path. Comparing absolute paths would make every
         * promotion look like a mismatch.
         */
        @Test
        @DisplayName("survives relocation to a different directory")
        void surviveRelocation(@TempDir Path root) throws IOException {
            Path staging = root.resolve("staging");
            Path target = root.resolve("warehouse/source=yellow/month=01");
            write(staging, "part-00000.parquet", "abcde");
            write(staging, "part-00001.parquet", "fgh");

            UnitFingerprint staged = UnitFingerprint.of(staging, 42, SCHEMA);

            Files.createDirectories(target.getParent());
            Files.move(staging, target);

            assertThat(staged.matches(UnitFingerprint.of(target, 42, SCHEMA))).isTrue();
        }

        @Test
        @DisplayName("a truncated file does not match")
        void truncatedFileIsDetected(@TempDir Path root) throws IOException {
            Path complete = root.resolve("a");
            Path truncated = root.resolve("b");
            write(complete, "part-00000.parquet", "abcde");
            write(truncated, "part-00000.parquet", "abc");

            assertThat(UnitFingerprint.of(complete, 42, SCHEMA)
                    .matches(UnitFingerprint.of(truncated, 42, SCHEMA))).isFalse();
        }

        @Test
        @DisplayName("a missing part does not match")
        void missingPartIsDetected(@TempDir Path root) throws IOException {
            Path complete = root.resolve("a");
            Path partial = root.resolve("b");
            write(complete, "part-00000.parquet", "abc");
            write(complete, "part-00001.parquet", "de");
            write(partial, "part-00000.parquet", "abc");

            assertThat(UnitFingerprint.of(complete, 42, SCHEMA)
                    .matches(UnitFingerprint.of(partial, 42, SCHEMA))).isFalse();
        }

        /** A leftover part from an earlier attempt is as wrong as a missing one. */
        @Test
        @DisplayName("an extra file from a previous attempt does not match")
        void extraFileIsDetected(@TempDir Path root) throws IOException {
            Path expected = root.resolve("a");
            Path polluted = root.resolve("b");
            write(expected, "part-00000.parquet", "abc");
            write(polluted, "part-00000.parquet", "abc");
            write(polluted, "part-00001.parquet", "stale");

            assertThat(UnitFingerprint.of(expected, 42, SCHEMA)
                    .matches(UnitFingerprint.of(polluted, 42, SCHEMA))).isFalse();
        }

        /**
         * Same bytes, different row count. Treating these as equal would let a partial write with
         * coincidentally similar sizes pass verification.
         */
        @Test
        @DisplayName("a differing row count does not match even when the bytes agree")
        void rowCountIsPartOfIdentity(@TempDir Path dir) throws IOException {
            write(dir, "part-00000.parquet", "abcde");

            assertThat(UnitFingerprint.of(dir, 42, SCHEMA)
                    .matches(UnitFingerprint.of(dir, 43, SCHEMA))).isFalse();
        }

        @Test
        @DisplayName("a differing schema hash does not match")
        void schemaIsPartOfIdentity(@TempDir Path dir) throws IOException {
            write(dir, "part-00000.parquet", "abcde");

            assertThat(UnitFingerprint.of(dir, 42, SCHEMA)
                    .matches(UnitFingerprint.of(dir, 42, "different"))).isFalse();
        }

        @Test
        @DisplayName("file ordering on disk does not affect the fingerprint")
        void orderingIsIrrelevant(@TempDir Path root) throws IOException {
            Path first = root.resolve("a");
            Path second = root.resolve("b");
            write(first, "part-00000.parquet", "abc");
            write(first, "part-00001.parquet", "de");
            // Same content, written in the opposite order.
            write(second, "part-00001.parquet", "de");
            write(second, "part-00000.parquet", "abc");

            assertThat(UnitFingerprint.of(first, 42, SCHEMA)
                    .matches(UnitFingerprint.of(second, 42, SCHEMA))).isTrue();
        }

        @Test
        @DisplayName("null never matches")
        void nullDoesNotMatch(@TempDir Path dir) throws IOException {
            write(dir, "part-00000.parquet", "abc");

            assertThat(UnitFingerprint.of(dir, 1, SCHEMA).matches(null)).isFalse();
        }
    }

    @Test
    @DisplayName("describe abbreviates hashes for log lines")
    void describeIsReadable(@TempDir Path dir) throws IOException {
        Files.createDirectories(dir);
        Files.writeString(dir.resolve("part-0.parquet"), "abc");

        assertThat(UnitFingerprint.of(dir, 7, "0123456789abcdef0123").describe())
                .contains("rows=7", "files=1", "bytes=3", "schema=0123456789ab")
                .doesNotContain("0123456789abcdef0123");
    }
}

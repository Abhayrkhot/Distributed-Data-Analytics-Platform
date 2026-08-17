package com.analyticsplatform.ingest.publish;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.stream.Stream;

/**
 * A verifiable description of what a published unit contains.
 *
 * <p>This is what makes the manifest a usable commit record. After a crash the next run has to
 * decide whether the files sitting in the target are the ones the manifest describes, and "a
 * directory exists" cannot answer that — a half-written target and a complete one look identical
 * from the outside.
 *
 * <p>The content hash covers relative path and byte size for every data file, sorted. That detects
 * a truncated file, a missing part, or an extra part left by an earlier attempt, without reading
 * file contents — which matters because the alternative is re-hashing gigabytes on every recovery
 * check.
 *
 * <p>It deliberately does <em>not</em> detect a file whose bytes changed while its size stayed the
 * same. Guarding against that would mean reading every byte on every check, and the failure it
 * protects against — silent in-place corruption of an immutable Parquet part — is not one this
 * pipeline can produce. Stated here so nobody later assumes a stronger guarantee than exists.
 */
public record UnitFingerprint(
        long rowCount,
        int fileCount,
        long totalBytes,
        String schemaHash,
        String contentHash) {

    /** Parquet writers leave these behind; they are not data and vary between runs. */
    private static boolean isDataFile(Path path) {
        String name = path.getFileName().toString();
        return !name.startsWith(".")
                && !name.startsWith("_")
                && !name.endsWith(".crc");
    }

    /**
     * Describes the data files under {@code directory}.
     *
     * @param rowCount   authoritative row count, taken from the writer rather than re-read
     * @param schemaHash canonical schema hash of what was written
     */
    public static UnitFingerprint of(Path directory, long rowCount, String schemaHash) {
        List<String> entries = new ArrayList<>();
        long totalBytes = 0;
        int fileCount = 0;

        try (Stream<Path> walk = Files.walk(directory)) {
            List<Path> files = walk.filter(Files::isRegularFile)
                    .filter(UnitFingerprint::isDataFile)
                    .toList();
            for (Path file : files) {
                long size = Files.size(file);
                totalBytes += size;
                fileCount++;
                // Relative so a fingerprint taken in staging still matches after promotion to a
                // different absolute path. Comparing absolute paths would make every promotion
                // look like a mismatch.
                entries.add(directory.relativize(file) + ":" + size);
            }
        } catch (IOException e) {
            throw new UncheckedIOException("failed to fingerprint " + directory, e);
        }

        Collections.sort(entries);
        return new UnitFingerprint(rowCount, fileCount, totalBytes, schemaHash,
                sha256Hex(String.join("\n", entries)));
    }

    /**
     * Whether two fingerprints describe the same published output.
     *
     * <p>Compares every component. A row count that matches while the byte count does not means a
     * partial write, and treating that as equal is exactly the mistake this exists to prevent.
     */
    public boolean matches(UnitFingerprint other) {
        return other != null
                && rowCount == other.rowCount
                && fileCount == other.fileCount
                && totalBytes == other.totalBytes
                && schemaHash.equals(other.schemaHash)
                && contentHash.equals(other.contentHash);
    }

    /** A one-line description for reconciliation log messages. */
    public String describe() {
        return "rows=" + rowCount + " files=" + fileCount + " bytes=" + totalBytes
                + " schema=" + abbreviate(schemaHash) + " content=" + abbreviate(contentHash);
    }

    private static String abbreviate(String hash) {
        return hash == null || hash.length() <= 12 ? String.valueOf(hash) : hash.substring(0, 12);
    }

    private static String sha256Hex(String input) {
        try {
            byte[] digest = MessageDigest.getInstance("SHA-256")
                    .digest(input.getBytes(StandardCharsets.UTF_8));
            StringBuilder hex = new StringBuilder(digest.length * 2);
            for (byte b : digest) {
                hex.append(Character.forDigit((b >> 4) & 0xF, 16));
                hex.append(Character.forDigit(b & 0xF, 16));
            }
            return hex.toString();
        } catch (NoSuchAlgorithmException e) {
            throw new IllegalStateException("SHA-256 unavailable", e);
        }
    }
}

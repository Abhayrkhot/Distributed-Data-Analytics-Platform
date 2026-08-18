package com.analyticsplatform.ingest.publish;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
import java.util.stream.Stream;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Removes stale staging directories.
 *
 * <h2>Why this is written defensively</h2>
 *
 * <p>This class deletes directories recursively, driven by configuration. The failure mode is not
 * "cleanup does not run" — it is "cleanup runs somewhere it should not", and the consequence is
 * deleted warehouse data. So every path is normalized and checked for containment before anything
 * is removed, and the checks are tested with {@code ../} traversal, symlinks and adversarial
 * configuration rather than assumed.
 *
 * <h2>Retention policy</h2>
 *
 * <ul>
 *   <li><strong>Successful staging</strong> — removed once the unit is committed; the data now
 *       lives in the target and the staging copy is pure waste.
 *   <li><strong>Failed staging</strong> — retained for a grace period. It is the only evidence of
 *       what a failed run actually produced, and deleting it immediately means debugging the next
 *       failure from nothing.
 *   <li><strong>Active staging</strong> — never touched, whatever its age. A long-running job's
 *       working directory disappearing underneath it is a worse outcome than disk pressure.
 * </ul>
 */
public final class StagingCleaner {

    private static final Logger log = LoggerFactory.getLogger(StagingCleaner.class);

    /** What a cleanup pass did, or would do. */
    public record Outcome(List<Path> removed, List<Path> retained, boolean dryRun) {

        public int removedCount() {
            return removed.size();
        }

        public String describe() {
            return (dryRun ? "[dry-run] " : "")
                    + removed.size() + " removed, " + retained.size() + " retained";
        }
    }

    /** Refuses a configuration that could delete the wrong thing. */
    public static final class UnsafeCleanupException extends RuntimeException {
        UnsafeCleanupException(String message) {
            super(message);
        }
    }

    private final Path stagingRoot;
    private final Path dataRoot;
    private final Duration failedRetention;

    /**
     * @param stagingRoot      the only directory this cleaner may delete inside
     * @param dataRoot         the warehouse; used to prove staging is not inside it
     * @param failedRetention  how long a failed attempt's staging survives for debugging
     */
    public StagingCleaner(Path stagingRoot, Path dataRoot, Duration failedRetention) {
        Path staging = normalize(stagingRoot);
        Path data = normalize(dataRoot);

        if (staging.equals(data)) {
            throw new UnsafeCleanupException(
                    "staging root and data root are the same directory (" + staging
                            + "); cleanup would delete the warehouse");
        }
        // Both directions are fatal. Staging inside the warehouse means cleanup walks warehouse
        // data; the warehouse inside staging means cleanup can reach all of it.
        if (staging.startsWith(data)) {
            throw new UnsafeCleanupException(
                    "staging root " + staging + " is inside the data root " + data
                            + "; cleanup could delete published output");
        }
        if (data.startsWith(staging)) {
            throw new UnsafeCleanupException(
                    "data root " + data + " is inside the staging root " + staging
                            + "; cleanup could delete the entire warehouse");
        }
        if (staging.getNameCount() == 0 || staging.equals(staging.getRoot())) {
            throw new UnsafeCleanupException(
                    "refusing to use a filesystem root as the staging root");
        }
        if (failedRetention == null || failedRetention.isNegative()) {
            throw new IllegalArgumentException("failedRetention must not be negative");
        }

        this.stagingRoot = staging;
        this.dataRoot = data;
        this.failedRetention = failedRetention;
    }

    /**
     * Removes staging directories older than the retention window.
     *
     * @param activeStagingPaths directories currently in use; never removed regardless of age
     * @param dryRun             when true, reports what would be removed and deletes nothing
     */
    public Outcome clean(List<Path> activeStagingPaths, boolean dryRun) {
        List<Path> removed = new ArrayList<>();
        List<Path> retained = new ArrayList<>();

        if (!Files.isDirectory(stagingRoot)) {
            return new Outcome(removed, retained, dryRun);
        }

        List<Path> active = activeStagingPaths.stream().map(StagingCleaner::normalize).toList();
        Instant cutoff = Instant.now().minus(failedRetention);

        for (Path candidate : attemptDirectories()) {
            if (active.contains(candidate)) {
                // A live job's working directory. Age is irrelevant.
                retained.add(candidate);
                continue;
            }
            if (lastModified(candidate).isAfter(cutoff)) {
                retained.add(candidate);
                continue;
            }

            requireInsideStagingRoot(candidate);
            if (!dryRun) {
                deleteRecursively(candidate);
            }
            removed.add(candidate);
        }

        log.info("staging cleanup: {}", new Outcome(removed, retained, dryRun).describe());
        return new Outcome(removed, retained, dryRun);
    }

    /**
     * Attempt directories: {@code <staging>/<stage>/<dataset>/<unit>/<owner>}.
     *
     * <p>Deletion happens at the attempt level, not at any level above it. Removing a whole
     * dataset directory would take other units' in-flight attempts with it.
     */
    private List<Path> attemptDirectories() {
        List<Path> attempts = new ArrayList<>();
        try (Stream<Path> walk = Files.walk(stagingRoot, 5)) {
            walk.filter(Files::isDirectory)
                    .filter(path -> stagingRoot.relativize(path).getNameCount() == 4)
                    .forEach(attempts::add);
        } catch (IOException e) {
            throw new UncheckedIOException("failed to scan " + stagingRoot, e);
        }
        return attempts;
    }

    /**
     * Second-layer containment check.
     *
     * <p>Stated accurately rather than overclaimed: <strong>this is not what stops a symlink escape
     * today.</strong> {@link Files#walk} does not follow symbolic links unless
     * {@code FOLLOW_LINKS} is passed, so a symlink inside staging pointing at the warehouse is
     * deleted as a link and its target is untouched. That is the primary defence, and it is
     * asserted directly in {@code StagingCleanerTest}.
     *
     * <p>This check is kept as defence in depth against a future change — someone adding
     * {@code FOLLOW_LINKS} to make cleanup handle a symlinked staging mount would otherwise turn
     * this class into a data-loss bug with no test failing. Because the primary defence already
     * holds, no mutation of this method can turn a test red, and it is deliberately not claimed as
     * a verified guarantee.
     */
    private void requireInsideStagingRoot(Path candidate) {
        // BOTH sides must be resolved the same way. Comparing a real path against a merely
        // normalized one is broken wherever the path traverses a symlink: on macOS /var is a
        // symlink to /private/var, so a candidate resolves to /private/var/... while the root
        // stays /var/... and every deletion is refused as an escape. Any symlinked mount does
        // the same thing.
        Path resolved = realPath(candidate);
        Path staging = realPath(stagingRoot);
        Path data = realPath(dataRoot);

        if (!resolved.startsWith(staging)) {
            throw new UnsafeCleanupException(
                    "refusing to delete " + resolved + ": outside the staging root "
                            + staging + " (symlink escape?)");
        }
        if (resolved.startsWith(data)) {
            throw new UnsafeCleanupException(
                    "refusing to delete " + resolved + ": inside the data root");
        }
        if (resolved.equals(staging)) {
            throw new UnsafeCleanupException("refusing to delete the staging root itself");
        }
    }

    private static Path normalize(Path path) {
        if (path == null) {
            throw new IllegalArgumentException("path must not be null");
        }
        // toAbsolutePath().normalize() collapses ../ textually, so a configured value like
        // /data/warehouse/../warehouse/staging cannot disguise where it actually points.
        return path.toAbsolutePath().normalize();
    }

    /** Resolves symlinks. Falls back to the normalized path when the target is already gone. */
    private static Path realPath(Path path) {
        try {
            return path.toRealPath();
        } catch (IOException e) {
            return normalize(path);
        }
    }

    private static Instant lastModified(Path path) {
        try {
            return Files.getLastModifiedTime(path).toInstant();
        } catch (IOException e) {
            // Unreadable timestamp: treat as recent so an unknown directory is retained rather
            // than deleted. Failing safe here costs disk; failing open costs data.
            return Instant.now();
        }
    }

    private static void deleteRecursively(Path path) {
        try (Stream<Path> walk = Files.walk(path)) {
            walk.sorted(Comparator.reverseOrder()).forEach(entry -> {
                try {
                    Files.deleteIfExists(entry);
                } catch (IOException e) {
                    throw new UncheckedIOException("failed to delete " + entry, e);
                }
            });
        } catch (IOException e) {
            throw new UncheckedIOException("failed to delete " + path, e);
        }
    }

    public Path stagingRoot() {
        return stagingRoot;
    }
}

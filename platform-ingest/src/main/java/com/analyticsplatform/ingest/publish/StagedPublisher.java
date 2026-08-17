package com.analyticsplatform.ingest.publish;

import com.analyticsplatform.ingest.publish.ProcessingUnitStore.Claim;
import com.analyticsplatform.ingest.publish.ProcessingUnitStore.Manifest;
import com.analyticsplatform.ingest.publish.ProcessingUnitStore.PublishException;
import com.analyticsplatform.ingest.publish.ProcessingUnitStore.UnitKey;
import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.file.AtomicMoveNotSupportedException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardCopyOption;
import java.util.Comparator;
import java.util.Optional;
import java.util.stream.Stream;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * The staged-publish protocol.
 *
 * <pre>
 *   claim (leased)
 *     → write to ATTEMPT-SPECIFIC staging
 *     → validate staged output
 *     → promote to DETERMINISTIC target
 *     → verify target against the staged fingerprint
 *     → INSERT manifest        &lt;-- COMMIT POINT (atomic)
 *     → mark COMPLETE          &lt;-- bookkeeping
 * </pre>
 *
 * <h2>What is and is not guaranteed</h2>
 *
 * <p>No claim is made that promotion is atomic. On a bind-mounted filesystem a directory replace is
 * not one operation, and pretending otherwise would be the kind of guarantee that holds until the
 * day it matters. What is guaranteed is weaker and true: <strong>the pipeline never incrementally
 * writes into the live partition</strong> — data is fully materialized and validated in staging
 * first — and every interruption point has a test proving the next run recovers.
 *
 * <p>The commit point is moved off the filesystem entirely and onto a single-row insert in
 * Postgres, which <em>is</em> atomic. The filesystem only has to be recoverable, not transactional.
 *
 * <h2>Files in the target are never trusted alone</h2>
 *
 * <p>An uncommitted target is indistinguishable from a partial write, so it is always discarded
 * rather than adopted. Adopting it would mean treating "some bytes exist" as "the write finished",
 * which is precisely the assumption that turns a crash into silent data loss.
 */
public final class StagedPublisher {

    private static final Logger log = LoggerFactory.getLogger(StagedPublisher.class);

    /** Writes a unit's data into the supplied staging directory. */
    @FunctionalInterface
    public interface StagedWrite {
        WriteResult write(Path stagingDirectory) throws Exception;
    }

    /** What the writer produced. Row count comes from the writer, not from re-reading. */
    public record WriteResult(long rowCount, String schemaHash) {
    }

    /** What reconciliation decided about an interrupted unit. */
    public enum Recovery {
        /** No commit record and nothing usable in the target. */
        NOT_COMMITTED,
        /** Files present but no commit record: uncommitted, discarded. */
        UNCOMMITTED_TARGET_DISCARDED,
        /** Commit record matches the target; only the status write was lost. */
        COMMITTED_STATUS_REPAIRED,
        /** Commit record contradicts the target; both discarded and rebuilt. */
        INCONSISTENT_REBUILT
    }

    /** Result of a publish attempt. */
    public record Outcome(boolean published, boolean skipped, String reason, long rowCount) {
        public static Outcome skipped(String reason) {
            return new Outcome(false, true, reason, 0);
        }

        public static Outcome published(long rowCount) {
            return new Outcome(true, false, "published", rowCount);
        }
    }

    private final ProcessingUnitStore store;
    private final Path stagingRoot;
    private final int leaseSeconds;

    public StagedPublisher(ProcessingUnitStore store, Path stagingRoot, int leaseSeconds) {
        this.store = store;
        this.stagingRoot = stagingRoot;
        this.leaseSeconds = leaseSeconds;
    }

    /**
     * Publishes one unit, or skips it if it is already committed.
     *
     * @param target the deterministic destination; unchanged across attempts
     */
    public Outcome publish(UnitKey key, long runId, String owner, Path target, StagedWrite write) {
        Recovery recovery = reconcile(key, target);
        if (recovery == Recovery.COMMITTED_STATUS_REPAIRED) {
            return Outcome.skipped("already committed; status repaired");
        }

        // No second manifest check here: reconcile() already returns COMMITTED_STATUS_REPAIRED
        // whenever a manifest exists and matches, and discards it otherwise, so a manifest can
        // never still be present at this point. A defensive re-check would be unreachable code
        // pretending to be a safety net.

        // Attempt-specific staging: a retry must never inherit a failed attempt's files. Only the
        // target path is deterministic.
        Path staging = stagingRoot
                .resolve(key.pipelineStage())
                .resolve(sanitize(key.datasetName()))
                .resolve(sanitize(key.processingUnit()))
                .resolve(owner);

        Optional<Claim> claim = store.claim(key, owner, leaseSeconds, staging.toString(), runId);
        if (claim.isEmpty()) {
            return Outcome.skipped("not claimable: committed, or actively owned elsewhere");
        }

        try {
            deleteRecursively(staging);
            Files.createDirectories(staging.getParent());

            WriteResult written = write.write(staging);
            FailPoint.check(FailPoint.Site.AFTER_STAGING_WRITE);

            UnitFingerprint staged =
                    UnitFingerprint.of(staging, written.rowCount(), written.schemaHash());
            validate(key, staged);
            FailPoint.check(FailPoint.Site.AFTER_STAGING_VALIDATION);

            promote(staging, target);
            FailPoint.check(FailPoint.Site.AFTER_PROMOTION);

            UnitFingerprint published =
                    UnitFingerprint.of(target, written.rowCount(), written.schemaHash());
            if (!staged.matches(published)) {
                throw new PublishException(
                        "published target does not match staging for " + key
                                + "\n  staged:    " + staged.describe()
                                + "\n  published: " + published.describe());
            }
            FailPoint.check(FailPoint.Site.AFTER_TARGET_VERIFICATION);

            // ---- COMMIT POINT ----
            store.commit(key, runId, published, target.toString());
            FailPoint.check(FailPoint.Site.AFTER_MANIFEST_WRITE);

            FailPoint.check(FailPoint.Site.BEFORE_COMPLETE);
            store.markComplete(key, owner, written.rowCount());

            log.info("published {} ({})", key, published.describe());
            return Outcome.published(written.rowCount());

        } catch (Exception e) {
            store.markFailed(key, e.getClass().getName() + ": " + e.getMessage());
            if (e instanceof RuntimeException runtime) {
                throw runtime;
            }
            throw new PublishException("failed to publish " + key, e);
        }
    }

    /**
     * Decides what an interrupted unit's on-disk state means.
     *
     * <p>The manifest is the authority. A target with no manifest is uncommitted no matter how
     * complete it looks.
     */
    public Recovery reconcile(UnitKey key, Path target) {
        Optional<Manifest> manifest = store.findManifest(key);
        boolean targetExists = Files.isDirectory(target);

        if (manifest.isEmpty()) {
            if (!targetExists) {
                return Recovery.NOT_COMMITTED;
            }
            // Files without a commit record. Indistinguishable from a partial write, so discard.
            log.warn("discarding uncommitted target for {} at {}", key, target);
            deleteRecursively(target);
            return Recovery.UNCOMMITTED_TARGET_DISCARDED;
        }

        Manifest committed = manifest.get();
        if (!targetExists) {
            log.error("manifest exists for {} but target {} is missing; rebuilding", key, target);
            discardCommit(key, "manifest present but target missing");
            return Recovery.INCONSISTENT_REBUILT;
        }

        UnitFingerprint actual = UnitFingerprint.of(
                target, committed.rowCount(), committed.schemaHash());
        if (!committed.fingerprint().matches(actual)) {
            log.error("manifest for {} contradicts the target; rebuilding\n  manifest: {}\n  actual:   {}",
                    key, committed.fingerprint().describe(), actual.describe());
            discardCommit(key, "manifest contradicts target: " + actual.describe());
            deleteRecursively(target);
            return Recovery.INCONSISTENT_REBUILT;
        }

        // Committed and intact. Only the status write was lost.
        if (store.repairToComplete(key)) {
            log.info("repaired status for already-committed unit {}", key);
        }
        return Recovery.COMMITTED_STATUS_REPAIRED;
    }

    /**
     * Abandons a commit that turned out to be wrong.
     *
     * <p>Both halves are required. Dropping the manifest alone leaves the unit COMPLETE, which the
     * claim statement refuses — so the corruption would be detected and then impossible to repair,
     * which is strictly worse than not detecting it.
     */
    private void discardCommit(UnitKey key, String reason) {
        store.discardManifest(key);
        if (!store.forceRebuild(key, reason)) {
            throw new PublishException(
                    "cannot rebuild " + key + ": a live worker still holds its lease");
        }
    }

    /** Staged output must be non-empty and self-consistent before it can reach the target. */
    private static void validate(UnitKey key, UnitFingerprint staged) {
        if (staged.fileCount() == 0) {
            throw new PublishException("staged output for " + key + " contains no data files");
        }
        if (staged.totalBytes() == 0) {
            throw new PublishException("staged output for " + key + " is zero bytes");
        }
        if (staged.rowCount() < 0) {
            throw new PublishException("staged output for " + key + " reports a negative row count");
        }
    }

    /**
     * Moves staging onto the target.
     *
     * <p>Tries an atomic rename first, which is genuinely atomic when both sides are on one
     * filesystem. Falls back to a recursive copy when they are not — and that fallback is
     * explicitly <em>not</em> atomic, which is why the manifest rather than the target is the
     * commit record.
     */
    private static void promote(Path staging, Path target) {
        try {
            FailPoint.check(FailPoint.Site.DURING_PROMOTION);
            Files.createDirectories(target.getParent());
            deleteRecursively(target);
            try {
                Files.move(staging, target, StandardCopyOption.ATOMIC_MOVE);
            } catch (AtomicMoveNotSupportedException e) {
                copyRecursively(staging, target);
                deleteRecursively(staging);
            }
        } catch (IOException e) {
            throw new UncheckedIOException("failed to promote " + staging + " to " + target, e);
        }
    }

    private static void copyRecursively(Path source, Path target) throws IOException {
        try (Stream<Path> walk = Files.walk(source)) {
            for (Path path : walk.toList()) {
                Path destination = target.resolve(source.relativize(path).toString());
                if (Files.isDirectory(path)) {
                    Files.createDirectories(destination);
                } else {
                    Files.createDirectories(destination.getParent());
                    Files.copy(path, destination, StandardCopyOption.REPLACE_EXISTING);
                }
            }
        }
    }

    static void deleteRecursively(Path path) {
        if (!Files.exists(path)) {
            return;
        }
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

    /** Path components must not contain separators from dataset or unit names. */
    private static String sanitize(String component) {
        return component.replace('/', '_').replace('\\', '_');
    }
}

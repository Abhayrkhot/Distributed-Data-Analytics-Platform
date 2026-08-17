package com.analyticsplatform.ingest.publish;

import java.util.Collections;
import java.util.EnumSet;
import java.util.Set;

/**
 * Deterministic crash injection for the publish protocol.
 *
 * <p>Recovery is the hardest thing this platform claims and the easiest thing to claim falsely.
 * "The retry works" is not evidence; crashing at a precise point and observing the next run
 * reconcile correctly is. That requires the production code to be interruptible at named
 * boundaries, so these calls live in the real publish path rather than in a test double.
 *
 * <p>That is a visible cost in {@link StagedPublisher} — a handful of {@code FailPoint.check(...)}
 * lines that do nothing in production. The alternative is a test double that reimplements the
 * protocol, which would verify the double rather than the code that runs.
 *
 * <h2>It cannot be armed by accident</h2>
 *
 * <p>{@link #arm} throws unless {@code -Dplatform.failpoints.enabled=true} is set, which surefire
 * sets and nothing else does. So a stray call in production code fails loudly at the arming site
 * rather than silently making a job abortable. {@link #check} costs one volatile read when
 * disarmed.
 */
public final class FailPoint {

    /** Boundaries the publish protocol can be interrupted at. */
    public enum Site {
        /** Staging written, not yet validated. */
        AFTER_STAGING_WRITE,
        /** Staging validated, target untouched. */
        AFTER_STAGING_VALIDATION,
        /** Mid-promotion: the target may be partially written. */
        DURING_PROMOTION,
        /** Target fully written, not yet verified. */
        AFTER_PROMOTION,
        /** Target verified against staging, manifest not yet written. Still uncommitted. */
        AFTER_TARGET_VERIFICATION,
        /** Manifest written — the unit IS committed — but status not yet updated. */
        AFTER_MANIFEST_WRITE,
        /** Everything done except the status write. */
        BEFORE_COMPLETE
    }

    private static final String ENABLE_PROPERTY = "platform.failpoints.enabled";

    /** Volatile so an armed site set on a test thread is visible to a Spark driver thread. */
    private static volatile Set<Site> armed = Collections.emptySet();

    private FailPoint() {
    }

    /**
     * Aborts if this site is armed. A no-op otherwise.
     *
     * @throws InjectedFailure when armed, simulating a process dying at this exact boundary
     */
    public static void check(Site site) {
        Set<Site> current = armed;
        if (!current.isEmpty() && current.contains(site)) {
            throw new InjectedFailure(site);
        }
    }

    /**
     * Arms one or more sites. Test-only.
     *
     * @throws IllegalStateException unless failpoints are explicitly enabled for this JVM
     */
    public static void arm(Site... sites) {
        if (!Boolean.getBoolean(ENABLE_PROPERTY)) {
            throw new IllegalStateException(
                    "fail points are disabled; set -D" + ENABLE_PROPERTY + "=true to arm them. "
                            + "If you are seeing this outside a test, a fail point was armed in "
                            + "production code, which is a bug.");
        }
        armed = sites.length == 0
                ? Collections.emptySet()
                : Collections.unmodifiableSet(EnumSet.of(sites[0], sites));
    }

    /** Disarms everything. Tests call this in teardown so one case cannot affect the next. */
    public static void disarm() {
        armed = Collections.emptySet();
    }

    /** Whether any site is currently armed. */
    public static boolean isArmed() {
        return !armed.isEmpty();
    }

    /** Thrown by an armed fail point; distinguishable from a genuine failure. */
    public static final class InjectedFailure extends RuntimeException {
        private final transient Site site;

        InjectedFailure(Site site) {
            super("injected failure at " + site);
            this.site = site;
        }

        public Site site() {
            return site;
        }
    }
}

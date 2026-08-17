package com.analyticsplatform.ingest.publish;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatCode;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import com.analyticsplatform.ingest.publish.FailPoint.InjectedFailure;
import com.analyticsplatform.ingest.publish.FailPoint.Site;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.EnumSource;

/**
 * The fail-point mechanism itself.
 *
 * <p>Worth testing directly because it lives in production code paths: if it were armable outside a
 * test, or if a disarm leaked between tests, the failure would show up as an unrelated job aborting
 * and would be very hard to trace back here.
 */
class FailPointTest {

    @AfterEach
    void disarm() {
        FailPoint.disarm();
    }

    @Test
    @DisplayName("disarmed checks do nothing")
    void disarmedIsANoOp() {
        FailPoint.disarm();

        for (Site site : Site.values()) {
            assertThatCode(() -> FailPoint.check(site)).doesNotThrowAnyException();
        }
        assertThat(FailPoint.isArmed()).isFalse();
    }

    @ParameterizedTest
    @EnumSource(Site.class)
    @DisplayName("an armed site throws, and only that site")
    void armedSiteThrows(Site armed) {
        FailPoint.arm(armed);

        assertThatThrownBy(() -> FailPoint.check(armed))
                .isInstanceOf(InjectedFailure.class)
                .hasMessageContaining(armed.name());

        for (Site other : Site.values()) {
            if (other != armed) {
                assertThatCode(() -> FailPoint.check(other))
                        .as("%s must not fire when %s is armed", other, armed)
                        .doesNotThrowAnyException();
            }
        }
    }

    @Test
    @DisplayName("several sites can be armed at once")
    void multipleSitesCanBeArmed() {
        FailPoint.arm(Site.AFTER_PROMOTION, Site.BEFORE_COMPLETE);

        assertThatThrownBy(() -> FailPoint.check(Site.AFTER_PROMOTION))
                .isInstanceOf(InjectedFailure.class);
        assertThatThrownBy(() -> FailPoint.check(Site.BEFORE_COMPLETE))
                .isInstanceOf(InjectedFailure.class);
        assertThatCode(() -> FailPoint.check(Site.DURING_PROMOTION))
                .doesNotThrowAnyException();
    }

    /** A leaked arming would make an unrelated later test fail for a baffling reason. */
    @Test
    @DisplayName("disarm clears everything")
    void disarmClearsState() {
        FailPoint.arm(Site.AFTER_MANIFEST_WRITE);
        assertThat(FailPoint.isArmed()).isTrue();

        FailPoint.disarm();

        assertThat(FailPoint.isArmed()).isFalse();
        assertThatCode(() -> FailPoint.check(Site.AFTER_MANIFEST_WRITE))
                .doesNotThrowAnyException();
    }

    @Test
    @DisplayName("the injected failure names its site")
    void failureCarriesItsSite() {
        FailPoint.arm(Site.DURING_PROMOTION);

        assertThatThrownBy(() -> FailPoint.check(Site.DURING_PROMOTION))
                .isInstanceOf(InjectedFailure.class)
                .extracting(e -> ((InjectedFailure) e).site())
                .isEqualTo(Site.DURING_PROMOTION);
    }

    /**
     * The safety property. Surefire sets the enabling property; production JVMs do not, so an
     * accidental arm() in shipped code throws at the call site rather than silently arming.
     */
    @Test
    @DisplayName("arming is refused when the enabling property is absent")
    void armingRequiresExplicitEnabling() {
        String property = "platform.failpoints.enabled";
        String original = System.getProperty(property);
        try {
            System.clearProperty(property);

            assertThatThrownBy(() -> FailPoint.arm(Site.AFTER_PROMOTION))
                    .isInstanceOf(IllegalStateException.class)
                    .hasMessageContaining("fail points are disabled");
        } finally {
            if (original != null) {
                System.setProperty(property, original);
            }
        }
    }
}

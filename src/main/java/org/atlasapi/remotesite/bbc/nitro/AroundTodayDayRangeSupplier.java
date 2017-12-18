package org.atlasapi.remotesite.bbc.nitro;

import com.google.common.base.Supplier;
import com.google.common.collect.Range;
import com.metabroadcast.common.time.Clock;
import com.metabroadcast.common.time.SystemClock;
import org.joda.time.LocalDate;

/**
 * <p>
 * Supplies {@link Range}s of {@link LocalDate}s around the current instant
 * according to the {@link Clock} with which it is configured.
 * </p>
 * 
 * <p>
 * The number of days backwards and forwards are used to create the lower and
 * upper bounds of the created {@code Range}, relative to the current instant.
 * </p>
 * 
 * <p>
 * The supplied range's bounds are closed (inclusive) at each end.
 * <ul>
 * <li>7 days forward, 7 days back: 15 day range</li>
 * <li>0 days forward, 7 days back: 8 day range</li>
 * <li>0 days forward, 0 days back: contains only today</li>
 * </ul>
 * </p>
 * 
 */
// It seems that when talking about ranges of days it's almost always inclusive
// both ends, e.g. 1st - 31st. If there are cases where exclusive ranges are
// needed then BoundType configuration can be added.
public class AroundTodayDayRangeSupplier implements Supplier<Range<LocalDate>> {

    public static final Builder builder() {
        return new Builder();
    }

    public static final class Builder {

        private Clock clock = new SystemClock();

        private int daysBack;
        private int daysForward;

        public Builder withClock(Clock clock) {
            this.clock = clock;
            return this;
        }

        public Builder withDaysForward(int forward) {
            this.daysForward = forward;
            return this;
        }

        public Builder withDaysBack(int back) {
            this.daysBack = back;
            return this;
        }

        public AroundTodayDayRangeSupplier build() {
            return new AroundTodayDayRangeSupplier(clock, daysBack, daysForward);
        }

    }

    private final Clock clock;

    private final int daysBack;
    private final int daysForward;

    public AroundTodayDayRangeSupplier(Clock clock, int daysBack, int daysForward) {
        this.clock = clock;
        this.daysBack = daysBack;
        this.daysForward = daysForward;
    }

    @Override
    public Range<LocalDate> get() {
        LocalDate today = clock.now().toLocalDate();
        LocalDate lower = today.minusDays(daysBack);
        LocalDate upper = today.plusDays(daysForward);
        return Range.closed(lower, upper);
    }

}

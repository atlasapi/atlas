package org.atlasapi.remotesite.youview;

import static com.google.common.base.Preconditions.checkNotNull;

import org.joda.time.DateTime;
import org.joda.time.Duration;

import com.metabroadcast.common.scheduling.ScheduledTask;
import com.metabroadcast.common.time.Clock;
import com.metabroadcast.common.time.SystemClock;


public class YouViewEquivalenceBreakerTask extends ScheduledTask {

    private static final Duration DAYS_INTO_FUTURE_OF_FURTHEST_PA_SCHEDULE = Duration.standardDays(13);
    private final YouViewEquivalenceBreaker equivalenceBreaker;
    private Clock clock;
    
    public YouViewEquivalenceBreakerTask(YouViewEquivalenceBreaker equivalenceBreaker) {
        this.equivalenceBreaker = checkNotNull(equivalenceBreaker);
        this.clock = new SystemClock();
        withName("YouView Equivalence Breaker");
    }
    
    @Override
    protected void runTask() {
        // Although we expect this to run daily, and hence could run for the schedule
        // period matching the farthest out schedule day we have from PA, since the 
        // job is idempotent (and a no-op at the very end of the schedule if there is no 
        // schedule) we'll create a window that overlaps with the previous run

        DateTime start = clock.now()
                              .plus(DAYS_INTO_FUTURE_OF_FURTHEST_PA_SCHEDULE)
                              .minusDays(1)
                              .toLocalDate()
                              .toDateTimeAtStartOfDay();
        
        DateTime end = start.plusDays(3);
        
        equivalenceBreaker.orphanItems(start, end);
        
    }

}

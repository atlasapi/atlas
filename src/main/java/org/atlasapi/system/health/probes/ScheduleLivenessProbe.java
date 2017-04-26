package org.atlasapi.system.health.probes;

import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Iterables;
import com.metabroadcast.common.health.probes.Probe;
import com.metabroadcast.common.health.probes.ProbeResult;
import org.atlasapi.media.channel.Channel;
import org.atlasapi.media.entity.Publisher;
import org.atlasapi.media.entity.Schedule;
import org.atlasapi.persistence.content.ScheduleResolver;
import org.joda.time.DateTime;

import java.util.concurrent.Callable;
import static java.lang.String.format;

public class ScheduleLivenessProbe extends Probe {

    private static final int TWELVE_POINT_FIVE_DAYS_IN_HOURS = 300;

    private final ScheduleResolver scheduleResolver;
    private final Iterable<Channel> channels;
    private final Publisher publisher;

    private ScheduleLivenessProbe(
            String identifier,
            ScheduleResolver scheduleResolver,
            Iterable<Channel> channels,
            Publisher publisher
    ) {
        super(identifier);
        this.scheduleResolver = scheduleResolver;
        this.channels = ImmutableSet.copyOf(channels);
        this.publisher = publisher;

    }

    public static ScheduleLivenessProbe create(
            String identifier,
            ScheduleResolver scheduleResolver,
            Iterable<Channel> channels,
            Publisher publisher
    ) {
        return new ScheduleLivenessProbe(identifier, scheduleResolver, channels, publisher);
    }


    public Callable<ProbeResult> createRequest() {
        return () -> {

            DateTime startTime = new DateTime().plusHours(TWELVE_POINT_FIVE_DAYS_IN_HOURS);
            DateTime endTime = startTime.plusHours(12);

            boolean healthy = true;
            StringBuilder errors = new StringBuilder();

            for(Channel channel : channels) {
                try {
                    Schedule schedule = scheduleResolver.unmergedSchedule(
                            startTime,
                            endTime,
                            ImmutableSet.of(channel),
                            ImmutableSet.of(publisher)
                    );

                    int itemCount = Iterables.getOnlyElement(schedule.scheduleChannels()).items().size();

                    if (itemCount <= 0) {
                        healthy = false;
                        errors.append(
                                format("{channel: %s, items: %d}", channel.getTitle(), itemCount)
                        );
                    }
                } catch (Exception e) {
                    return ProbeResult.unhealthy(identifier, e);
                }
            }

            return healthy ? ProbeResult.healthy(identifier)
                           : ProbeResult.unhealthy(identifier, errors.toString());
        };
    }
}

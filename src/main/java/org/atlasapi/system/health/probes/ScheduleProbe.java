package org.atlasapi.system.health.probes;

import java.util.List;
import java.util.concurrent.Callable;

import com.metabroadcast.common.health.probes.Probe;
import com.metabroadcast.common.health.probes.ProbeResult;
import org.atlasapi.media.channel.Channel;
import org.atlasapi.media.entity.Item;
import org.atlasapi.media.entity.Publisher;
import org.atlasapi.media.entity.Schedule;
import org.atlasapi.persistence.content.ScheduleResolver;
import org.joda.time.DateTime;

import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Iterables;
import com.metabroadcast.common.time.Clock;

import static com.google.common.base.Preconditions.checkNotNull;

public class ScheduleProbe extends Probe {

    private final Publisher publisher;
    private final Channel channel;
    private final ScheduleResolver scheduleResolver;
    private final Clock clock;

    private ScheduleProbe(Builder builder) {
        super(builder.identifier);
        publisher = checkNotNull(builder.publisher);
        scheduleResolver = checkNotNull(builder.scheduleResolver);
        clock = checkNotNull(builder.clock);

        channel = builder.channel;
    }

    public static Builder builder() {
        return new Builder();
    }

    @Override
    public Callable<ProbeResult> createRequest() {
        return () -> {
            if (channel == null) {
                return ProbeResult.unhealthy(identifier, "Channel not found");
            }

            DateTime dateFrom = clock.now().withTime(0, 0, 0, 0).minusMillis(1);
            DateTime dateTo = clock.now().withTime(0, 0, 0, 0).plusDays(1);

            Schedule schedule = scheduleResolver.unmergedSchedule(
                    dateFrom,
                    dateTo,
                    ImmutableSet.of(channel),
                    ImmutableSet.of(publisher)
            );

            List<Item> items = Iterables.getOnlyElement(schedule.scheduleChannels()).items();

            if (items.isEmpty()) {
                return ProbeResult.unhealthy(
                        identifier,
                        String.format("Schedule is empty: %s - %s", dateFrom, dateTo)
                );
            }

            return ProbeResult.healthy(identifier);
        };
    }

    public static final class Builder {

        private String identifier;
        private Publisher publisher;
        private Channel channel;
        private ScheduleResolver scheduleResolver;
        private Clock clock;

        private Builder() {
        }

        public Builder withIdentifier(String identifier) {
            this.identifier = identifier;
            return this;
        }

        public Builder withPublisher(Publisher publisher) {
            this.publisher = publisher;
            return this;
        }

        public Builder withChannel(Channel channel) {
            this.channel = channel;
            return this;
        }

        public Builder withScheduleResolver(ScheduleResolver scheduleResolver) {
            this.scheduleResolver = scheduleResolver;
            return this;
        }

        public Builder withClock(Clock clock) {
            this.clock = clock;
            return this;
        }

        public ScheduleProbe build() {
            return new ScheduleProbe(this);
        }
    }
}

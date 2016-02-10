package org.atlasapi.remotesite.pa;

import java.util.List;

import org.atlasapi.media.channel.Channel;
import org.atlasapi.media.entity.Item;
import org.atlasapi.media.entity.Publisher;
import org.atlasapi.media.entity.Schedule;
import org.atlasapi.persistence.content.ScheduleResolver;
import org.atlasapi.remotesite.pa.listings.bindings.ProgData;

import com.google.common.base.Optional;
import org.joda.time.DateTime;
import org.joda.time.DateTimeZone;
import org.joda.time.Duration;

import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Iterables;
import com.metabroadcast.common.time.Timestamp;

public class PaEmptyScheduleProcessor implements PaProgDataProcessor {
    
    private final PaProgDataProcessor delegate;
    private final ScheduleResolver scheduleResolver;

    public PaEmptyScheduleProcessor(PaProgDataProcessor delegate, ScheduleResolver scheduleResolver) {
        this.delegate = delegate;
        this.scheduleResolver = scheduleResolver;
    }

    @Override
    public Optional<ContentHierarchyAndSummaries> process(ProgData progData, Channel channel, DateTimeZone zone, Timestamp updatedAt) {
        if (emptySlot(progData, channel, zone)) {
            return delegate.process(progData, channel, zone, updatedAt);
        }
        else {
        	return Optional.absent();
        }
    }
    
    private boolean emptySlot(ProgData progData, Channel channel, DateTimeZone zone) {
        Duration duration = Duration.standardMinutes(Long.valueOf(progData.getDuration()));

        DateTime transmissionTime = PaProgrammeProcessor.getTransmissionTime(progData.getDate(), progData.getTime(), zone);
        DateTime transmissionEndTime = transmissionTime.plus(duration);
        
        Schedule schedule = scheduleResolver.unmergedSchedule(transmissionTime, transmissionEndTime, ImmutableSet.of(channel), ImmutableSet.of(Publisher.PA));
        if (schedule.scheduleChannels().isEmpty()) {
            return true;
        }
        
        List<Item> items = Iterables.getOnlyElement(schedule.scheduleChannels()).items();
        return items.isEmpty();
    }
}

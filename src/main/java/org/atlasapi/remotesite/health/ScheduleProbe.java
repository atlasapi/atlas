package org.atlasapi.remotesite.health;

import static com.metabroadcast.common.health.ProbeResult.ProbeResultType.FAILURE;
import static com.metabroadcast.common.health.ProbeResult.ProbeResultType.INFO;

import java.util.List;

import org.atlasapi.media.channel.Channel;
import org.atlasapi.media.entity.Broadcast;
import org.atlasapi.media.entity.Item;
import org.atlasapi.media.entity.Publisher;
import org.atlasapi.media.entity.Schedule;
import org.atlasapi.media.entity.ScheduleEntry;
import org.atlasapi.persistence.content.ScheduleResolver;
import org.joda.time.DateTime;
import org.joda.time.Duration;

import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Iterables;
import com.metabroadcast.common.health.HealthProbe;
import com.metabroadcast.common.health.ProbeResult;
import com.metabroadcast.common.health.ProbeResult.ProbeResultEntry;
import com.metabroadcast.common.time.Clock;

public class ScheduleProbe implements HealthProbe {

    private static final Duration MAX_GAP = Duration.standardMinutes(5);
    private static final Duration MAX_STALENESS = Duration.standardHours(1);
    
    private final Publisher publisher;
    private final Channel channel;
    private final ScheduleResolver scheduleResolver;
    private final Clock clock;

    public ScheduleProbe(Publisher publisher, Channel channel, ScheduleResolver scheduleResolver, Clock clock) {
        this.publisher = publisher;
        this.channel = channel;
        this.scheduleResolver = scheduleResolver;
        this.clock = clock;
    }

    @Override
    public ProbeResult probe() throws Exception {
        ProbeResult result = new ProbeResult(title());
        if (channel == null) {
            result.addInfo("Channel", "not found");
            return result;
        }
        
        DateTime date = clock.now().withTime(0, 0, 0, 0);

        Schedule schedule = scheduleResolver.unmergedSchedule(
                date.minusMillis(1),
                date.plusDays(1),
                ImmutableSet.of(channel),
                ImmutableSet.of(publisher)
        );
        List<Item> items = Iterables.getOnlyElement(schedule.scheduleChannels()).items();
        result.addEntry(scheduleSize(items.size()));

        if(items.isEmpty()) {
            return result;
        }

        addContiguityEntries(items, result);
        addLastFetchedCheckEntry(items, result);

        return result;
    }

    private ProbeResultEntry scheduleSize(int scheduleEntries) {
        return new ProbeResultEntry(scheduleEntries > 0 ? INFO : FAILURE, "Schedule Entries", String.valueOf(scheduleEntries));
    }

    private void addContiguityEntries(List<Item> items, ProbeResult result) {
        int breaks = 0;
        int overlaps = 0;

        DateTime lastEnd = ScheduleEntry.BROADCAST.apply(items.get(0)).getTransmissionTime();

        for (Item item: items) {
            Broadcast broadcast = ScheduleEntry.BROADCAST.apply(item);
            DateTime transmissionStart = broadcast.getTransmissionTime();
            
            if(transmissionStart.isAfter(lastEnd.plus(MAX_GAP))) {
                breaks++;
            } else if (transmissionStart.isBefore(lastEnd)) {
                overlaps++;
            }
            
            lastEnd = broadcast.getTransmissionEndTime();
        }

        result.add("Schedule Breaks", String.valueOf(breaks), !(breaks > 0));
        result.add("Schedule Overlaps", String.valueOf(overlaps), !(overlaps > 0));
    }

    private void addLastFetchedCheckEntry(List<Item> items, ProbeResult result) {
        DateTime oldestFetch = clock.now();
        
        for (Item item : items) {
            if(item.getLastFetched().isBefore(oldestFetch)) {
                oldestFetch = item.getLastFetched();
            }
        }
        
        result.add("Oldest Fetch", oldestFetch.toString("HH:mm:ss dd/MM/yyyy"), oldestFetch.isAfter(clock.now().minus(MAX_STALENESS)));
    }

    @Override
    public String title() {
        return String.format("Schedule %s: %s", publisher.title(), channel == null ? "Unknown Channel" : channel.getTitle());
    }

    @Override
    public String slug() {
        return String.format("%s%dschedule", publisher.name(), channel == null ? 0 : channel.getId());
    }
}

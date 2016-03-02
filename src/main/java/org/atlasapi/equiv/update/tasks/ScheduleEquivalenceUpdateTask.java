package org.atlasapi.equiv.update.tasks;

import static com.metabroadcast.common.scheduling.UpdateProgress.FAILURE;
import static com.metabroadcast.common.scheduling.UpdateProgress.SUCCESS;

import java.util.Iterator;
import java.util.List;

import org.atlasapi.equiv.update.EquivalenceUpdater;
import org.atlasapi.media.channel.Channel;
import org.atlasapi.media.entity.Content;
import org.atlasapi.media.entity.Item;
import org.atlasapi.media.entity.Publisher;
import org.atlasapi.media.entity.Schedule;
import org.atlasapi.media.entity.Schedule.ScheduleChannel;
import org.atlasapi.persistence.content.ScheduleResolver;

import com.google.common.collect.Lists;
import org.joda.time.LocalDate;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.collect.ImmutableList;
import com.metabroadcast.common.scheduling.ScheduledTask;
import com.metabroadcast.common.scheduling.UpdateProgress;
import com.metabroadcast.common.time.DayRange;
import com.metabroadcast.common.time.DayRangeGenerator;

public class ScheduleEquivalenceUpdateTask extends ScheduledTask {

    private final EquivalenceUpdater<Content> updater;
    private final ScheduleResolver scheduleResolver;
    private final List<Publisher> publishers;
    private final List<Channel> channels;
    private final int back;
    private final int forward;

    private final Logger log = LoggerFactory.getLogger(ScheduleEquivalenceUpdateTask.class);

    public static Builder builder() {
        return new Builder();
    }

    private ScheduleEquivalenceUpdateTask(EquivalenceUpdater<Content> updater,
            ScheduleResolver scheduleResolver, List<Publisher> publishers, List<Channel> channels,
            int back, int forward) {
        this.updater = updater;
        this.scheduleResolver = scheduleResolver;
        this.publishers = publishers;
        this.channels = channels;
        this.back = back;
        this.forward = forward;
    }

    @Override
    protected void runTask() {
        UpdateProgress progress = UpdateProgress.START;

        DayRange range = new DayRangeGenerator()
            .withLookBack(back)
            .withLookAhead(forward)
            .generate(new LocalDate());
        
        Iterator<LocalDate> dayIterator = range.iterator();
        LocalDate start, end;
        
        while(dayIterator.hasNext()) {
            start = dayIterator.next();
            end = start.plusDays(1);
            progress = progress.reduce(equivalateSchedule(start, end));
        }
        
        reportStatus(String.format("Finished. %d Items processed, %d failed", progress.getProcessed(), progress.getFailures()));
    }

    public UpdateProgress equivalateSchedule(LocalDate start, LocalDate end) {
        UpdateProgress progress = UpdateProgress.START;
        for (Publisher publisher : publishers) {
            for (Channel channel : channels) {
                if (!shouldContinue()) {
                    return progress;
                }
                
                Schedule schedule = scheduleResolver.unmergedSchedule(
                        start.toDateTimeAtStartOfDay(),
                        end.toDateTimeAtStartOfDay(),
                        ImmutableList.of(channel),
                        ImmutableList.of(publisher));
                
                Iterator<ScheduleChannel> channelItr = schedule.scheduleChannels().iterator();
                if (!channelItr.hasNext()) {
                    throw new RuntimeException(String.format(
                        "No schedule channel in schedule for %s, channel %s, from %s to %s", 
                        publisher.name(), 
                        channel.getTitle(), 
                        start.toString(), 
                        end.toString()
                    ));
                }
                ScheduleChannel scheduleChannel = channelItr.next();

                // It's better to run in reverse order, since the furthest point in the schedule
                // is likely to have not been run before, and if run last it's more subject
                // to job interruptions.
                //
                // If we were to run forwards, we would first recompute days which have been
                // computed before, rather than first running days at the end of the schedule for
                // the first time.
                Iterator<Item> channelItems = Lists.reverse(scheduleChannel.items()).iterator();
                while (channelItems.hasNext() && shouldContinue()) {
                    Item scheduleItem = channelItems.next();
                    progress = progress.reduce(process(scheduleItem));
                    reportStatus(generateStatus(start, end, progress, publisher, scheduleItem, channel));
                }
            }   
        }
        return progress;
    }

    private String generateStatus(LocalDate start, LocalDate end, UpdateProgress progress, Publisher publisher, Item item, Channel channel) {
        return String.format(
            "Updating %s on %s, with publisher %s, between %s and %s. Current progress: %d processed, %d failures",
            item.getCanonicalUri(),
            channel.getCanonicalUri(),
            publisher.name(), 
            start.toString(), 
            end.toString(),
            progress.getProcessed(),
            progress.getFailures()
        );
    }

    private UpdateProgress process(Item item) {
        try {
            updater.updateEquivalences(item);
            log.info("successfully updated equivalences on " + item.getCanonicalUri());
            return SUCCESS;
        } catch (Exception e) {
            log.error("Error updating equivalences on " + item.getCanonicalUri(), e);
            return FAILURE;
        }
    }

    public static class Builder {

        private EquivalenceUpdater<Content> updater;
        private ScheduleResolver scheduleResolver;
        private List<Publisher> publishers;
        private List<Channel> channels;
        private int back;
        private int forward;

        public ScheduleEquivalenceUpdateTask build() {
            return new ScheduleEquivalenceUpdateTask(
                    updater,
                    scheduleResolver,
                    publishers,
                    channels,
                    back,
                    forward);
        }

        private Builder() {
        }

        public Builder withUpdater(EquivalenceUpdater<Content> updater) {
            this.updater = updater;
            return this;
        }

        public Builder withScheduleResolver(ScheduleResolver scheduleResolver) {
            this.scheduleResolver = scheduleResolver;
            return this;
        }

        public Builder withPublishers(Iterable<Publisher> publishers) {
            this.publishers = ImmutableList.copyOf(publishers);
            return this;
        }

        public Builder withPublishers(Publisher... publishers) {
            return withPublishers(ImmutableList.copyOf(publishers));
        }

        public Builder withChannels(Iterable<Channel> channels) {
            this.channels = ImmutableList.copyOf(channels);
            return this;
        }

        public Builder withBack(int back) {
            this.back = back;
            return this;
        }

        public Builder withForward(int forward) {
            this.forward = forward;
            return this;
        }
    }
}

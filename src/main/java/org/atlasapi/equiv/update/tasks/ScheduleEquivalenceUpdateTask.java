package org.atlasapi.equiv.update.tasks;

import java.util.Iterator;
import java.util.List;
import java.util.Optional;

import org.atlasapi.equiv.ColumbusTelescopeReporter;
import org.atlasapi.equiv.update.EquivalenceUpdater;
import org.atlasapi.media.channel.Channel;
import org.atlasapi.media.entity.Content;
import org.atlasapi.media.entity.Identified;
import org.atlasapi.media.entity.Item;
import org.atlasapi.media.entity.Publisher;
import org.atlasapi.media.entity.Schedule;
import org.atlasapi.media.entity.Schedule.ScheduleChannel;
import org.atlasapi.persistence.content.ContentResolver;
import org.atlasapi.persistence.content.ResolvedContent;
import org.atlasapi.persistence.content.ScheduleResolver;

import com.metabroadcast.columbus.telescope.client.IngestTelescopeClientImpl;
import com.metabroadcast.common.base.Maybe;
import com.metabroadcast.common.scheduling.ScheduledTask;
import com.metabroadcast.common.scheduling.UpdateProgress;
import com.metabroadcast.common.time.DayRange;
import com.metabroadcast.common.time.DayRangeGenerator;

import com.google.common.base.Supplier;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Iterables;
import com.google.common.collect.Lists;
import org.joda.time.LocalDate;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static com.metabroadcast.common.scheduling.UpdateProgress.FAILURE;
import static com.metabroadcast.common.scheduling.UpdateProgress.SUCCESS;

public class ScheduleEquivalenceUpdateTask extends ScheduledTask {

    private final EquivalenceUpdater<Content> updater;
    private final ScheduleResolver scheduleResolver;
    private final List<Publisher> publishers;
    private final Supplier<Iterable<Channel>> channelsSupplier;
    private final ContentResolver contentResolver;
    private final int back;
    private final int forward;
    private final String reportingEnvironment;
    private final IngestTelescopeClientImpl telescopeClient;

    private final Logger log = LoggerFactory.getLogger(ScheduleEquivalenceUpdateTask.class);

    public static Builder builder() {
        return new Builder();
    }

    private ScheduleEquivalenceUpdateTask(
            ContentResolver contentResolver,
            EquivalenceUpdater<Content> updater,
            ScheduleResolver scheduleResolver,
            List<Publisher> publishers,
            Supplier<Iterable<Channel>> channelsSupplier,
            int back,
            int forward,
            String reportingEnvironment,
            IngestTelescopeClientImpl telescopeClient
    ) {
        this.contentResolver = contentResolver;
        this.updater = updater;
        this.scheduleResolver = scheduleResolver;
        this.publishers = publishers;
        this.channelsSupplier = channelsSupplier;
        this.back = back;
        this.forward = forward;
        this.reportingEnvironment = reportingEnvironment;
        this.telescopeClient = telescopeClient;
    }

    @Override
    protected void runTask() {
        ColumbusTelescopeReporter reporter = new ColumbusTelescopeReporter(telescopeClient);

        UpdateProgress progress = UpdateProgress.START;

        DayRange range = new DayRangeGenerator()
            .withLookBack(back)
            .withLookAhead(forward)
            .generate(new LocalDate());

        // It's better to run in reverse order, since the furthest point in the schedule
        // is likely to have not been run before, and if run last it's more subject
        // to job interruptions.
        //
        // If we were to run forwards, we would first recompute days which have been
        // computed before, rather than first running days at the end of the schedule for
        // the first time.

        Iterator<LocalDate> dayIterator = Lists.reverse(Lists.newArrayList(range.iterator()))
                .iterator();
        LocalDate start, end;

        Optional<String> taskId = reporter.startReporting(
                Iterables.getOnlyElement(publishers),
                reportingEnvironment
        );

        while(dayIterator.hasNext()) {
            start = dayIterator.next();
            end = start.plusDays(1);
            progress = progress.reduce(equivalateSchedule(start, end, taskId));
        }

        reporter.endReporting(taskId);

        reportStatus(String.format(
                "Finished. %d Items processed, %d failed",
                progress.getProcessed(),
                progress.getFailures())
        );
    }

    public UpdateProgress equivalateSchedule(
            LocalDate start,
            LocalDate end,
            Optional<String> taskId
    ) {
        UpdateProgress progress = UpdateProgress.START;
        for (Publisher publisher : publishers) {
            for (Channel channel : channelsSupplier.get()) {

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

                Iterator<Item> channelItems = scheduleChannel.items().iterator();
                while (channelItems.hasNext() && shouldContinue()) {
                    Item scheduleItem = channelItems.next();
                    ResolvedContent resolvedContent = contentResolver.findByCanonicalUris(
                            ImmutableSet.of(scheduleItem.getCanonicalUri()));
                    Maybe<Identified> identified = resolvedContent.get(scheduleItem.getCanonicalUri());
                    if (identified.hasValue()) {
                        Item value = (Item) identified.requireValue();
                        progress = progress.reduce(process(value, taskId, telescopeClient));
                        reportStatus(generateStatus(start, end, progress, publisher, scheduleItem, channel));
                    }
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

    private UpdateProgress process(
            Item item,
            Optional<String> taskId,
            IngestTelescopeClientImpl telescopeClient
    ) {
        try {
            updater.updateEquivalences(item, taskId, telescopeClient);
            log.info("successfully updated equivalences on " + item.getCanonicalUri());
            return SUCCESS;
        } catch (Exception e) {
            log.error("Error updating equivalences on " + item.getCanonicalUri(), e);
            return FAILURE;
        }
    }

    public static class Builder {

        private EquivalenceUpdater<Content> updater;
        private ContentResolver contentResolver;
        private ScheduleResolver scheduleResolver;
        private List<Publisher> publishers;
        private Supplier<Iterable<Channel>> channelsSupplier;
        private int back;
        private int forward;
        private String reportingEnvironment;
        private IngestTelescopeClientImpl telescopeClient;

        public ScheduleEquivalenceUpdateTask build() {
            return new ScheduleEquivalenceUpdateTask(
                    contentResolver,
                    updater,
                    scheduleResolver,
                    publishers,
                    channelsSupplier,
                    back,
                    forward,
                    reportingEnvironment,
                    telescopeClient
            );
        }

        private Builder() {
        }

        public Builder withUpdater(EquivalenceUpdater<Content> updater) {
            this.updater = updater;
            return this;
        }

        public Builder withContentResolver(ContentResolver contentResolver) {
            this.contentResolver = contentResolver;
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

        public Builder withChannelsSupplier(Supplier<Iterable<Channel>> channelsSupplier) {
            this.channelsSupplier = channelsSupplier;
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

        public Builder withReportingEnvironment(String reportingEnvironment) {
            this.reportingEnvironment = reportingEnvironment;
            return this;
        }

        public Builder withTelescopeClient(IngestTelescopeClientImpl telescopeClient) {
            this.telescopeClient = telescopeClient;
            return this;
        }
    }
}

package org.atlasapi.remotesite.pa;

import java.util.HashSet;
import java.util.Set;

import org.atlasapi.media.channel.Channel;
import org.atlasapi.media.entity.Publisher;
import org.atlasapi.media.entity.ScheduleEntry.ItemRefAndBroadcast;
import org.atlasapi.persistence.content.schedule.mongo.ScheduleWriter;
import org.atlasapi.remotesite.channel4.pmlsd.epg.BroadcastTrimmer;
import org.atlasapi.remotesite.pa.PaBaseProgrammeUpdater.PaChannelData;
import org.atlasapi.remotesite.pa.listings.bindings.ProgData;
import org.atlasapi.remotesite.pa.persistence.PaScheduleVersionStore;

import com.google.common.base.Optional;
import com.google.common.base.Strings;
import com.google.common.collect.ImmutableMap;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static com.google.common.base.Preconditions.checkNotNull;

public class PaChannelProcessor {

    private static final Logger log = LoggerFactory.getLogger(PaChannelProcessor.class);

    private final PaProgDataProcessor processor;
    private final BroadcastTrimmer trimmer;
    private final ScheduleWriter scheduleWriter;
    private final PaScheduleVersionStore scheduleVersionStore;
    private final ContentBuffer contentBuffer;

    private PaChannelProcessor(Builder builder) {
        this.processor = checkNotNull(builder.processor);
        this.trimmer = checkNotNull(builder.trimmer);
        this.scheduleWriter = checkNotNull(builder.scheduleWriter);
        this.scheduleVersionStore = checkNotNull(builder.scheduleVersionStore);
        this.contentBuffer = checkNotNull(builder.contentBuffer);
    }

    public static ProcessorStep builder() {
        return new Builder();
    }

    public int process(PaChannelData channelData, Set<String> currentlyProcessing) {
        int processed = 0;
        Set<ItemRefAndBroadcast> broadcasts = new HashSet<>();

        Channel channel = channelData.channel();

        ImmutableMap.Builder<String, String> acceptableBroadcastIds = ImmutableMap.builder();

        try {
            for (ProgData programme : channelData.programmes()) {
                String programmeLock = lockIdentifier(programme);
                lock(currentlyProcessing, programmeLock);

                try {
                    Optional<ContentHierarchyAndSummaries> possibleHierarchy = processor.process(
                            programme,
                            channel,
                            channelData.zone(),
                            channelData.lastUpdated()
                    );

                    if (possibleHierarchy.isPresent()) {
                        ContentHierarchyAndSummaries hierarchy = possibleHierarchy.get();
                        contentBuffer.add(hierarchy);

                        broadcasts.add(new ItemRefAndBroadcast(
                                hierarchy.getItem(),
                                hierarchy.getBroadcast()
                        ));

                        acceptableBroadcastIds.put(
                                hierarchy.getBroadcast().getSourceId(),
                                hierarchy.getItem().getCanonicalUri()
                        );
                    }

                    processed++;
                } catch (Exception e) {
                    log.error(
                            "Error processing channel {}, prog id {}",
                            channel.getKey(),
                            programme.getProgId()
                    );
                } finally {
                    unlock(currentlyProcessing, programmeLock);
                }
            }
        } catch (Exception e) {
            log.error("Error processing channel {}", channel.getKey(), e);
        }

        try {
            contentBuffer.flush();
        } catch (Exception e) {
            log.error("Error writing content for channel {}", channel.getKey(), e);
        }

        try {
            ImmutableMap<String, String> acceptableIds = acceptableBroadcastIds.build();
            log.trace("Trimming broadcasts for period {}; will remove IDs others than {}",
                    channelData.schedulePeriod(), acceptableIds
            );
            trimmer.trimBroadcasts(channelData.schedulePeriod(), channel, acceptableIds);

            scheduleWriter.replaceScheduleBlock(Publisher.PA, channel, broadcasts);

            log.trace(
                    "Storing version {} for channel {} on day {}",
                    channelData.version(),
                    channel,
                    channelData.scheduleDay()
            );

            scheduleVersionStore.store(channel, channelData.scheduleDay(), channelData.version());
        } catch (Exception e) {
            log.error(
                    "Error trimming and writing schedule for channel {}",
                    channel.getKey(),
                    e
            );
        }

        return processed;
    }

    private void unlock(Set<String> currentlyProcessing, String programmeLock) {
        synchronized (currentlyProcessing) {
            currentlyProcessing.remove(programmeLock);
            currentlyProcessing.notifyAll();
        }
    }

    private void lock(Set<String> currentlyProcessing, String programmeLock)
            throws InterruptedException {
        synchronized (currentlyProcessing) {
            while (currentlyProcessing.contains(programmeLock)) {
                currentlyProcessing.wait();
            }
            currentlyProcessing.add(programmeLock);
        }
    }

    private String lockIdentifier(ProgData programme) {
        return Strings.isNullOrEmpty(programme.getSeriesId())
               ? programme.getProgId()
               : programme.getSeriesId();
    }

    public interface ProcessorStep {

        TrimmerStep withProcessor(PaProgDataProcessor processor);
    }

    public interface TrimmerStep {

        ScheduleWriterStep withTrimmer(BroadcastTrimmer trimmer);
    }

    public interface ScheduleWriterStep {

        ScheduleVersionStoreStep withScheduleWriter(ScheduleWriter scheduleWriter);
    }

    public interface ScheduleVersionStoreStep {

        ContentBufferStep withScheduleVersionStore(PaScheduleVersionStore scheduleVersionStore);
    }

    public interface ContentBufferStep {

        BuildStep withContentBuffer(ContentBuffer contentBuffer);
    }

    public interface BuildStep {

        PaChannelProcessor build();
    }

    public static class Builder
            implements ProcessorStep, TrimmerStep, ScheduleWriterStep, ScheduleVersionStoreStep,
            ContentBufferStep, BuildStep {

        private PaProgDataProcessor processor;
        private BroadcastTrimmer trimmer;
        private ScheduleWriter scheduleWriter;
        private PaScheduleVersionStore scheduleVersionStore;
        private ContentBuffer contentBuffer;

        private Builder() {
        }

        @Override
        public TrimmerStep withProcessor(PaProgDataProcessor processor) {
            this.processor = processor;
            return this;
        }

        @Override
        public ScheduleWriterStep withTrimmer(BroadcastTrimmer trimmer) {
            this.trimmer = trimmer;
            return this;
        }

        @Override
        public ScheduleVersionStoreStep withScheduleWriter(ScheduleWriter scheduleWriter) {
            this.scheduleWriter = scheduleWriter;
            return this;
        }

        @Override
        public ContentBufferStep withScheduleVersionStore(
                PaScheduleVersionStore scheduleVersionStore
        ) {
            this.scheduleVersionStore = scheduleVersionStore;
            return this;
        }

        @Override
        public BuildStep withContentBuffer(ContentBuffer contentBuffer) {
            this.contentBuffer = contentBuffer;
            return this;
        }

        @Override
        public PaChannelProcessor build() {
            return new PaChannelProcessor(this);
        }
    }
}

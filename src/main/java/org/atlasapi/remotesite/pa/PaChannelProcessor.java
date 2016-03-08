package org.atlasapi.remotesite.pa;

import java.util.HashSet;
import java.util.Set;

import org.atlasapi.media.channel.Channel;
import org.atlasapi.media.entity.Publisher;
import org.atlasapi.media.entity.ScheduleEntry.ItemRefAndBroadcast;
import org.atlasapi.persistence.content.ContentWriter;
import org.atlasapi.persistence.content.schedule.mongo.ScheduleWriter;
import org.atlasapi.remotesite.channel4.epg.BroadcastTrimmer;
import org.atlasapi.remotesite.pa.PaBaseProgrammeUpdater.PaChannelData;
import org.atlasapi.remotesite.pa.listings.bindings.ProgData;
import org.atlasapi.remotesite.pa.persistence.PaScheduleVersionStore;

import com.google.common.base.Optional;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.base.Strings;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableMap.Builder;
import com.google.common.collect.Sets;

public class PaChannelProcessor {

    private static final Logger log = LoggerFactory.getLogger(PaChannelProcessor.class);
    private final PaProgDataProcessor processor;
    private final BroadcastTrimmer trimmer;
    private final ScheduleWriter scheduleWriter;
    private final PaScheduleVersionStore scheduleVersionStore;
    private final ContentBuffer contentBuffer;
    private final ContentWriter contentWriter;

    public PaChannelProcessor(PaProgDataProcessor processor, BroadcastTrimmer trimmer, ScheduleWriter scheduleWriter, 
            PaScheduleVersionStore scheduleVersionStore, ContentBuffer contentBuffer, ContentWriter contentWriter) {
        this.processor = processor;
        this.trimmer = trimmer;
        this.scheduleWriter = scheduleWriter;
        this.scheduleVersionStore = scheduleVersionStore;
        this.contentBuffer = contentBuffer;
        this.contentWriter = contentWriter;
    }

    public int process(PaChannelData channelData, Set<String> currentlyProcessing) {
        int processed = 0;
        Set<ItemRefAndBroadcast> broadcasts = new HashSet<ItemRefAndBroadcast>();
        Channel channel = channelData.channel();
        Set<ContentHierarchyAndSummaries> hierarchiesAndSummaries = Sets.newHashSet();
        Builder<String, String> acceptableBroadcastIds = ImmutableMap.builder();
        try {
            for (ProgData programme : channelData.programmes()) {
                String programmeLock = lockIdentifier(programme);
                lock(currentlyProcessing, programmeLock);
                try {

                    Optional<ContentHierarchyAndSummaries> possibleHierarchy = processor.process(programme, channel,
                            channelData.zone(), channelData.lastUpdated());
                    if (possibleHierarchy.isPresent()) {
                        ContentHierarchyAndSummaries hierarchy = possibleHierarchy.get();
                        contentBuffer.add(hierarchy);
                        hierarchiesAndSummaries.add(hierarchy);
                        broadcasts.add(new ItemRefAndBroadcast(hierarchy.getItem(), hierarchy.getBroadcast()));
                        acceptableBroadcastIds.put(hierarchy.getBroadcast().getSourceId(), hierarchy.getItem().getCanonicalUri());
                    }
                    processed++;
                } catch (Exception e) {
                    log.error(String.format("Error processing channel %s, prog id %s", channel.getKey(), programme.getProgId()));
                } finally {
                    unlock(currentlyProcessing, programmeLock);
                }
            }
        } catch (Exception e) {
            //TODO: should we just throw e?
            log.error(String.format("Error processing channel %s", channel.getKey()), e);
        } finally {
            writeContent(hierarchiesAndSummaries, channel);
        }
        
        try {
            if (trimmer != null) {
                ImmutableMap<String, String> acceptableIds = acceptableBroadcastIds.build();
                log.trace("Trimming broadcasts for period {}; will remove IDs others than {}",
                        channelData.schedulePeriod(), acceptableIds);
                trimmer.trimBroadcasts(channelData.schedulePeriod(), channel, acceptableIds);
            }
            scheduleWriter.replaceScheduleBlock(Publisher.PA, channel, broadcasts);

            log.trace("Storing version {} for channel {} on day {}",
                    channelData.version(),
                    channel,
                    channelData.scheduleDay());

            scheduleVersionStore.store(channel, channelData.scheduleDay(), channelData.version());
        } catch (Exception e) {
            log.error(String.format("Error trimming and writing schedule for channel %s", channel.getKey()), e);
        }
        
        return processed;
    }

    private void writeContent(Set<ContentHierarchyAndSummaries> hierarchies, Channel channel) {
        try {
            contentBuffer.flush();
            for (ContentHierarchyAndSummaries hierarchy : hierarchies) {
                if (hierarchy.getBrandSummary().isPresent()) {
                    contentWriter.createOrUpdate(hierarchy.getBrandSummary().get());
                }
                if (hierarchy.getSeriesSummary().isPresent()) {
                    contentWriter.createOrUpdate(hierarchy.getSeriesSummary().get());
                }
            }
        } catch (Exception e) {
            log.error(String.format("Error writing content for channel %s", channel.getKey()), e);
        }
    }
    
    private void unlock(Set<String> currentlyProcessing, String programmeLock) {
        synchronized (currentlyProcessing) {
            currentlyProcessing.remove(programmeLock);
            currentlyProcessing.notifyAll();
        }
    }

    private void lock(Set<String> currentlyProcessing, String programmeLock) throws InterruptedException {
        synchronized (currentlyProcessing) {
            while (currentlyProcessing.contains(programmeLock)) {
                currentlyProcessing.wait();
            }
            currentlyProcessing.add(programmeLock);
        }
    }

    private String lockIdentifier(ProgData programme) {
        return Strings.isNullOrEmpty(programme.getSeriesId()) ? programme.getProgId() : programme.getSeriesId();
    }
}

package org.atlasapi.remotesite.pa;

import java.io.DataInputStream;
import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.util.HashSet;
import java.util.Set;

import org.atlasapi.media.channel.Channel;
import org.atlasapi.media.entity.Brand;
import org.atlasapi.media.entity.Identified;
import org.atlasapi.media.entity.Publisher;
import org.atlasapi.media.entity.ScheduleEntry.ItemRefAndBroadcast;
import org.atlasapi.media.entity.Series;
import org.atlasapi.persistence.content.ContentWriter;
import org.atlasapi.persistence.content.schedule.mongo.ScheduleWriter;
import org.atlasapi.remotesite.channel4.epg.BroadcastTrimmer;
import org.atlasapi.remotesite.pa.PaBaseProgrammeUpdater.PaChannelData;
import org.atlasapi.remotesite.pa.listings.bindings.ProgData;
import org.atlasapi.remotesite.pa.persistence.PaScheduleVersionStore;

import com.metabroadcast.common.ingest.monitorclient.IngestMonitorClient;
import com.metabroadcast.common.ingest.monitorclient.model.Entity;
import com.metabroadcast.common.ingest.monitorclient.model.IngestResultUpdate;
import com.metabroadcast.common.ingest.monitorclient.model.ProcessingError;
import com.metabroadcast.common.ingest.monitorclient.model.ProcessingTime;
import com.metabroadcast.common.ingest.s3.process.ProcessingResult;

import com.google.common.base.Optional;
import com.google.common.collect.ImmutableList;
import org.apache.commons.codec.digest.DigestUtils;
import org.joda.time.DateTime;
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
    private final IngestMonitorClient ingestMonitorClient;

    public PaChannelProcessor(PaProgDataProcessor processor, BroadcastTrimmer trimmer, ScheduleWriter scheduleWriter, 
            PaScheduleVersionStore scheduleVersionStore, ContentBuffer contentBuffer, ContentWriter contentWriter,
            IngestMonitorClient ingestMonitorClient) {
        this.processor = processor;
        this.trimmer = trimmer;
        this.scheduleWriter = scheduleWriter;
        this.scheduleVersionStore = scheduleVersionStore;
        this.contentBuffer = contentBuffer;
        this.contentWriter = contentWriter;
        this.ingestMonitorClient = ingestMonitorClient;
    }

    public int process(PaChannelData channelData, File fileToProcess, Set<String> currentlyProcessing) {
        int processed = 0;
        ProcessingResult processingResult = ProcessingResult.builder().build();
        Optional<Float> idOptional = Optional.absent();
        try {
            idOptional = ingestMonitorClient.sendResult(processingResult,
                    DateTime.now(),
                    null,
                    fileToProcess.getPath(),
                    generateSha256Hash(fileToProcess.getAbsolutePath()));
        } catch (IOException e) {
            log.warn("Failed to generate hash from file {}",fileToProcess, e);
        }
        Set<ItemRefAndBroadcast> broadcasts = new HashSet<>();
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
            writeContent(hierarchiesAndSummaries, channel, idOptional);

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
            if (idOptional.isPresent()) {
                for (ItemRefAndBroadcast itemRefAndBroadcast : broadcasts) {
                    sendUpdateMessage(idOptional.get(), itemRefAndBroadcast.getItemUri(), "episode");
                }
            }
        } catch (Exception e) {
            String message = String.format(
                    "Error trimming and writing schedule for channel %s",
                    channel.getKey()
            );
            log.error(message, e);
            if (idOptional.isPresent()) {
                sendUpdateErrorMessage(channel, idOptional.get(), e, message);
            }
        }

        if (idOptional.isPresent()) {
            sendClosingMessage(idOptional.get(), DateTime.now());
        }
        
        return processed;
    }

    private void writeContent(Set<ContentHierarchyAndSummaries> hierarchies, Channel channel, Optional<Float> ingestId) {
        try {
            contentBuffer.flush();
            for (ContentHierarchyAndSummaries hierarchy : hierarchies) {
                Optional<Brand> brandSummary = hierarchy.getBrandSummary();
                if (brandSummary.isPresent()) {
                    contentWriter.createOrUpdate(brandSummary.get());
                    if (ingestId.isPresent()) {
                        sendUpdateMessage(ingestId.get(), brandSummary.get().getCanonicalUri(), "brand");
                    }
                }
                Optional<Series> seriesSummary = hierarchy.getSeriesSummary();
                if (seriesSummary.isPresent()) {
                    contentWriter.createOrUpdate(seriesSummary.get());
                    if (ingestId.isPresent()) {
                        sendUpdateMessage(ingestId.get(), seriesSummary.get().getCanonicalUri(), "series");
                    }
                }
            }
        } catch (Exception e) {
            String message = String.format("Error writing content for channel %s", channel.getKey());
            log.error(message, e);
            if (ingestId.isPresent()) {
                sendUpdateErrorMessage(channel, ingestId.get(), e, message);
            }
        }
    }

    private void sendUpdateErrorMessage(Channel channel, Float ingestId, Exception e,
            String message) {
        IngestResultUpdate update = new IngestResultUpdate();
        update.setId(ingestId);
        update.setErrorCount(1);
        ProcessingError error = new ProcessingError(channel.getTitle(), e.getMessage(), message);
        update.setErrors(ImmutableList.of(error));
    }

    private void sendUpdateMessage(Float ingestId, String uri,
            String type) {
        IngestResultUpdate update = new IngestResultUpdate();
        update.setProcessedEntityCount(1);
        update.setId(ingestId);
        Entity contentEntity = new Entity(
                uri,
                type
        );
        update.setAffectedEntities(ImmutableList.of(contentEntity));
        ingestMonitorClient.sendUpdatedResult(update);
    }

    private void sendClosingMessage(Float ingestId, DateTime timestamp) {
        IngestResultUpdate update = new IngestResultUpdate();
        update.setId(ingestId);
        ProcessingTime processingTime = new ProcessingTime(timestamp, timestamp);
        update.setProcessingTime(processingTime);
        ingestMonitorClient.sendUpdatedResult(update);
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

    private String generateSha256Hash(String absolutePath)
            throws IOException {
        return DigestUtils.md5Hex(absolutePath);
    }
}

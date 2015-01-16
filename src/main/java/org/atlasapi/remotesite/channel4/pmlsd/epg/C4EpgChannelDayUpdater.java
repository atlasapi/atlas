package org.atlasapi.remotesite.channel4.pmlsd.epg;

import static com.google.common.base.Preconditions.checkNotNull;

import java.util.List;
import java.util.Map;

import org.atlasapi.media.channel.Channel;
import org.atlasapi.media.entity.Brand;
import org.atlasapi.media.entity.Content;
import org.atlasapi.media.entity.Item;
import org.atlasapi.media.entity.Publisher;
import org.atlasapi.media.entity.ScheduleEntry.ItemRefAndBroadcast;
import org.atlasapi.media.entity.Series;
import org.atlasapi.persistence.content.ContentResolver;
import org.atlasapi.persistence.content.ContentWriter;
import org.atlasapi.persistence.content.schedule.mongo.ScheduleWriter;
import org.atlasapi.persistence.system.RemoteSiteClient;
import org.atlasapi.remotesite.channel4.epg.BroadcastTrimmer;
import org.atlasapi.remotesite.channel4.pmlsd.C4BrandUpdater;
import org.atlasapi.remotesite.channel4.pmlsd.C4LocationPolicyIds;
import org.atlasapi.remotesite.channel4.pmlsd.ContentFactory;
import org.atlasapi.remotesite.channel4.pmlsd.epg.model.C4EpgEntry;
import org.joda.time.DateTime;
import org.joda.time.Interval;
import org.joda.time.LocalDate;
import org.joda.time.LocalTime;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.api.client.repackaged.com.google.common.base.Preconditions;
import com.google.common.base.Optional;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Iterables;
import com.metabroadcast.common.scheduling.UpdateProgress;
import com.metabroadcast.common.time.DateTimeZones;

public class C4EpgChannelDayUpdater {

    private final static String epgUriPattern = "https://pmlsc.channel4.com/pmlsd/tv-listings/daily/%s/%s.atom";

    private final RemoteSiteClient<List<C4EpgEntry>> scheduleClient;
    private final ContentWriter writer;
    private final C4EpgEntryContentExtractor epgEntryContentExtractor;
    private final BroadcastTrimmer trimmer;
    private final Logger log = LoggerFactory.getLogger(getClass());
    private final Optional<String> platform;
    private final ScheduleWriter scheduleWriter;
    private final Publisher publisher;
    
    public C4EpgChannelDayUpdater(RemoteSiteClient<List<C4EpgEntry>> scheduleClient, ContentWriter writer, 
            ContentResolver resolver, C4BrandUpdater brandUpdater, BroadcastTrimmer trimmer,
            ScheduleWriter scheduleWriter, Publisher publisher, 
            ContentFactory<C4EpgEntry, C4EpgEntry, C4EpgEntry> contentFactory, Optional<String> platform, 
            C4LocationPolicyIds locationPolicyIds) {
        Preconditions.checkArgument(!platform.isPresent(), "If configuring the EPG updater for a platform, then modifications must be made to not write iOS app locations.");
        this.scheduleClient = checkNotNull(scheduleClient);
        this.writer = checkNotNull(writer);
        this.scheduleWriter = checkNotNull(scheduleWriter);
        this.platform = checkNotNull(platform);
        this.publisher = checkNotNull(publisher);
        this.epgEntryContentExtractor = new C4EpgEntryContentExtractor(resolver, brandUpdater, 
                contentFactory, publisher, locationPolicyIds);
        this.trimmer = checkNotNull(trimmer);
    }
    
    public UpdateProgress update(String channelUriKey, Channel channel, LocalDate scheduleDay) {
        String uri = uriFor(channelUriKey, scheduleDay);
        log.debug("Updating from {}", uri);

        try {
            List<C4EpgEntry> entries = getSchedule(uri);
            if(entries == null || entries.isEmpty()) {
                log.warn("Empty or null schedule at: {}", uri);
                return UpdateProgress.FAILURE;
            }
            
            List<Optional<ItemRefAndBroadcast>> processedItems = process(entries, channel);
            Iterable<ItemRefAndBroadcast> successfullyProcessed = Optional.presentInstances(processedItems);
            maintainSchedule(scheduleDay, channel, successfullyProcessed);
            int successfullyProcessedCount = Iterables.size(successfullyProcessed);
            return new UpdateProgress(successfullyProcessedCount, processedItems.size()-successfullyProcessedCount);
        } catch (Exception e) {
            log.error(uri, e);
            return UpdateProgress.FAILURE;
        }
    }
    
    private String uriFor(String channelKey, LocalDate scheduleDay) {
        String uri = String.format(epgUriPattern, scheduleDay.toString("yyyy/MM/dd"), channelKey);
        if (platform.isPresent()) {
            uri = uri + "?platform=" + platform.get();
        }
        return uri;
    }
    
    private List<C4EpgEntry> getSchedule(String uri) {
        try {
            return scheduleClient.get(uri);
        } catch (Exception e) {
            log.error("fetching " + uri, e);
            return null;
        }
    }

    private void maintainSchedule(LocalDate scheduleDay, Channel channel, Iterable<ItemRefAndBroadcast> processedItems) {
        DateTime scheduleStart = scheduleDay.toDateTime(new LocalTime(6,0,0), DateTimeZones.LONDON);
        Interval scheduleInterval = new Interval(scheduleStart, scheduleStart.plusDays(1));
        trimmer.trimBroadcasts(scheduleInterval, channel, broacastIdsFrom(processedItems));
        scheduleWriter.replaceScheduleBlock(publisher, channel, processedItems);
    }

    private Map<String, String> broacastIdsFrom(Iterable<ItemRefAndBroadcast> processedItems) {
        ImmutableMap.Builder<String, String> broadcastIdItemIdMap = ImmutableMap.builder();
        for (ItemRefAndBroadcast itemRefAndBroadcast : processedItems) {
            broadcastIdItemIdMap.put(itemRefAndBroadcast.getBroadcast().getSourceId(), itemRefAndBroadcast.getItemUri());
        }
        return broadcastIdItemIdMap.build();
    }

    private List<Optional<ItemRefAndBroadcast>> process(Iterable<C4EpgEntry> entries, Channel channel) {
        ImmutableList.Builder<Optional<ItemRefAndBroadcast>> episodes = ImmutableList.builder();
        for (C4EpgEntry entry : entries) {
            Optional<ItemRefAndBroadcast> itemAndBroadcast = processEntry(channel, entry);
            if (itemAndBroadcast != null) {
                episodes.add(itemAndBroadcast);
            }
        }
        return episodes.build();
    }

    private Optional<ItemRefAndBroadcast> processEntry(Channel channel, C4EpgEntry entry) {
        ItemRefAndBroadcast itemAndBroadcast = null;
        try {
            ContentHierarchyAndBroadcast extractedContent = epgEntryContentExtractor.extract(new C4EpgChannelEntry(entry, channel));
            if (extractedContent.getBrand().isPresent()) {
                Brand brand = extractedContent.getBrand().get();
                checkUri(brand);
                checkSource(brand);
                writer.createOrUpdate(brand);
            }
            if (extractedContent.getSeries().isPresent()) {
                Series series = extractedContent.getSeries().get();
                checkUri(series);
                checkSource(series);
                writer.createOrUpdate(series);
            }
            Item item = extractedContent.getItem();
            checkUri(item);
            checkSource(item);
            writer.createOrUpdate(item);
            itemAndBroadcast = new ItemRefAndBroadcast(item, extractedContent.getBroadcast());
        } catch (Exception e) {
            log.error(entry.id(), e);
        }
        return Optional.fromNullable(itemAndBroadcast);
    }

    private void checkUri(Content c) {
//        checkArgument(c.getCanonicalUri().startsWith(C4AtomApi.PROGRAMMES_BASE),
//            "%s doesn't start with %s", c, C4AtomApi.PROGRAMMES_BASE);
    }

    private void checkSource(Content c) {
//        checkArgument(c.getPublisher().equals(Publisher.C4_PMLSD),
//            "%s not %s", c, Publisher.C4_PMLSD);
    }

}

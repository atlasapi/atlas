package org.atlasapi.remotesite.youview;

import static com.google.common.base.Preconditions.checkNotNull;

import org.atlasapi.equiv.ContentRef;
import org.atlasapi.media.entity.Alias;
import org.atlasapi.media.entity.Broadcast;
import org.atlasapi.media.entity.Identified;
import org.atlasapi.media.entity.Item;
import org.atlasapi.media.entity.LookupRef;
import org.atlasapi.media.entity.Publisher;
import org.atlasapi.media.entity.Schedule;
import org.atlasapi.media.entity.Version;
import org.atlasapi.persistence.content.ContentResolver;
import org.atlasapi.persistence.content.ScheduleResolver;
import org.atlasapi.persistence.lookup.LookupWriter;
import org.atlasapi.persistence.lookup.entry.LookupEntry;
import org.atlasapi.persistence.lookup.entry.LookupEntryStore;
import org.joda.time.DateTime;
import org.slf4j.Logger;

import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Iterables;


/**
 * Orphans items from the rest of their equivalent set, using the schedule
 * of a provided publisher to identify candidates for being orphaned.
 * 
 * Candidates are considered if they are equivalent to items in the 
 * schedule, and are items of the publishers in publishersToOrphan. They will
 * be orphaned if their latest broadcast is more than 30 days in the past,
 * or they don't have a scheduleevent {@link Alias}
 * 
 * @author tom
 *
 */
public class YouViewEquivalenceBreaker {

    private static final Logger log = org.slf4j.LoggerFactory.getLogger(YouViewEquivalenceBreaker.class);
    
    private final ScheduleResolver scheduleResolver;
    private final YouViewChannelResolver youViewChannelResolver;
    private final LookupEntryStore lookupEntryStore;
    private final ContentResolver contentResolver;
    private final LookupWriter lookupWriter;
    private final ImmutableSet<Publisher> publishersToOrphan;
    private final Publisher referenceSchedulePublisher;

    public YouViewEquivalenceBreaker(ScheduleResolver scheduleResolver, YouViewChannelResolver youViewChannelResolver,
            LookupEntryStore lookupEntryStore, ContentResolver contentResolver, LookupWriter lookupWriter,
            Publisher referenceSchedulePublisher, Iterable<Publisher> publishersToOrphan) {
        this.youViewChannelResolver = checkNotNull(youViewChannelResolver);
        this.scheduleResolver = checkNotNull(scheduleResolver);
        this.lookupEntryStore = checkNotNull(lookupEntryStore);
        this.contentResolver = checkNotNull(contentResolver);
        this.lookupWriter = checkNotNull(lookupWriter);
        this.referenceSchedulePublisher = checkNotNull(referenceSchedulePublisher);
        this.publishersToOrphan = ImmutableSet.copyOf(publishersToOrphan);
    }
    
    public void orphanItems(DateTime from, DateTime to) {
        for (org.atlasapi.media.channel.Channel channel : youViewChannelResolver.getAllChannels()) {
            process(scheduleResolver.unmergedSchedule(from, to, ImmutableSet.of(channel), ImmutableSet.of(referenceSchedulePublisher)));
        }
    }

    private void process(Schedule schedule) {
        for (Item item : Iterables.getOnlyElement(schedule.scheduleChannels()).items()) {
            LookupEntry lookupEntry = Iterables.getOnlyElement(lookupEntryStore.entriesForCanonicalUris(ImmutableSet.of(item.getCanonicalUri())));
            for (LookupRef lookupRef : lookupEntry.equivalents()) {
                if (publishersToOrphan.contains(lookupRef.publisher())
                        && !lookupRef.uri().equals(item.getCanonicalUri())) {
                    Item equiv = (Item) contentResolver.findByCanonicalUris(ImmutableSet.of(lookupRef.uri())).getFirstValue().requireValue();
                    if (shouldOrphan(equiv)) {
                        log.trace("Orphaning item {}", equiv.getCanonicalUri());
                        orphan(equiv);
                    } else {
                        log.trace("Not orphaning item {}", equiv.getCanonicalUri());
                    }
                }
            }
        }
        
    }

    private void orphan(Item equiv) {
        lookupWriter.writeLookup(ContentRef.valueOf(equiv), 
                Iterables.transform(ImmutableSet.of(equiv), ContentRef.FROM_CONTENT), Publisher.all());
    }

    private boolean shouldOrphan(Identified identified) {
        Item item = (Item) identified;
        Iterable<Broadcast> broadcasts = Iterables.concat(Iterables.transform(item.getVersions(), Version.TO_BROADCASTS));
        
        DateTime latestTxTime = null;
        
        for (Broadcast broadcast : broadcasts) {
            if (latestTxTime == null 
                    || broadcast.getTransmissionTime().isAfter(latestTxTime)) {
                latestTxTime = broadcast.getTransmissionTime();
            }
        }
        
        if (latestTxTime != null 
                && latestTxTime.isBefore(DateTime.now().minusDays(30))) {
            return true;
        }
        
        for (Alias alias : identified.getAliases()) {
            if (alias.getNamespace().contains(":scheduleevent")) {
                // this is still the valid item on a schedule, so retain it
                return false;
            }
        }
        return true;
    }

}

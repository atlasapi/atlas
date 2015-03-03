package org.atlasapi.remotesite.youview;

import static com.google.common.base.Preconditions.checkNotNull;

import java.util.Set;

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
import org.atlasapi.persistence.lookup.entry.LookupEntry;
import org.atlasapi.persistence.lookup.entry.LookupEntryStore;
import org.joda.time.DateTime;
import org.slf4j.Logger;

import com.google.common.base.Predicate;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Iterables;
import com.google.common.collect.Sets;


/**
 * Orphans items from the rest of their equivalent set, using the schedule
 * of a provided publisher to identify candidates for being orphaned.
 * 
 * Candidates are considered if they are equivalent to items in the 
 * schedule, and are items of the publishers in publishersToOrphan. They will
 * be orphaned if their latest broadcast is more than 21 days in the past,
 * or they don't have a scheduleevent {@link Alias}
 * 
 * @author tom
 *
 */
public class YouViewEquivalenceBreaker {

    private static final Logger log = org.slf4j.LoggerFactory.getLogger(YouViewEquivalenceBreaker.class);
    private static final Integer EQUIVALENCE_DAYS_TO_KEEP = 21;
    
    private final ScheduleResolver scheduleResolver;
    private final YouViewChannelResolver youViewChannelResolver;
    private final LookupEntryStore lookupEntryStore;
    private final ContentResolver contentResolver;
    private final ImmutableSet<Publisher> publishersToOrphan;
    private final Publisher referenceSchedulePublisher;

    public YouViewEquivalenceBreaker(ScheduleResolver scheduleResolver, YouViewChannelResolver youViewChannelResolver,
            LookupEntryStore lookupEntryStore, ContentResolver contentResolver, Publisher referenceSchedulePublisher, 
            Iterable<Publisher> publishersToOrphan) {
        this.youViewChannelResolver = checkNotNull(youViewChannelResolver);
        this.scheduleResolver = checkNotNull(scheduleResolver);
        this.lookupEntryStore = checkNotNull(lookupEntryStore);
        this.contentResolver = checkNotNull(contentResolver);
        this.referenceSchedulePublisher = checkNotNull(referenceSchedulePublisher);
        this.publishersToOrphan = ImmutableSet.copyOf(publishersToOrphan);
    }
    
    
    public void orphanItems(DateTime from, DateTime to) {
        for (org.atlasapi.media.channel.Channel channel : youViewChannelResolver.getAllChannels()) {
            log.debug("Searching for orphans on channel {} ({}) between {} and {}", 
                    new String[] { String.valueOf(channel.getId()), channel.getTitle(), from.toString(), to.toString() });
            process(scheduleResolver.unmergedSchedule(from, to, ImmutableSet.of(channel), ImmutableSet.of(referenceSchedulePublisher)));
        }
    }

    private void process(Schedule schedule) {
        for (Item item : Iterables.getOnlyElement(schedule.scheduleChannels()).items()) {
            LookupEntry lookupEntry = Iterables.getOnlyElement(lookupEntryStore.entriesForCanonicalUris(ImmutableSet.of(item.getCanonicalUri())));
            Set<String> toOrphan = Sets.newHashSet();
            for (LookupRef lookupRef : lookupEntry.equivalents()) {
                if (publishersToOrphan.contains(lookupRef.publisher())
                        && !lookupRef.uri().equals(item.getCanonicalUri())) {
                    Item equiv = (Item) contentResolver.findByCanonicalUris(ImmutableSet.of(lookupRef.uri())).getFirstValue().requireValue();
                    if (shouldOrphan(equiv)) {
                        toOrphan.add(equiv.getCanonicalUri());
                        log.trace("Orphaning item {}", equiv.getCanonicalUri());
                    } else {
                        log.trace("Not orphaning item {}", equiv.getCanonicalUri());
                    }
                }
            }
            if (!toOrphan.isEmpty()) {
                orphanFromEquivalentSet(lookupEntry, toOrphan);
            }
        }
        
    }

    private void orphanFromEquivalentSet(LookupEntry lookupEntry, Set<String> toOrphan) {
        Predicate<LookupRef> shouldRetainLookupRef = createShouldRetainLookupRefPredicate(toOrphan);
        for (LookupRef equivalentLookupRef : Sets.union(Sets.union(lookupEntry.equivalents(), lookupEntry.directEquivalents()), lookupEntry.explicitEquivalents())) {
            LookupEntry entry = Iterables.getOnlyElement(lookupEntryStore.entriesForCanonicalUris(ImmutableSet.of(equivalentLookupRef.uri())));
            if (toOrphan.contains(entry.uri())) {
                lookupEntryStore.store(createOrphanedLookupEntry(entry));
            } else {
                lookupEntryStore.store(createFilteredLookupEntry(entry, shouldRetainLookupRef));
            }
        }
    }

    private LookupEntry createOrphanedLookupEntry(LookupEntry entry) {
        return entry.copyWithDirectEquivalents(ImmutableSet.<LookupRef>of())
                    .copyWithEquivalents(ImmutableSet.<LookupRef>of())
                    .copyWithExplicitEquivalents(ImmutableSet.<LookupRef>of());
    }

    private LookupEntry createFilteredLookupEntry(LookupEntry entry, Predicate<LookupRef> shouldRetainLookupRef) {
        return entry.copyWithDirectEquivalents(Iterables.filter(entry.directEquivalents(), shouldRetainLookupRef))
                    .copyWithEquivalents(Iterables.filter(entry.equivalents(), shouldRetainLookupRef))
                    .copyWithExplicitEquivalents(Iterables.filter(entry.explicitEquivalents(), shouldRetainLookupRef));
        
    }

    private Predicate<LookupRef> createShouldRetainLookupRefPredicate(final Set<String> toOrphan) {
        return new Predicate<LookupRef>() {

            @Override
            public boolean apply(LookupRef input) {
                return !toOrphan.contains(input.uri());
            }
        };
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
                && latestTxTime.isBefore(DateTime.now().minusDays(EQUIVALENCE_DAYS_TO_KEEP))) {
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

package org.atlasapi.remotesite.metabroadcast.picks;

import static com.google.common.collect.Iterables.concat;
import static com.google.common.collect.Iterables.filter;
import static com.google.common.collect.Iterables.transform;

import java.util.List;
import java.util.Map;

import org.atlasapi.media.entity.ChildRef;
import org.atlasapi.media.entity.ContentGroup;
import org.atlasapi.media.entity.Identified;
import org.atlasapi.media.entity.Item;
import org.atlasapi.media.entity.Publisher;
import org.atlasapi.media.entity.Schedule.ScheduleChannel;
import org.atlasapi.media.util.ItemAndBroadcast;
import org.atlasapi.persistence.content.ContentGroupResolver;
import org.atlasapi.persistence.content.ContentGroupWriter;
import org.atlasapi.persistence.content.ContentResolver;
import org.atlasapi.persistence.content.ResolvedContent;
import org.atlasapi.persistence.content.ScheduleResolver;
import org.atlasapi.remotesite.bbc.nitro.ChannelDay;
import org.atlasapi.remotesite.bbc.nitro.ChannelDayProcessor;
import org.joda.time.DateTimeZone;
import org.joda.time.Duration;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.base.Function;
import com.google.common.base.Predicate;
import com.google.common.base.Predicates;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Iterables;
import com.metabroadcast.common.base.Maybe;
import com.metabroadcast.common.scheduling.UpdateProgress;


public class PicksDayUpdater implements ChannelDayProcessor {

    private static final int MAX_CONTENT_GROUP_SIZE = 10000;
    private static final int LARGE_BRAND_SIZE = 1000;
    private static final Duration SHORT_BROADCAST_LENGTH = Duration.standardMinutes(10);
    
    private static final Logger log = LoggerFactory.getLogger(PicksDayUpdater.class);
    
    private static final String CONTENT_GROUP = "http://picks.metabroadcast.com/schedule-picks";
    private static final String SOAP_GENRE = "http://pressassociation.com/genres/1500";
    
    private final ScheduleResolver scheduleResolver;
    private final ContentGroupResolver contentGroupResolver;
    private final ContentResolver contentResolver;
    private final ContentGroupWriter contentGroupWriter;
    private final Predicate<ItemAndBroadcast> picksPredicate;
    
    public PicksDayUpdater(ScheduleResolver scheduleResolver, ContentGroupResolver contentGroupResolver,
            ContentResolver contentResolver, ContentGroupWriter contentGroupWriter, PicksChannelsSupplier picksChannelsSupplier) {
        this.scheduleResolver = scheduleResolver;
        this.contentGroupResolver = contentGroupResolver;
        this.contentGroupWriter = contentGroupWriter;
        this.contentResolver = contentResolver;
        this.picksPredicate = Predicates.and(ImmutableList.of(
                new InterestingItemAndBroadcastPredicate(picksChannelsSupplier.get()), 
                Predicates.or(
                                Predicates.not(new ItemInLargeBrandPredicate(contentResolver, LARGE_BRAND_SIZE)),
                                Predicates.and(
                                        new GenreMatchPredicate(ImmutableSet.of(SOAP_GENRE)),
                                        new FirstBroadcastPredicate()
                                )
                             ),
                Predicates.not(new ShortBroadcastPredicate(SHORT_BROADCAST_LENGTH)))
            );
    }
    
    @Override
    public UpdateProgress process(ChannelDay channelDay) throws Exception {
        try {
            Iterable<Item> picks = concat(transform(scheduleResolver.unmergedSchedule(
                    channelDay.getDay().toDateTimeAtStartOfDay(DateTimeZone.UTC), 
                    channelDay.getDay().plusDays(1).toDateTimeAtStartOfDay(DateTimeZone.UTC), 
                    ImmutableSet.of(channelDay.getChannel()), 
                    ImmutableSet.of(Publisher.PA)).scheduleChannels(), TO_ITEMS));
            
            // We need to resolve the content to get all broadcasts, since items returned
            // from a schedule request only have a single broadcast corresponding to the
            // schedule slot
            final Map<String, Maybe<Identified>> resolved = 
                    contentResolver.findByCanonicalUris(
                            ImmutableSet.copyOf(Iterables.transform(picks, Identified.TO_URI))
                    ).asMap();
            
            Iterable<ItemAndBroadcast> itemsAndBroadcasts = Iterables.transform(picks, new Function<Item, ItemAndBroadcast>() {

                @Override
                public ItemAndBroadcast apply(Item item) {
                    return new ItemAndBroadcast((Item) resolved.get(item.getCanonicalUri()).requireValue(), 
                            Maybe.just(Iterables.getOnlyElement(Item.FLATTEN_BROADCASTS.apply(item))));
                }
                
            });
            
            addPicksToContentGroup(findPicks(itemsAndBroadcasts));
            
            return UpdateProgress.SUCCESS;
        } catch (Exception e) {
            log.error("Processing " + channelDay.getChannel().getCanonicalUri() 
                    + " [" + channelDay.getChannel().getTitle() + "] Day " 
                    + channelDay.getDay().toString(), e);
            return UpdateProgress.FAILURE;
        }
    }

    private Iterable<Item> findPicks(Iterable<ItemAndBroadcast> itemsAndBroadcasts) {
        return transform(filter(itemsAndBroadcasts, picksPredicate), TO_ITEM);
    }
    
    private void addPicksToContentGroup(Iterable<Item> items) {
        ContentGroup contentGroup = resolveOrCreateContentGroup();
        Iterable<ChildRef> childRefs = transform(items, Item.TO_CHILD_REF);
        pruneContents(contentGroup);
        boolean changed = false;
        for (ChildRef childRef : childRefs) {
            
            if (!contentGroup.getContents().contains(childRef)) {
                changed = true;
                contentGroup.addContent(childRef);
            }
        }
        if (changed) {
            contentGroupWriter.createOrUpdate(contentGroup);
        }
    }
    
    // The picks should be kept to a finite size, else we'll hit document size limits in mongo
    private void pruneContents(ContentGroup contentGroup) {
        ImmutableList<ChildRef> contents = contentGroup.getContents();
        int size = contentGroup.getContents().size();
        if (size > MAX_CONTENT_GROUP_SIZE) {
            contentGroup.setContents(Iterables.skip(contents, size - MAX_CONTENT_GROUP_SIZE));
        }
    }

    private ContentGroup resolveOrCreateContentGroup() {
        ResolvedContent contentGroup = contentGroupResolver.findByCanonicalUris(ImmutableSet.of(CONTENT_GROUP));
        Maybe<Identified> first = contentGroup.getFirstValue();
        
        if (first.hasValue()) {
            return (ContentGroup) first.requireValue();
        }
        
        return new ContentGroup(CONTENT_GROUP, Publisher.METABROADCAST_PICKS);
    }
    
    private static final Function<ScheduleChannel, List<Item>> TO_ITEMS = new Function<ScheduleChannel, List<Item>>() {

        @Override
        public List<Item> apply(ScheduleChannel scheduleChannel) {
            return scheduleChannel.items();
        }
    };
    
    private static final Function<ItemAndBroadcast, Item> TO_ITEM = new Function<ItemAndBroadcast, Item>() {

        @Override
        public Item apply(ItemAndBroadcast itemAndBroadcast) {
            return itemAndBroadcast.getItem();
        }
        
    };
}

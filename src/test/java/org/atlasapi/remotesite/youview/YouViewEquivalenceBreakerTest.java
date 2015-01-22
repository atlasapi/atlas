package org.atlasapi.remotesite.youview;

import static org.junit.Assert.assertEquals;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.util.Set;

import org.atlasapi.equiv.ContentRef;
import org.atlasapi.media.channel.Channel;
import org.atlasapi.media.entity.Item;
import org.atlasapi.media.entity.LookupRef;
import org.atlasapi.media.entity.MediaType;
import org.atlasapi.media.entity.Publisher;
import org.atlasapi.media.entity.Schedule;
import org.atlasapi.media.entity.Schedule.ScheduleChannel;
import org.atlasapi.media.entity.testing.ComplexItemTestDataBuilder;
import org.atlasapi.persistence.content.ContentResolver;
import org.atlasapi.persistence.content.ResolvedContent;
import org.atlasapi.persistence.content.ScheduleResolver;
import org.atlasapi.persistence.lookup.LookupWriter;
import org.atlasapi.persistence.lookup.entry.LookupEntry;
import org.atlasapi.persistence.lookup.entry.LookupEntryStore;
import org.joda.time.DateTime;
import org.joda.time.Interval;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.ArgumentCaptor;
import org.mockito.Captor;
import org.mockito.Mock;
import org.mockito.runners.MockitoJUnitRunner;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Iterables;


@RunWith( MockitoJUnitRunner.class )
public class YouViewEquivalenceBreakerTest {

    private static final Publisher REFERENCE_SCHEDULE_PUBLISHER = Publisher.METABROADCAST;
    private static final Publisher PUBLISHER_TO_ORPHAN = Publisher.YOUVIEW;
    private static final Channel DUMMY_CHANNEL = new Channel(Publisher.METABROADCAST, "Title", "key", true, MediaType.VIDEO, "http://example.org/");
    
    private @Mock ScheduleResolver scheduleResolver;
    private @Mock YouViewChannelResolver youViewChannelResolver;
    private @Mock ContentResolver contentResolver;
    private @Mock LookupEntryStore lookupEntryStore;
    private @Mock LookupWriter lookupWriter;
    private @Captor ArgumentCaptor<Iterable<ContentRef>> equivRefsCaptor;
    private @Captor ArgumentCaptor<Set<Publisher>> publisherCaptor;
    private @Captor ArgumentCaptor<ContentRef> subjectCaptor;
    
    private YouViewEquivalenceBreaker equivalenceBreaker;
    
    @Before
    public void setUp() {
        equivalenceBreaker = new YouViewEquivalenceBreaker(scheduleResolver, 
                youViewChannelResolver, lookupEntryStore, contentResolver, 
                lookupWriter, REFERENCE_SCHEDULE_PUBLISHER, ImmutableSet.of(PUBLISHER_TO_ORPHAN));
    }
    
    @Test
    public void testOrphansTargetEquivs() {
        DateTime from = DateTime.now();
        DateTime to = from.plusDays(1);
        Item item = ComplexItemTestDataBuilder
                        .complexItem()
                        .withUri("http://example.org/a")
                        .withPublisher(Publisher.METABROADCAST)
                        .build();
        
        Item itemToOrphan = ComplexItemTestDataBuilder
                .complexItem()
                .withUri("http://example.org/b")
                .withPublisher(PUBLISHER_TO_ORPHAN)
                .build();
        
        Item itemToNotOrphan = ComplexItemTestDataBuilder
                .complexItem()
                .withUri("http://example.org/c")
                .withPublisher(Publisher.BBC)
                .build();
        
        LookupEntry lookupEntry = LookupEntry.lookupEntryFrom(item);
        lookupEntry = lookupEntry.copyWithEquivalents(ImmutableSet.of(
                LookupRef.from(itemToOrphan),
                LookupRef.from(itemToNotOrphan)));
        
        ScheduleChannel scheduleChannel = new ScheduleChannel(DUMMY_CHANNEL, ImmutableSet.of(item));
        when(youViewChannelResolver.getAllChannels()).thenReturn(ImmutableSet.of(DUMMY_CHANNEL));
        when(scheduleResolver.unmergedSchedule(from, to, ImmutableSet.of(DUMMY_CHANNEL), ImmutableSet.of(REFERENCE_SCHEDULE_PUBLISHER)))
            .thenReturn(new Schedule(ImmutableList.of(scheduleChannel), new Interval(from, to)));
        
        when(lookupEntryStore.entriesForCanonicalUris(ImmutableSet.of(item.getCanonicalUri())))
            .thenReturn(ImmutableSet.of(lookupEntry));
        
        when(contentResolver.findByCanonicalUris(ImmutableSet.of(itemToOrphan.getCanonicalUri())))
            .thenReturn(ResolvedContent.builder().put(itemToOrphan.getCanonicalUri(), itemToOrphan).build());
        
        equivalenceBreaker.orphanItems(from, to);
        
        verify(lookupWriter).writeLookup(subjectCaptor.capture(), 
                equivRefsCaptor.capture(), publisherCaptor.capture());
        
        assertEquals(itemToOrphan.getCanonicalUri(), subjectCaptor.getValue().getCanonicalUri());
        assertEquals(itemToOrphan.getCanonicalUri(), Iterables.getOnlyElement(equivRefsCaptor.getValue()).getCanonicalUri());
        assertEquals(Publisher.all(), publisherCaptor.getValue());
        
    }
    
}

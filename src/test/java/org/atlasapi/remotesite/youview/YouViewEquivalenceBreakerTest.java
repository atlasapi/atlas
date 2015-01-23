package org.atlasapi.remotesite.youview;

import static org.junit.Assert.assertEquals;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.util.Map;
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
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Iterables;
import com.google.common.collect.Maps;
import com.google.common.collect.Sets;


@RunWith( MockitoJUnitRunner.class )
public class YouViewEquivalenceBreakerTest {

    private static final Publisher REFERENCE_SCHEDULE_PUBLISHER = Publisher.METABROADCAST;
    private static final Publisher PUBLISHER_TO_ORPHAN = Publisher.YOUVIEW;
    private static final Channel DUMMY_CHANNEL = new Channel(Publisher.METABROADCAST, "Title", "key", true, MediaType.VIDEO, "http://example.org/");
    
    private @Mock ScheduleResolver scheduleResolver;
    private @Mock YouViewChannelResolver youViewChannelResolver;
    private @Mock ContentResolver contentResolver;
    private @Mock LookupEntryStore lookupEntryStore;
    private @Captor ArgumentCaptor<Iterable<ContentRef>> equivRefsCaptor;
    private @Captor ArgumentCaptor<Set<Publisher>> publisherCaptor;
    private @Captor ArgumentCaptor<ContentRef> subjectCaptor;
    
    private YouViewEquivalenceBreaker equivalenceBreaker;
    
    
    
    @Before
    public void setUp() {
        equivalenceBreaker = new YouViewEquivalenceBreaker(scheduleResolver, 
                youViewChannelResolver, lookupEntryStore, contentResolver, REFERENCE_SCHEDULE_PUBLISHER, 
                ImmutableSet.of(PUBLISHER_TO_ORPHAN));
    }
    
    @Test
    public void testOrphansTargetEquivs() {
        DateTime from = DateTime.now();
        DateTime to = from.plusDays(1);
        
        Item itemToOrphan = ComplexItemTestDataBuilder
                .complexItem()
                .withUri("http://example.org/orphan")
                .withPublisher(PUBLISHER_TO_ORPHAN)
                .build();
        
       Item itemToNotOrphan = ComplexItemTestDataBuilder
                .complexItem()
                .withUri("http://example.org/leave")
                .withPublisher(Publisher.BBC)
                .build();
        
        Item itemInSchedule = ComplexItemTestDataBuilder
                .complexItem()
                .withUri("http://example.org/scheduleitem")
                .withPublisher(Publisher.METABROADCAST)
                .build();

        Set<LookupRef> equivalentSet = ImmutableSet.of(
                LookupRef.from(itemInSchedule),
                LookupRef.from(itemToNotOrphan),
                LookupRef.from(itemToOrphan)
                );
        
        LookupEntry lookupEntryForItemToNotOrphan = 
                LookupEntry.lookupEntryFrom(itemToNotOrphan)
                    .copyWithDirectEquivalents(ImmutableSet.of(LookupRef.from(itemInSchedule)))
                    .copyWithEquivalents(equivalentSet);
        
        LookupEntry lookupEntryForItemInSchedule = 
                LookupEntry.lookupEntryFrom(itemInSchedule)
                    .copyWithDirectEquivalents(ImmutableSet.of(LookupRef.from(itemToNotOrphan), LookupRef.from(itemToOrphan)))
                    .copyWithEquivalents(equivalentSet);
        
        LookupEntry lookupEntryForItemToOrphan = 
                LookupEntry.lookupEntryFrom(itemToOrphan)
                    .copyWithDirectEquivalents(ImmutableSet.of(LookupRef.from(itemInSchedule)))
                    .copyWithEquivalents(equivalentSet);
        
        ScheduleChannel scheduleChannel = new ScheduleChannel(DUMMY_CHANNEL, ImmutableSet.of(itemInSchedule));
        
        when(youViewChannelResolver.getAllChannels()).thenReturn(ImmutableSet.of(DUMMY_CHANNEL));
        when(scheduleResolver.unmergedSchedule(from, to, ImmutableSet.of(DUMMY_CHANNEL), ImmutableSet.of(REFERENCE_SCHEDULE_PUBLISHER)))
            .thenReturn(new Schedule(ImmutableList.of(scheduleChannel), new Interval(from, to)));
        
        when(lookupEntryStore.entriesForCanonicalUris(ImmutableSet.of(itemInSchedule.getCanonicalUri())))
            .thenReturn(ImmutableSet.of(lookupEntryForItemInSchedule));
        
        when(lookupEntryStore.entriesForCanonicalUris(ImmutableSet.of(itemToOrphan.getCanonicalUri())))
            .thenReturn(ImmutableSet.of(lookupEntryForItemToOrphan));
        
        when(lookupEntryStore.entriesForCanonicalUris(ImmutableSet.of(itemToNotOrphan.getCanonicalUri())))
        .thenReturn(ImmutableSet.of(lookupEntryForItemToNotOrphan));
        
        when(contentResolver.findByCanonicalUris(ImmutableSet.of(itemToOrphan.getCanonicalUri())))
            .thenReturn(ResolvedContent.builder().put(itemToOrphan.getCanonicalUri(), itemToOrphan).build());
        
        equivalenceBreaker.orphanItems(from, to);
        
        LookupEntry expectedLookupEntryForItemToNotOrphanAfterDetaching = 
                LookupEntry.lookupEntryFrom(itemToNotOrphan)
                    .copyWithDirectEquivalents(ImmutableSet.of(LookupRef.from(itemInSchedule)))
                    .copyWithEquivalents(ImmutableSet.of(LookupRef.from(itemInSchedule)));
        
        LookupEntry expectedLookupEntryForItemInScheduleAfterDetaching = 
                LookupEntry.lookupEntryFrom(itemInSchedule)
                    .copyWithEquivalents(ImmutableSet.of(LookupRef.from(itemToNotOrphan)))
                    .copyWithDirectEquivalents(ImmutableSet.of(LookupRef.from(itemToNotOrphan)));
        
        LookupEntry expectedLookupEntryForOrphanAfterDetaching = 
                LookupEntry.lookupEntryFrom(itemToOrphan);
        
        Set<LookupEntry> expectedLookupEntrySaves = ImmutableSet.of(expectedLookupEntryForItemToNotOrphanAfterDetaching, 
                expectedLookupEntryForItemInScheduleAfterDetaching, expectedLookupEntryForOrphanAfterDetaching);
        
        ArgumentCaptor<LookupEntry> lookupEntryCaptor = ArgumentCaptor.forClass(LookupEntry.class);
        
        // We can't rely on object equality, since there are other fields, such as timestamps
        // that are taken into account on LookupEntry.equals() which we will allow to differ
        verify(lookupEntryStore, times(3)).store(lookupEntryCaptor.capture());
        
        Map<String, LookupEntry> savedEntries = Maps.uniqueIndex(lookupEntryCaptor.getAllValues(), LookupEntry.TO_ID);
        
        for (LookupEntry expectedLookupSave : expectedLookupEntrySaves) {
            LookupEntry saved = savedEntries.get(expectedLookupSave.uri());
            assertEquals("Problem with " + saved.uri(), expectedLookupSave.aliases(), saved.aliases());
            assertEquals("Problem with " + saved.uri(), expectedLookupSave.directEquivalents(), saved.directEquivalents());
            assertEquals("Problem with " + saved.uri(), expectedLookupSave.explicitEquivalents(), saved.explicitEquivalents());
            assertEquals("Problem with " + saved.uri(), expectedLookupSave.equivalents(), saved.equivalents());
        }
        
        assertEquals(expectedLookupEntrySaves.size(), savedEntries.size());
    }
    
}

package org.atlasapi.equiv;

import static org.atlasapi.media.entity.Publisher.BBC;
import static org.mockito.Matchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import org.atlasapi.equiv.results.persistence.CombinedEquivalenceScore;
import org.atlasapi.equiv.results.persistence.EquivalenceResultStore;
import org.atlasapi.equiv.results.persistence.StoredEquivalenceResult;
import org.atlasapi.equiv.update.EquivalenceUpdater;
import org.atlasapi.media.entity.Brand;
import org.atlasapi.media.entity.Content;
import org.atlasapi.media.entity.Item;
import org.atlasapi.media.entity.Topic;
import org.atlasapi.messaging.v3.EntityUpdatedMessage;
import org.atlasapi.persistence.content.ContentResolver;
import org.atlasapi.persistence.content.ResolvedContent;
import org.atlasapi.persistence.lookup.entry.LookupEntry;
import org.atlasapi.persistence.lookup.entry.LookupEntryStore;
import org.joda.time.DateTime;
import org.junit.Ignore;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.runners.MockitoJUnitRunner;

import com.google.common.base.Predicate;
import com.google.common.base.Predicates;
import com.google.common.collect.HashBasedTable;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Lists;
import com.metabroadcast.common.time.DateTimeZones;
import com.metabroadcast.common.time.Timestamp;

@RunWith(MockitoJUnitRunner.class)
public class EquivalenceUpdatingWorkerTest {

    private final ContentResolver resolver = mock(ContentResolver.class);
    private final LookupEntryStore entryStore = mock(LookupEntryStore.class);
    private final EquivalenceResultStore resultStore
        = mock(EquivalenceResultStore.class); 
    @SuppressWarnings("unchecked")
    private final EquivalenceUpdater<Content> updater = mock(EquivalenceUpdater.class);
    private final Predicate<Object> filter = Predicates.instanceOf(Item.class);
    @SuppressWarnings("unchecked")
    private final EquivalenceUpdatingWorker workerThatOnlyUpdatesItems
        = new EquivalenceUpdatingWorker(resolver, 
                entryStore, resultStore, updater, Predicates.<Content>and(filter));
    
    @Test
    public void testWorkerThatOnlyUpdatesItemsUpdatesAnItem() {
        
        String eid = "cyp";
        Item item = new Item(eid, eid, BBC);
        item.setId(1225L);
        when(entryStore.entriesForIds(ImmutableSet.of(item.getId())))
            .thenReturn(ImmutableList.of(LookupEntry.lookupEntryFrom(item)));
        when(resolver.findByCanonicalUris(ImmutableSet.of(eid)))
            .thenReturn(ResolvedContent.builder().put(eid, item).build());
        
        EntityUpdatedMessage msg = new EntityUpdatedMessage("1", Timestamp.of(1L), eid, "item", "bbc.co.uk");
        workerThatOnlyUpdatesItems.process(msg);
        
        verify(updater).updateEquivalences(item);
    }

    @Test
    public void testWorkerThatOnlyUpdatesItemsDoesntUpdateAnContainer() {
        
        String eid = "cyp";
        Brand brand = new Brand(eid, eid, BBC);
        brand.setId(1225L);
        when(entryStore.entriesForIds(ImmutableSet.of(brand.getId())))
            .thenReturn(ImmutableList.of(LookupEntry.lookupEntryFrom(brand)));
        when(resolver.findByCanonicalUris(ImmutableSet.of(eid)))
            .thenReturn(ResolvedContent.builder().put(eid, brand).build());
        
        EntityUpdatedMessage msg = new EntityUpdatedMessage("1", Timestamp.of(1L), eid, "brand", "bbc.co.uk");
        workerThatOnlyUpdatesItems.process(msg);
        
        verify(updater, never()).updateEquivalences(brand);
    }
    
    @Test
    public void testWorkerThatOnlyUpdatesItemsHandlesResolvingNothing() {
        
        String eid = "cyp";
        Item item = new Item(eid, eid, BBC);
        item.setId(1225L);
        when(entryStore.entriesForIds(ImmutableSet.of(item.getId())))
            .thenReturn(ImmutableList.of(LookupEntry.lookupEntryFrom(item)));
        when(resolver.findByCanonicalUris(ImmutableSet.of(eid)))
            .thenReturn(ResolvedContent.builder().put(eid, null).build());
        
        EntityUpdatedMessage msg = new EntityUpdatedMessage("1", Timestamp.of(1L), eid, "brand", "bbc.co.uk");
        workerThatOnlyUpdatesItems.process(msg);
        
        verify(updater, never()).updateEquivalences(null);
    }

    @Test
    public void testWorkerThatOnlyUpdatesItemsHandlesResolvingNotContent() {
        
        String eid = "cyp";
        Topic topic = new Topic(1225L);
        topic.setCanonicalUri(eid);
        when(entryStore.entriesForIds(ImmutableSet.of(topic.getId())))
            .thenReturn(ImmutableList.of(LookupEntry.lookupEntryFrom(topic)));
        when(resolver.findByCanonicalUris(ImmutableSet.of(eid)))
            .thenReturn(ResolvedContent.builder().put(eid, topic).build());
        
        EntityUpdatedMessage msg = new EntityUpdatedMessage("1", Timestamp.of(1L), eid, "brand", "bbc.co.uk");
        workerThatOnlyUpdatesItems.process(msg);
        
        verify(updater, never()).updateEquivalences(any(Content.class));
    }
    
    @Test
    public void testWorkerThatOnlyUpdatesItemsHandlesResolvingById() {
        
        String eid = "cyp";
        Item item = new Item(eid, eid, BBC);
        item.setId(1225L);
        when(entryStore.entriesForIds(ImmutableSet.of(item.getId())))
            .thenReturn(ImmutableList.of(LookupEntry.lookupEntryFrom(item)));
        when(resolver.findByCanonicalUris(ImmutableSet.of(eid)))
            .thenReturn(ResolvedContent.builder().put(eid, item).build());
        
        EntityUpdatedMessage msg = new EntityUpdatedMessage("1", Timestamp.of(1L), eid, "item", "bbc.co.uk");
        workerThatOnlyUpdatesItems.process(msg);
        
        verify(updater).updateEquivalences(item);
        
    }
    
    @Test
    @Ignore
    // Changed functionality to always recompute equivalences. We're trialling for now
    // so will leave this test but ignored until we decide it's a goer
    public void testWorkerDoesntUpdateItemWithAnEquivalenceResult() {
        
        String eid = "cyp";
        Item item = new Item(eid, eid, BBC);
        item.setId(1225L);
        when(entryStore.entriesForIds(ImmutableSet.of(item.getId())))
            .thenReturn(ImmutableList.of(LookupEntry.lookupEntryFrom(item)));
        when(resolver.findByCanonicalUris(ImmutableSet.of(eid)))
            .thenReturn(ResolvedContent.builder().put(eid, item).build());
        when(resultStore.forId(eid))
            .thenReturn(new StoredEquivalenceResult(eid, "title", 
                    HashBasedTable.<String, String, Double>create(), 
                    Lists.<CombinedEquivalenceScore>newArrayList(), 
                    new DateTime(DateTimeZones.UTC), Lists.newArrayList()));
        
        EntityUpdatedMessage msg = new EntityUpdatedMessage("1", Timestamp.of(1L), eid, "item", "bbc.co.uk");
        workerThatOnlyUpdatesItems.process(msg);
        
        verify(updater, never()).updateEquivalences(any(Content.class));
        
    }
    
}

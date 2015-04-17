package org.atlasapi.equiv;

import static org.hamcrest.Matchers.hasItem;
import static org.hamcrest.Matchers.is;
import static org.mockito.Matchers.argThat;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import org.atlasapi.media.entity.Alias;
import org.atlasapi.media.entity.Described;
import org.atlasapi.media.entity.LookupRef;
import org.atlasapi.media.entity.Publisher;
import org.atlasapi.media.entity.testing.ComplexItemTestDataBuilder;
import org.atlasapi.persistence.content.ContentResolver;
import org.atlasapi.persistence.content.ResolvedContent;
import org.atlasapi.persistence.lookup.LookupWriter;
import org.atlasapi.persistence.lookup.entry.LookupEntry;
import org.atlasapi.persistence.lookup.entry.LookupEntryStore;
import org.joda.time.DateTime;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.runners.MockitoJUnitRunner;

import com.google.common.collect.ImmutableSet;

@RunWith(MockitoJUnitRunner.class)
public class EquivalenceBreakerTest {

    private static final String REMOVE_FROM_URI = "http://example.org/item/1";
    private static final String ITEM_TO_REMOVE_URI = "http://bad.apple.org/item/1";
    private static final String ITEM_TO_KEEP_URI = "http://good.apple.org/item/1";
    
    private final Described EXAMPLE_ITEM = ComplexItemTestDataBuilder
                                                .complexItem()
                                                .withUri(REMOVE_FROM_URI)
                                                .build();
    
    private final Described ITEM_TO_REMOVE = ComplexItemTestDataBuilder
                                                .complexItem()
                                                .withUri(ITEM_TO_REMOVE_URI)
                                                .build();
    
    private final Described ITEM_TO_KEEP = ComplexItemTestDataBuilder
                                                .complexItem()
                                                .withUri(ITEM_TO_KEEP_URI)
                                                .build();
    
    private final LookupEntry EXAMPLE_ITEM_LOOKUP = new LookupEntry(REMOVE_FROM_URI, 1L,  
            LookupRef.from(EXAMPLE_ITEM), ImmutableSet.<String>of(), ImmutableSet.<Alias>of(), 
            ImmutableSet.of(LookupRef.from(ITEM_TO_REMOVE), LookupRef.from(ITEM_TO_KEEP)), 
            ImmutableSet.<LookupRef>of(), 
            ImmutableSet.of(LookupRef.from(ITEM_TO_REMOVE), LookupRef.from(ITEM_TO_KEEP)), 
            new DateTime(), new DateTime(), true);
    
    private final ContentResolver contentResolver = mock(ContentResolver.class);
    private final LookupWriter lookupWriter = mock(LookupWriter.class);
    private final LookupEntryStore lookupEntryStore = mock(LookupEntryStore.class);
    
    private final EquivalenceBreaker equivalenceBreaker = new EquivalenceBreaker(contentResolver, lookupEntryStore, lookupWriter);
    
    @Before
    public void setUp() {
        EXAMPLE_ITEM.setEquivalentTo(ImmutableSet.of(LookupRef.from(ITEM_TO_REMOVE), LookupRef.from(ITEM_TO_KEEP)));
    }
    
    @Test
    public void testRemovesItemFromEquivalentSet() {
        when(lookupEntryStore.entriesForCanonicalUris(argThat(hasItem(REMOVE_FROM_URI))))
            .thenReturn(ImmutableSet.of(EXAMPLE_ITEM_LOOKUP));
        
        when(contentResolver.findByCanonicalUris(argThat(hasItem(REMOVE_FROM_URI))))
            .thenReturn(ResolvedContent.builder().put(REMOVE_FROM_URI, EXAMPLE_ITEM).build());
        
        when(contentResolver.findByCanonicalUris(argThat(hasItem(ITEM_TO_KEEP_URI))))
            .thenReturn(ResolvedContent.builder()   
                                       .put(ITEM_TO_KEEP_URI, ITEM_TO_KEEP)
                                       .build());
        
        equivalenceBreaker.removeFromSet(REMOVE_FROM_URI, ITEM_TO_REMOVE_URI);
        
        verify(lookupWriter).writeLookup(argThat(is(ContentRef.valueOf(EXAMPLE_ITEM))), 
                argThat(hasItem(ContentRef.valueOf(ITEM_TO_KEEP))), argThat(is(Publisher.all())));
    }
}

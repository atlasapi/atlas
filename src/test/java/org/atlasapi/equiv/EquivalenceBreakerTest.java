package org.atlasapi.equiv;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import com.metabroadcast.common.stream.MoreCollectors;
import org.atlasapi.media.entity.Alias;
import org.atlasapi.media.entity.Described;
import org.atlasapi.media.entity.LookupRef;
import org.atlasapi.media.entity.Publisher;
import org.atlasapi.media.entity.testing.ComplexItemTestDataBuilder;
import org.atlasapi.persistence.content.ContentResolver;
import org.atlasapi.persistence.content.ResolvedContent;
import org.atlasapi.persistence.lookup.LookupWriter;
import org.atlasapi.persistence.lookup.entry.EquivRefs;
import org.atlasapi.persistence.lookup.entry.LookupEntry;
import org.atlasapi.persistence.lookup.entry.LookupEntryStore;
import org.joda.time.DateTime;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.runners.MockitoJUnitRunner;

import static org.atlasapi.persistence.lookup.entry.EquivRefs.EquivDirection.OUTGOING;
import static org.hamcrest.Matchers.hasItem;
import static org.hamcrest.Matchers.is;
import static org.mockito.Matchers.argThat;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

@RunWith(MockitoJUnitRunner.class)
public class EquivalenceBreakerTest {

    private static final String REMOVE_FROM_URI = "http://example.org/item/1";
    private static final String DIRECT_EQUIV_TO_REMOVE_URI = "http://bad.apple.org/item/1";
    private static final String EXPLICIT_EQUIV_TO_REMOVE_URI = "http://bad.apple.org/item/2";
    private static final String ITEM_TO_KEEP_URI = "http://good.apple.org/item/1";

    private final Described EXAMPLE_ITEM = ComplexItemTestDataBuilder
            .complexItem()
            .withUri(REMOVE_FROM_URI)
            .build();

    //directly equiv'd item that we want to remove
    private final Described DIRECT_EQUIV_TO_REMOVE = ComplexItemTestDataBuilder
            .complexItem()
            .withUri(DIRECT_EQUIV_TO_REMOVE_URI)
            .build();

    //explicitly equiv'd item that we want to remove
    private final Described EXPLICIT_EQUIV_TO_REMOVE = ComplexItemTestDataBuilder
            .complexItem()
            .withUri(EXPLICIT_EQUIV_TO_REMOVE_URI)
            .build();

    private final Described ITEM_TO_KEEP = ComplexItemTestDataBuilder
                                                .complexItem()
                                                .withUri(ITEM_TO_KEEP_URI)
                                                .build();
    
    private final LookupEntry EXAMPLE_ITEM_LOOKUP = new LookupEntry(REMOVE_FROM_URI, 1L,
            LookupRef.from(EXAMPLE_ITEM), ImmutableSet.<String>of(), ImmutableSet.<Alias>of(),
            EquivRefs.of(
                    ImmutableMap.of(
                            LookupRef.from(DIRECT_EQUIV_TO_REMOVE), OUTGOING,
                            LookupRef.from(ITEM_TO_KEEP), OUTGOING
                    )
            ),
            EquivRefs.of(
                    ImmutableMap.of(
                            LookupRef.from(EXPLICIT_EQUIV_TO_REMOVE), OUTGOING,
                            LookupRef.from(ITEM_TO_KEEP), OUTGOING
                    )
            ),
            EquivRefs.of(),
            ImmutableSet.of(LookupRef.from(DIRECT_EQUIV_TO_REMOVE), LookupRef.from(EXPLICIT_EQUIV_TO_REMOVE), LookupRef.from(ITEM_TO_KEEP)),
            new DateTime(), new DateTime(), new DateTime(), true);

    private final ContentResolver contentResolver = mock(ContentResolver.class);
    private final LookupWriter lookupWriter = mock(LookupWriter.class);
    private final LookupWriter explicitLookupWriter = mock(LookupWriter.class);
    private final LookupEntryStore lookupEntryStore = mock(LookupEntryStore.class);
    
    private final EquivalenceBreaker equivalenceBreaker = EquivalenceBreaker.create(
            contentResolver,
            lookupEntryStore,
            lookupWriter,
            explicitLookupWriter
    );
    
    @Before
    public void setUp() {
        EXAMPLE_ITEM.setEquivalentTo(ImmutableSet.of(
                LookupRef.from(EXPLICIT_EQUIV_TO_REMOVE),
                LookupRef.from(ITEM_TO_KEEP)
        ));

        when(lookupEntryStore.entriesForCanonicalUris(argThat(hasItem(REMOVE_FROM_URI))))
                .thenReturn(ImmutableSet.of(EXAMPLE_ITEM_LOOKUP));

        when(contentResolver.findByCanonicalUris(argThat(hasItem(REMOVE_FROM_URI))))
                .thenReturn(ResolvedContent.builder().put(REMOVE_FROM_URI, EXAMPLE_ITEM).build());

        when(contentResolver.findByCanonicalUris(argThat(hasItem(ITEM_TO_KEEP_URI))))
                .thenReturn(ResolvedContent.builder()
                        .put(ITEM_TO_KEEP_URI, ITEM_TO_KEEP)
                        .build());
    }
    
    @Test
    public void testRemovesItemFromEquivalentSet() {
        equivalenceBreaker.removeFromSet(REMOVE_FROM_URI, DIRECT_EQUIV_TO_REMOVE_URI);
        
        verify(lookupWriter).writeLookup(argThat(is(ContentRef.valueOf(EXAMPLE_ITEM))), 
                argThat(hasItem(ContentRef.valueOf(ITEM_TO_KEEP))), argThat(is(Publisher.all())));
    }


    @Test
    public void testRemovesMultipleItemsFromEquivalentSet() {
        ImmutableSet<String> directEquivUris = EXAMPLE_ITEM_LOOKUP.directEquivalents()
                .stream()
                .map(LookupRef::uri)
                .collect(MoreCollectors.toImmutableSet());
        equivalenceBreaker.removeDirectEquivs(EXAMPLE_ITEM, EXAMPLE_ITEM_LOOKUP, directEquivUris);

        verify(lookupWriter).writeLookup(argThat(is(ContentRef.valueOf(EXAMPLE_ITEM))),
                argThat(is(ImmutableList.of())), argThat(is(Publisher.all())));
    }

    @Test
    public void testRemovesExplicitEquivsFromEquivalentSet() {
        ImmutableSet<String> explicitEquivUris = EXAMPLE_ITEM_LOOKUP.explicitEquivalents()
                .stream()
                .map(LookupRef::uri)
                .collect(MoreCollectors.toImmutableSet());
        equivalenceBreaker.removeExplicitEquivs(EXAMPLE_ITEM, EXAMPLE_ITEM_LOOKUP, explicitEquivUris);

        verify(explicitLookupWriter).writeLookup(argThat(is(ContentRef.valueOf(EXAMPLE_ITEM))),
                argThat(is(ImmutableList.of())), argThat(is(Publisher.all())));
    }

    @Test
    public void testRemovesAllEquivsFromEquivalentSet() {
        ImmutableSet<String> directEquivUris = EXAMPLE_ITEM_LOOKUP.directEquivalents()
                .stream()
                .map(LookupRef::uri)
                .collect(MoreCollectors.toImmutableSet());
        ImmutableSet<String> explicitEquivUris = EXAMPLE_ITEM_LOOKUP.explicitEquivalents()
                .stream()
                .map(LookupRef::uri)
                .collect(MoreCollectors.toImmutableSet());
        equivalenceBreaker.removeFromSet(EXAMPLE_ITEM, EXAMPLE_ITEM_LOOKUP, directEquivUris, explicitEquivUris);

        verify(lookupWriter).writeLookup(argThat(is(ContentRef.valueOf(EXAMPLE_ITEM))),
                argThat(is(ImmutableList.of())), argThat(is(Publisher.all())));
        verify(explicitLookupWriter).writeLookup(argThat(is(ContentRef.valueOf(EXAMPLE_ITEM))),
                argThat(is(ImmutableList.of())), argThat(is(Publisher.all())));
    }

}

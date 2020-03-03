package org.atlasapi.remotesite.pa.deletes;

import com.google.common.collect.ImmutableList;
import org.atlasapi.equiv.EquivalenceBreaker;
import org.atlasapi.media.entity.Container;
import org.atlasapi.media.entity.Item;
import org.atlasapi.media.entity.LookupRef;
import org.atlasapi.persistence.content.ContentResolver;
import org.atlasapi.persistence.content.ContentWriter;
import org.atlasapi.persistence.content.ResolvedContent;
import org.atlasapi.persistence.lookup.entry.EquivRefs;
import org.atlasapi.persistence.lookup.entry.LookupEntry;
import org.atlasapi.persistence.lookup.entry.LookupEntryStore;
import org.joda.time.DateTime;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.ArgumentCaptor;
import org.mockito.Captor;
import org.mockito.Mock;
import org.mockito.runners.MockitoJUnitRunner;

import static org.atlasapi.persistence.lookup.entry.EquivRefs.EquivDirection.OUTGOING;
import static org.hamcrest.core.Is.is;
import static org.junit.Assert.assertThat;
import static org.mockito.Matchers.any;
import static org.mockito.Matchers.anyString;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

@RunWith(MockitoJUnitRunner.class)
public class ExistingItemUnPublisherTest {

    @Captor private ArgumentCaptor<Item> itemCaptor;

    @Mock private ContentResolver contentResolver;
    @Mock private ContentWriter contentWriter;
    @Mock private LookupEntryStore lookupEntryStore;
    @Mock private EquivalenceBreaker equivalenceBreaker;

    private ExistingItemUnPublisher unPublisher;

    private Item item;
    private Container container;
    private Item equivItem;

    @Before
    public void setUp() throws Exception {
        unPublisher = ExistingItemUnPublisher.create(
                contentResolver,
                contentWriter,
                lookupEntryStore,
                equivalenceBreaker,
                false
        );

        item = new Item();
        item.setCanonicalUri("uri");
        item.setActivelyPublished(true);
        item.setThisOrChildLastUpdated(DateTime.now());

        container = new Container();
        container.setCanonicalUri("container");

        equivItem = new Item();
        equivItem.setCanonicalUri("equivItem");
        equivItem.setActivelyPublished(true);
        equivItem.setThisOrChildLastUpdated(DateTime.now());
    }

    @Test
    public void unPublishMissingContentDoesNothing() throws Exception {
        setupMocks(
                item.getCanonicalUri(), ResolvedContent.builder().build(),
                ImmutableList.of()
        );

        unPublisher.unPublishItems(item.getCanonicalUri());

        verify(contentWriter, never()).createOrUpdate(any(Item.class));
        verify(contentWriter, never()).createOrUpdate(any(Container.class));
        verify(equivalenceBreaker, never()).removeFromSet(anyString(), anyString());
    }

    @Test
    public void unPublishItemMarksItActivelyPublishedFalse() throws Exception {
        setupMocks(
                item.getCanonicalUri(), ResolvedContent.builder()
                        .put(item.getCanonicalUri(), item)
                        .build(),
                ImmutableList.of(
                        LookupEntry.lookupEntryFrom(item)
                )
        );

        unPublisher.unPublishItems(item.getCanonicalUri());

        verify(contentWriter).createOrUpdate(itemCaptor.capture());

        assertThat(itemCaptor.getValue().isActivelyPublished(), is(false));
    }

    @Test
    public void unPublishContainerDoesNothing() throws Exception {
        setupMocks(
                container.getCanonicalUri(),
                ResolvedContent.builder()
                        .put(container.getCanonicalUri(), container)
                        .build(),
                ImmutableList.of(
                        LookupEntry.lookupEntryFrom(container)
                )
        );

        unPublisher.unPublishItems(container.getCanonicalUri());

        verify(contentWriter, never()).createOrUpdate(any(Container.class));
        verify(equivalenceBreaker, never()).removeFromSet(anyString(), anyString());
    }

    @Test
    public void unPublisherItemBreaksEquivalence() throws Exception {
        setupMocks(
                item.getCanonicalUri(),
                ResolvedContent.builder()
                        .put(item.getCanonicalUri(), item)
                        .build(),
                ImmutableList.of(
                        LookupEntry.lookupEntryFrom(item)
                                .copyWithDirectEquivalents(EquivRefs.of(
                                        LookupRef.from(equivItem), OUTGOING
                                ))
                )
        );

        unPublisher.unPublishItems(item.getCanonicalUri());


        verify(equivalenceBreaker).removeFromSet(
                item.getCanonicalUri(),
                equivItem.getCanonicalUri()
        );
    }

    private void setupMocks(
            String canonicalUri,
            ResolvedContent resolvedContent,
            ImmutableList<LookupEntry> lookupEntries
    ) {
        when(contentResolver.findByCanonicalUris(ImmutableList.of(canonicalUri)))
                .thenReturn(resolvedContent);
        when(lookupEntryStore.entriesForCanonicalUris(ImmutableList.of(canonicalUri)))
                .thenReturn(lookupEntries);
    }
}

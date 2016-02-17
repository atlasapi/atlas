package org.atlasapi.remotesite.util;

import static org.hamcrest.Matchers.is;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertThat;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoMoreInteractions;
import static org.mockito.Mockito.when;

import java.util.Set;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.Sets;
import org.atlasapi.media.entity.Content;
import org.atlasapi.media.entity.Identified;
import org.atlasapi.media.entity.Item;
import org.atlasapi.media.entity.Publisher;
import org.atlasapi.media.entity.testing.ComplexItemTestDataBuilder;
import org.atlasapi.media.entity.testing.ItemTestDataBuilder;
import org.atlasapi.persistence.content.ContentCategory;
import org.atlasapi.persistence.content.ContentResolver;
import org.atlasapi.persistence.content.ContentWriter;
import org.atlasapi.persistence.content.ResolvedContent;
import org.atlasapi.persistence.content.listing.ContentLister;
import org.atlasapi.persistence.content.listing.ContentListingCriteria;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.ArgumentCaptor;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.runners.MockitoJUnitRunner;

import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Iterables;


@RunWith(MockitoJUnitRunner.class)
public class OldContentDeactivatorTest {

    private static final Publisher PUBLISHER = Publisher.METABROADCAST;
    private final ContentListingCriteria expectedCriteria =
            new ContentListingCriteria
                    .Builder()
                    .forPublisher(PUBLISHER)
                    .forContent(ImmutableList.copyOf(Sets.union(ContentCategory.CONTAINERS, ContentCategory.ITEMS)))
                    .build();

    private final Content item1 = ComplexItemTestDataBuilder
            .complexItem().withUri("http://example.org/1").build();
    private final Content item2 = ComplexItemTestDataBuilder
            .complexItem().withUri("http://example.org/2").build();
    private final Content item3 = ComplexItemTestDataBuilder
            .complexItem().withUri("http://example.org/3").build();
    private final Content item4 = ComplexItemTestDataBuilder
            .complexItem().withUri("http://example.org/4").build();

    private final Set<Content> allContent = ImmutableSet.of(item1, item2, item3, item4);

    @Mock
    private ContentLister contentLister;
    @Mock
    private ContentWriter contentWriter;
    @Mock
    private ContentResolver contentResolver;
    @InjectMocks
    private OldContentDeactivator oldContentDeactivator;

    @Test
    public void testRemovesContentIfNoThreshold() {

        when(contentLister.listContent(expectedCriteria))
                .thenReturn(allContent.iterator());
        setUpResolverForContent(item1);

        oldContentDeactivator.deactivateOldContent(PUBLISHER,
                ImmutableSet.of(item2.getCanonicalUri(), item3.getCanonicalUri(), item4.getCanonicalUri()), null);

        ArgumentCaptor<Item> itemCaptor = ArgumentCaptor.forClass(Item.class);

        verify(contentWriter).createOrUpdate(itemCaptor.capture());

        assertFalse(itemCaptor.getValue().isActivelyPublished());
        assertThat(itemCaptor.getValue().getCanonicalUri(), is(item1.getCanonicalUri()));
    }

    @Test
    public void testDoesntRemoveContentIfThresholdExceeded() {
        when(contentLister.listContent(expectedCriteria))
                .thenReturn(allContent.iterator());

        oldContentDeactivator.deactivateOldContent(PUBLISHER,
                ImmutableSet.of(item2.getCanonicalUri()), 50);

        verifyNoMoreInteractions(contentWriter);
    }

    @Test
    public void testRemovesContentIfThresholdSpecifiedAndNotExceeded() {
        when(contentLister.listContent(expectedCriteria))
                .thenReturn(allContent.iterator());

        setUpResolverForContent(item1);

        oldContentDeactivator.deactivateOldContent(PUBLISHER,
                ImmutableSet.of(item2.getCanonicalUri(), item3.getCanonicalUri(), item4.getCanonicalUri()), 70);

        ArgumentCaptor<Item> itemCaptor = ArgumentCaptor.forClass(Item.class);

        verify(contentWriter).createOrUpdate(itemCaptor.capture());

        assertFalse(itemCaptor.getValue().isActivelyPublished());
        assertThat(itemCaptor.getValue().getCanonicalUri(), is(item1.getCanonicalUri()));
    }

    private void setUpResolverForContent(Content c) {
        when(contentResolver.findByCanonicalUris(ImmutableSet.of(item1.getCanonicalUri())))
                .thenReturn(new ResolvedContent
                        .ResolvedContentBuilder()
                        .put(c.getCanonicalUri(), c).build());
    }
}

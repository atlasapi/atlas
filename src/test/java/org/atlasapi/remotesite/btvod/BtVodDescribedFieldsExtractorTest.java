package org.atlasapi.remotesite.btvod;

import static org.hamcrest.core.Is.is;
import static org.junit.Assert.assertThat;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.util.Set;

import org.atlasapi.media.entity.Item;
import org.atlasapi.media.entity.Publisher;
import org.atlasapi.media.entity.Topic;
import org.atlasapi.media.entity.TopicRef;
import org.atlasapi.persistence.topic.TopicCreatingTopicResolver;
import org.atlasapi.persistence.topic.TopicWriter;
import org.atlasapi.remotesite.btvod.contentgroups.BtVodContentMatchingPredicates;
import org.atlasapi.remotesite.btvod.model.BtVodEntry;
import org.atlasapi.remotesite.btvod.model.BtVodPlproduct$productTag;
import org.atlasapi.remotesite.btvod.model.BtVodProductMetadata;
import org.atlasapi.remotesite.btvod.model.BtVodProductScope;
import org.hamcrest.core.Is;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.runners.MockitoJUnitRunner;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Iterables;
import com.metabroadcast.common.base.Maybe;

@RunWith(MockitoJUnitRunner.class)
public class BtVodDescribedFieldsExtractorTest {

    private static final String BT_VOD_GUID_NAMESPACE = "guid namespace";
    private static final String BT_VOD_ID_NAMESPACE = "id namespace";
    private static final String BT_VOD_CONTENT_PROVIDER_NAMESPACE = "content provider namespace";
    private static final String BT_VOD_GENRE_NAMESPACE = "genre namespace";
    private static final String BT_VOD_KEYWORD_NAMESPACE = "keyword namespace";

    private Publisher publisher = Publisher.BT_TVE_VOD;

    @Mock
    private ImageExtractor imageExtractor;

    @Mock
    private TopicCreatingTopicResolver topicResolver;

    @Mock
    private TopicWriter topicWriter;

    private BtVodDescribedFieldsExtractor objectUnderTest;

    @Mock
    private BtVodContentMatchingPredicate newTopicContentMatchingPredicate;
    
    @Before
    public void setUp() {
        objectUnderTest = new BtVodDescribedFieldsExtractor(
                topicResolver,
                topicWriter,
                publisher,
                newTopicContentMatchingPredicate,
                BtVodContentMatchingPredicates.schedulerChannelPredicate("Kids"),
                BtVodContentMatchingPredicates.schedulerChannelPredicate("TV"),
                BtVodContentMatchingPredicates.schedulerChannelPredicate("TV Replay"),
                new Topic(123L),
                new Topic(234L),
                new Topic(345L),
                new Topic(456L),
                BT_VOD_GUID_NAMESPACE,
                BT_VOD_ID_NAMESPACE,
                BT_VOD_CONTENT_PROVIDER_NAMESPACE,
                BT_VOD_GENRE_NAMESPACE,
                BT_VOD_KEYWORD_NAMESPACE
        );
    }

    @Test
    public void testBtGenresFrom() {
        BtVodEntry entry = new BtVodEntry();
        Item item = new Item();
        BtVodPlproduct$productTag genreProductTag = new BtVodPlproduct$productTag();
        genreProductTag.setPlproduct$scheme("genre");
        genreProductTag.setPlproduct$title("Genre");


        BtVodProductScope scope = new BtVodProductScope();
        BtVodProductMetadata genre1MetaData = new BtVodProductMetadata();
        genre1MetaData.setSubGenres("[\"SubGenre1\",\"SubGenre2\",\"SubGenre3\"]");
        scope.setProductMetadata(genre1MetaData);
        entry.setProductScopes(ImmutableList.of(scope));

        entry.setProductTags(ImmutableList.of(genreProductTag));

        Topic genre = mock(Topic.class);
        when(genre.getId()).thenReturn(1L);

        Topic subGenre1 = mock(Topic.class);
        when(subGenre1.getId()).thenReturn(2L);

        Topic subGenre2 = mock(Topic.class);
        when(subGenre2.getId()).thenReturn(3L);

        Topic subGenre3 = mock(Topic.class);
        when(subGenre3.getId()).thenReturn(4L);

        when(topicResolver.topicFor(publisher, BT_VOD_GENRE_NAMESPACE, "Genre"))
                .thenReturn(Maybe.just(genre));

        when(topicResolver.topicFor(publisher, BT_VOD_GENRE_NAMESPACE, "SubGenre1"))
                .thenReturn(Maybe.just(subGenre1));
        when(topicResolver.topicFor(publisher, BT_VOD_GENRE_NAMESPACE, "SubGenre2"))
                .thenReturn(Maybe.just(subGenre2));
        when(topicResolver.topicFor(publisher, BT_VOD_GENRE_NAMESPACE, "SubGenre3"))
                .thenReturn(Maybe.just(subGenre3));

        VodEntryAndContent entryAndContent = new VodEntryAndContent(entry, item);
        Set<TopicRef> result = objectUnderTest.topicsFrom(entryAndContent);

        verify(genre).setTitle("Genre");
        verify(subGenre1).setTitle("SubGenre1");
        verify(subGenre2).setTitle("SubGenre2");
        verify(subGenre3).setTitle("SubGenre3");

        assertThat(
                result,
                Is.<Set<TopicRef>>is(
                        ImmutableSet.of(
                                new TopicRef(1L, 1.0f, false, TopicRef.Relationship.ABOUT),
                                new TopicRef(2L, 1.0f, false, TopicRef.Relationship.ABOUT),
                                new TopicRef(3L, 1.0f, false, TopicRef.Relationship.ABOUT),
                                new TopicRef(4L, 1.0f, false, TopicRef.Relationship.ABOUT)
                        )
                )
        );

    }


    @Test
    public void testTopicFor() throws Exception {
        BtVodEntry entry = new BtVodEntry();
        entry.setProductScopes(ImmutableList.<BtVodProductScope>of());
        Item item = new Item();

        BtVodPlproduct$productTag productTag = new BtVodPlproduct$productTag();
        productTag.setPlproduct$scheme("contentProvider");
        productTag.setPlproduct$title("contentProviderTitle");

        entry.setProductTags(
                ImmutableList.of(
                        productTag
                )
        );

        Topic created = mock(Topic.class);
        when(created.getId()).thenReturn(42L);
        when(topicResolver.topicFor(publisher, BT_VOD_CONTENT_PROVIDER_NAMESPACE, "contentProviderTitle"))
                .thenReturn(Maybe.just(created));

        VodEntryAndContent entryAndContent = new VodEntryAndContent(entry, item);
        Set<TopicRef> topicRef = objectUnderTest.topicsFrom(entryAndContent);

        verify(created).setTitle("contentProviderTitle");
        assertThat(Iterables.getOnlyElement(topicRef).getTopic(), is(42L));
    }

    @Test
    public void testExtractTopicForKeywords() throws Exception {
        BtVodEntry entry = new BtVodEntry();
        entry.setProductScopes(ImmutableList.<BtVodProductScope>of());
        Item item = new Item();

        BtVodPlproduct$productTag productTag = new BtVodPlproduct$productTag();
        productTag.setPlproduct$scheme("keyword");
        productTag.setPlproduct$title("value");

        entry.setProductTags(ImmutableList.of(productTag));

        Topic created = mock(Topic.class);
        when(created.getId()).thenReturn(42L);
        when(topicResolver.topicFor(publisher, BT_VOD_KEYWORD_NAMESPACE, "value"))
                .thenReturn(Maybe.just(created));

        VodEntryAndContent entryAndContent = new VodEntryAndContent(entry, item);
        Set<TopicRef> topicRef = objectUnderTest.topicsFrom(entryAndContent);

        verify(created).setTitle("value");
        assertThat(Iterables.getOnlyElement(topicRef).getTopic(), is(42L));
    }
}
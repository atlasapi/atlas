package org.atlasapi.remotesite.btvod;

import com.google.common.base.Optional;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;
import com.metabroadcast.common.base.Maybe;
import org.atlasapi.media.entity.Publisher;
import org.atlasapi.media.entity.Topic;
import org.atlasapi.media.entity.TopicRef;
import org.atlasapi.persistence.topic.TopicCreatingTopicResolver;
import org.atlasapi.persistence.topic.TopicWriter;
import org.atlasapi.query.v2.TopicWriteController;
import org.atlasapi.remotesite.btvod.model.BtVodEntry;
import org.atlasapi.remotesite.btvod.model.BtVodPlproduct$productTag;
import org.atlasapi.remotesite.btvod.model.BtVodProductMetadata;
import org.atlasapi.remotesite.btvod.model.BtVodProductScope;
import org.hamcrest.core.Is;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.runners.MockitoJUnitRunner;

import java.util.Set;

import static org.hamcrest.core.Is.is;
import static org.junit.Assert.*;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

@RunWith(MockitoJUnitRunner.class)
public class BtVodDescribedFieldsExtractorTest {

    @Mock
    private ImageExtractor imageExtractor;

    @Mock
    private TopicCreatingTopicResolver topicResolver;

    @Mock
    private TopicWriter topicWriter;

    @InjectMocks
    private BtVodDescribedFieldsExtractor objectUnderTest;

    @Test
    public void testBtGenresFrom() {
        BtVodEntry entry = new BtVodEntry();
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

        when(topicResolver.topicFor(Publisher.BT_VOD, "gb:bt:tv:mpx:prod:genre", "Genre"))
                .thenReturn(Maybe.just(genre));

        when(topicResolver.topicFor(Publisher.BT_VOD, "gb:bt:tv:mpx:prod:genre", "SubGenre1"))
                .thenReturn(Maybe.just(subGenre1));
        when(topicResolver.topicFor(Publisher.BT_VOD, "gb:bt:tv:mpx:prod:genre", "SubGenre2"))
                .thenReturn(Maybe.just(subGenre2));
        when(topicResolver.topicFor(Publisher.BT_VOD, "gb:bt:tv:mpx:prod:genre", "SubGenre3"))
                .thenReturn(Maybe.just(subGenre3));

        Set<TopicRef> result = objectUnderTest.btGenresFrom(entry);

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
        when(topicResolver.topicFor(Publisher.BT_VOD, "gb:bt:tv:mpx:prod:contentProvider", "contentProviderTitle"))
                .thenReturn(Maybe.just(created));

        Optional<TopicRef> topicRef = objectUnderTest.topicFor(entry);

        verify(created).setTitle("contentProviderTitle");
        assertThat(topicRef.get().getTopic(), is(42L));
    }
}
package org.atlasapi.remotesite.btvod;

import com.google.common.base.Optional;
import com.google.common.collect.ImmutableList;
import com.metabroadcast.common.base.Maybe;
import org.atlasapi.media.entity.Publisher;
import org.atlasapi.media.entity.Topic;
import org.atlasapi.media.entity.TopicRef;
import org.atlasapi.persistence.topic.TopicCreatingTopicResolver;
import org.atlasapi.persistence.topic.TopicWriter;
import org.atlasapi.query.v2.TopicWriteController;
import org.atlasapi.remotesite.btvod.model.BtVodEntry;
import org.atlasapi.remotesite.btvod.model.BtVodPlproduct$productTag;
import org.atlasapi.remotesite.btvod.model.BtVodProductScope;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.runners.MockitoJUnitRunner;

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

        verify(topicWriter).write(created);
        verify(created).setTitle("contentProviderTitle");
        assertThat(topicRef.get().getTopic(), is(42L));
    }
}
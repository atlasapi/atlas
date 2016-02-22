package org.atlasapi.remotesite.pa;

import java.util.Set;

import org.atlasapi.media.entity.Publisher;
import org.atlasapi.media.entity.Topic;
import org.atlasapi.persistence.ids.MongoSequentialIdGenerator;
import org.atlasapi.persistence.topic.TopicStore;

import com.metabroadcast.common.base.Maybe;

import com.google.api.client.util.Sets;
import org.junit.Before;
import org.junit.Test;

import static org.junit.Assert.assertEquals;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class PaTagMapTest {

    private final String PA_NAMESPACE = "gb:pressassociation:prod:";
    private final String METABROADCAST_TAG = "http://metabroadcast.com/tags/";
    private TopicStore topicStore = mock(TopicStore.class);
    private MongoSequentialIdGenerator idGenerator = mock(MongoSequentialIdGenerator.class);
    private PaTagMap paTagMap;

    @Before
    public void setUp() throws Exception {
        this.paTagMap = new PaTagMap(topicStore, idGenerator);
    }

    @Test
    public void testPaTagMappingForKnownTopic() {
        Set<String> genres = Sets.newHashSet();
        genres.add("http://pressassociation.com/genres/1F03");
        String tag = "horror";

        when(topicStore.topicFor(Publisher.PA, PA_NAMESPACE + tag, METABROADCAST_TAG + tag))
                .thenReturn(Maybe.fromPossibleNullValue(new Topic(20l, PA_NAMESPACE, METABROADCAST_TAG + tag)));

        assertEquals(paTagMap.mapGenresToTopicRefs(genres).size(), 1);
        assertEquals(paTagMap.mapGenresToTopicRefs(genres).iterator().next().getTopic().longValue(), 20l);
    }

    @Test
    public void testPaTagMappingForUnknownTopic() {
        Set<String> genres = Sets.newHashSet();
        genres.add("http://pressassociation.com/genres/1F03");
        String tag = "horror";

        when(topicStore.topicFor(Publisher.PA, PA_NAMESPACE + tag, METABROADCAST_TAG + tag))
                .thenReturn(Maybe.<Topic>nothing());
        when(idGenerator.generateRaw()).thenReturn(10l);

        assertEquals(paTagMap.mapGenresToTopicRefs(genres).size(), 1);
        assertEquals(paTagMap.mapGenresToTopicRefs(genres).iterator().next().getTopic().longValue(), 10l);
    }


}
package org.atlasapi.remotesite.btvod;

import java.util.Set;

import org.atlasapi.media.entity.Publisher;
import org.atlasapi.media.entity.Topic;
import org.atlasapi.persistence.ids.MongoSequentialIdGenerator;
import org.atlasapi.persistence.topic.TopicStore;

import com.metabroadcast.common.base.Maybe;

import com.google.common.collect.Sets;
import org.junit.Before;
import org.junit.Test;

import static org.junit.Assert.assertEquals;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class BtVodMapTest {

    private final String BT_VOD_NAMESPACE = "vod.bt.com";
    private final String METABROADCAST_TAG = "http://metabroadcast.com/tags/";
    private TopicStore topicStore = mock(TopicStore.class);
    private MongoSequentialIdGenerator idGenerator = mock(MongoSequentialIdGenerator.class);
    private BtVodTagMap btVodTagMap;

    @Before
    public void setUp() throws Exception {
        this.btVodTagMap = new BtVodTagMap(topicStore, idGenerator);
    }

    @Test
    public void testPaTagMappingForKnownTopic() {
        Set<String> genres = Sets.newHashSet();
        genres.add("http://vod.bt.com/genres/Zombie");
        String tag = "horror";

        when(topicStore.topicFor(Publisher.BT_VOD, BT_VOD_NAMESPACE, METABROADCAST_TAG + tag))
                .thenReturn(Maybe.fromPossibleNullValue(new Topic(20l,
                        BT_VOD_NAMESPACE, METABROADCAST_TAG + tag)));

        assertEquals(btVodTagMap.map(genres).size(), 1);
        assertEquals(btVodTagMap.map(genres).iterator().next().getTopic().longValue(), 20l);
    }

    @Test
    public void testPaTagMappingForUnknownTopic() {
        Set<String> genres = Sets.newHashSet();
        genres.add("http://vod.bt.com/genres/Zombie");
        String tag = "horror";

        when(topicStore.topicFor(Publisher.BT_VOD, BT_VOD_NAMESPACE, METABROADCAST_TAG + tag))
                .thenReturn(Maybe.<Topic>nothing());
        when(idGenerator.generateRaw()).thenReturn(10l);

        assertEquals(btVodTagMap.map(genres).size(), 1);
        assertEquals(btVodTagMap.map(genres).iterator().next().getTopic().longValue(), 10l);
    }
}

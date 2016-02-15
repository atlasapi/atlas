package org.atlasapi.remotesite.pa;

import java.util.Set;

import org.atlasapi.media.entity.Publisher;
import org.atlasapi.media.entity.Topic;
import org.atlasapi.persistence.topic.TopicStore;

import com.metabroadcast.common.base.Maybe;
import com.metabroadcast.common.persistence.mongo.DatabasedMongo;

import com.google.api.client.util.Sets;
import org.junit.Before;
import org.junit.Test;

import static org.junit.Assert.assertEquals;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class PaTagMapTest {

    private final String PA_NAMESPACE = "pressassociation";
    private final String METABROADCAST_TAG = "http://metabroadcast.com/tags/";
    private TopicStore topicStore = mock(TopicStore.class);
    private DatabasedMongo mongo = mock(DatabasedMongo.class);
    private PaTagMap paTagMap;

    @Before
    public void setUp() throws Exception {
        this.paTagMap = new PaTagMap(topicStore, mongo);
    }

    @Test
    public void testPaTagMapping() {
        Set<String> genres = Sets.newHashSet();
        genres.add("http://pressassociation.com/genres/1F03");
        String tag = "horror";

        when(topicStore.topicFor(Publisher.PA, PA_NAMESPACE, METABROADCAST_TAG + tag))
                .thenReturn(Maybe.fromPossibleNullValue(new Topic(0l)));

        assertEquals(paTagMap.map(genres).size(), 1);
    }


}
package org.atlasapi.remotesite.bbc;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

import java.util.List;

import org.atlasapi.media.entity.Clip;
import org.atlasapi.media.entity.Content;
import org.atlasapi.media.entity.Publisher;
import org.atlasapi.media.entity.RelatedLink;
import org.atlasapi.media.entity.Topic;
import org.atlasapi.media.entity.Topic.Type;
import org.atlasapi.media.entity.TopicRef;
import org.atlasapi.persistence.logging.NullAdapterLog;
import org.atlasapi.persistence.logging.SystemOutAdapterLog;
import org.atlasapi.persistence.topic.TopicStore;
import org.atlasapi.remotesite.SiteSpecificAdapter;
import org.atlasapi.remotesite.bbc.ion.BbcExtendedDataContentAdapter;
import org.atlasapi.remotesite.bbc.ion.IonService.MediaSetsToPoliciesFunction;
import org.atlasapi.remotesite.channel4.RecordingContentWriter;
import org.hamcrest.Description;
import org.hamcrest.Matcher;
import org.hamcrest.TypeSafeMatcher;
import org.jmock.Expectations;
import org.jmock.Mockery;
import org.jmock.integration.junit4.JMock;
import org.junit.Ignore;
import org.junit.Test;
import org.junit.runner.RunWith;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;
import com.google.common.io.Resources;
import com.metabroadcast.common.base.Maybe;
import com.metabroadcast.common.http.FixedResponseHttpClient;
import com.metabroadcast.common.time.SystemClock;

@RunWith(JMock.class)
public class BbcSlashProgrammesEpisodeIntegrationTest {
    
    private static final long IPLAYER_PLAYER_ID = 12;
    private static final long WEB_SERVICE_ID = 15;
    
    Mockery context = new Mockery();
    private final TopicStore topicStore = context.mock(TopicStore.class);
    
    FixedResponseHttpClient httpClient = FixedResponseHttpClient.respondTo("http://www.bbc.co.uk/programmes/b015d4pt.json", Resources.getResource("bbc-topics-b015d4pt.json"));
    
    private final BbcSlashProgrammesJsonTopicsAdapter topicsAdapter = new BbcSlashProgrammesJsonTopicsAdapter(new BbcModule().jsonClient(httpClient), topicStore, new NullAdapterLog());
    private final BbcExtendedDataContentAdapter extendedDataAdapter = new BbcExtendedDataContentAdapter(nullAdapter((List<RelatedLink>)ImmutableList.<RelatedLink>of()), topicsAdapter);

    @Test
    @Ignore
    public void testClientGetsEpisode() throws Exception {
        
        RecordingContentWriter writer = new RecordingContentWriter();
        
        BbcLocationPolicyIds locationPolicyIds = BbcLocationPolicyIds
                .builder()
                .withIPlayerPlayerId(IPLAYER_PLAYER_ID)
                .withWebServiceId(WEB_SERVICE_ID)
                .build();

        MediaSetsToPoliciesFunction mediaSetsToPoliciesFunction = 
                new MediaSetsToPoliciesFunction(locationPolicyIds);

        BbcProgrammeEncodingAndLocationCreator encodingCreator = new BbcProgrammeEncodingAndLocationCreator(mediaSetsToPoliciesFunction, new SystemClock());
        BbcProgrammeAdapter adapter = new BbcProgrammeAdapter(writer, extendedDataAdapter, locationPolicyIds, encodingCreator, new SystemOutAdapterLog());
        
//        topics are disabled currently
        context.checking(new Expectations(){{
            oneOf(topicStore).topicFor(Publisher.DBPEDIA.name().toLowerCase(), "http://dbpedia.org/resource/Religion"); will(returnValue(newTopic(1, Publisher.DBPEDIA.name().toLowerCase(), "http://dbpedia.org/resource/Religion")));
            oneOf(topicStore).write(with(topicMatcher(1,Publisher.DBPEDIA.name().toLowerCase(), "http://dbpedia.org/resource/Religion", "Religion",Topic.Type.SUBJECT)));
            oneOf(topicStore).topicFor(Publisher.DBPEDIA.name().toLowerCase(), "http://dbpedia.org/resource/Rosh_Hashanah"); will(returnValue(newTopic(2, Publisher.DBPEDIA.name().toLowerCase(), "http://dbpedia.org/resource/Rosh_Hashanah")));
            oneOf(topicStore).write(with(topicMatcher(2,Publisher.DBPEDIA.name().toLowerCase(), "http://dbpedia.org/resource/Rosh_Hashanah", "Rosh Hashanah",Topic.Type.SUBJECT)));
            oneOf(topicStore).topicFor(Publisher.DBPEDIA.name().toLowerCase(), "http://dbpedia.org/resource/Jonathan_Sacks"); will(returnValue(newTopic(3, Publisher.DBPEDIA.name().toLowerCase(), "http://dbpedia.org/resource/Jonathan_Sacks")));
            oneOf(topicStore).write(with(topicMatcher(3,Publisher.DBPEDIA.name().toLowerCase(), "http://dbpedia.org/resource/Jonathan_Sacks", "Jonathan Sacks",Topic.Type.PERSON)));
            oneOf(topicStore).topicFor(Publisher.DBPEDIA.name().toLowerCase(), "http://dbpedia.org/resource/Debate"); will(returnValue(newTopic(4, Publisher.DBPEDIA.name().toLowerCase(), "http://dbpedia.org/resource/Debate")));
            oneOf(topicStore).write(with(topicMatcher(4,Publisher.DBPEDIA.name().toLowerCase(), "http://dbpedia.org/resource/Debate", "Debate",Topic.Type.SUBJECT)));
        }});

        Content programme = (Content) adapter.fetch("http://www.bbc.co.uk/programmes/b015d4pt");
        assertNotNull(programme);
        
        assertNotNull(programme.getClips());
        assertFalse(programme.getClips().isEmpty());
        assertTrue(programme.getImage().contains("b015d4pt"));
        assertTrue(programme.getThumbnail().contains("b015d4pt"));
        
        for (Clip clip: programme.getClips()) {
            assertNotNull(clip.getCanonicalUri());
            assertNotNull(clip.getVersions());
            assertFalse(clip.getVersions().isEmpty());
        }
        
        //topics are disabled currently
        TopicRef topic1 = new TopicRef(3l, 1.0f, true, TopicRef.Relationship.ABOUT);
        TopicRef topic2 = new TopicRef(1l, 1.0f, true, TopicRef.Relationship.ABOUT);
        TopicRef topic3 = new TopicRef(2l, 1.0f, true, TopicRef.Relationship.ABOUT);
        TopicRef topic4 = new TopicRef(4l, 1.0f, true, TopicRef.Relationship.ABOUT);
        
        assertEquals(ImmutableSet.of(topic1, topic2, topic3, topic4), ImmutableSet.copyOf(programme.getTopicRefs()));
        
        context.assertIsSatisfied();
    }
    
    private <T> SiteSpecificAdapter<T> nullAdapter(final T returns) {
        return new SiteSpecificAdapter<T>() {

            @Override
            public T fetch(String uri) {
                return returns;
            }

            @Override
            public boolean canFetch(String uri) {
                return true;
            }
        };
    }

    private Matcher<Topic> topicMatcher(final long id, final String ns, final String value, final String title, final Type type) {
        return new TypeSafeMatcher<Topic>() {

            @Override
            public void describeTo(Description desc) {
                desc.appendText(String.format("Matcher %s (%s), %s:%s - %s", id, type, ns, value, title));
            }

            @Override
            public boolean matchesSafely(Topic topic) {
                return topic.getId().equals(id) &&
                topic.getNamespace().equals(ns) &&
                topic.getValue().equals(value) &&
                topic.getTitle().equals(title) &&
                topic.getPublisher().equals(Publisher.DBPEDIA) &&
                topic.getType().equals(type);
            }
        };
    }
    
    private Maybe<Topic> newTopic(long id, String ns, String value) {
        Topic topic = new Topic(id);
        topic.setNamespace(ns);
        topic.setValue(value);
        topic.setPublisher(Publisher.DBPEDIA);
        return Maybe.just(topic);
    }
}

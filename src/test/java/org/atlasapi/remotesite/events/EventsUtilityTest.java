package org.atlasapi.remotesite.events;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;

import java.util.Map;
import java.util.Set;

import org.atlasapi.media.entity.Topic;
import org.atlasapi.persistence.topic.TopicStore;
import org.atlasapi.remotesite.opta.events.model.OptaSportType;
import org.joda.time.DateTime;
import org.junit.Test;
import org.mockito.Mockito;

import com.google.common.base.Optional;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Iterables;
import com.metabroadcast.common.base.Maybe;


public class EventsUtilityTest {

    private Topic testTopic = Mockito.mock(Topic.class);
    private TopicStore topicStore = Mockito.mock(TopicStore.class);
    private final EventsUtility<OptaSportType> eventsUtil = createEventUtil(topicStore);
    
    @Test
    public void testResolvesLocationUriThenTopic() {
        String location = "a location";
        Mockito.when(topicStore.topicFor("dbpedia", location)).thenReturn(Maybe.just(testTopic));
        
        Optional<Topic> resolved = eventsUtil.createOrResolveVenue(location);
        
        Mockito.verify(topicStore).topicFor("dbpedia", location);
        assertEquals(testTopic, resolved.get());
    }

    /**
     * This is simulating the lack of an entry for the provided uri in the mapping of location ->
     * topic uri.
     */
    @Test
    public void testReturnsAbsentIfNoUri() {
        Optional<Topic> resolved = eventsUtil.createOrResolveVenue(null);
        
        Mockito.verifyZeroInteractions(topicStore);
        assertFalse(resolved.isPresent());
    }

    @Test
    public void testResolvesLocationUriThenEventGroups() {
        OptaSportType sport = OptaSportType.RUGBY_AVIVA_PREMIERSHIP;
        Mockito.when(topicStore.topicFor("dbpedia", "sport uri")).thenReturn(Maybe.just(testTopic));
        
        Optional<Set<Topic>> resolved = eventsUtil.parseEventGroups(sport);
        
        Mockito.verify(topicStore).topicFor("dbpedia", "sport uri");
        assertEquals(testTopic, Iterables.getOnlyElement(resolved.get()));
    }

    /**
     * This is simulating the lack of an entry for the provided uri in the mapping of location ->
     * topic uri.
     */
    @Test
    public void testReturnsAbsentIfNoUris() {
        Optional<Set<Topic>> resolved = eventsUtil.parseEventGroups(null);
        
        Mockito.verifyZeroInteractions(topicStore);
        assertFalse(resolved.isPresent());
    }
    
    private EventsUtility<OptaSportType> createEventUtil(TopicStore topicStore) {
        return new EventsUtility<OptaSportType>(topicStore) {
            
            Map<OptaSportType, Map<String, String>> groupMapping = ImmutableMap.of(
                    OptaSportType.RUGBY_AVIVA_PREMIERSHIP, (Map<String, String>)ImmutableMap.of("Sport", "sport uri")
            );
            
            @Override
            public String createEventUri(String id) {
                throw new UnsupportedOperationException();
            }
            @Override
            public String createTeamUri(OptaSportType sportType, String id) {
                throw new UnsupportedOperationException();
            }
            @Override
            public Optional<DateTime> createEndTime(OptaSportType sport, DateTime start) {
                throw new UnsupportedOperationException();
            }
            
            @Override
            public Optional<String> fetchLocationUrl(String location) {
                return Optional.fromNullable(location);
            }
            
            @Override
            public Optional<Map<String, String>> fetchEventGroupUrls(OptaSportType sport) {
                return Optional.fromNullable(groupMapping.get(sport));
            }
        };
    }
}

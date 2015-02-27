package org.atlasapi.remotesite.events;

import org.atlasapi.media.entity.Event;
import org.atlasapi.media.entity.Organisation;
import org.atlasapi.persistence.content.organisation.OrganisationStore;
import org.atlasapi.persistence.event.EventStore;
import org.atlasapi.persistence.topic.TopicStore;
import org.atlasapi.remotesite.opta.events.model.OptaMatch;
import org.atlasapi.remotesite.opta.events.model.OptaSportType;
import org.atlasapi.remotesite.opta.events.model.OptaTeam;
import org.junit.Test;
import org.mockito.Mockito;

import com.google.common.base.Optional;


public class EventParsingDataHandlerTest {
    
    private Organisation testTeam = Mockito.mock(Organisation.class);
    private Event testEvent = Mockito.mock(Event.class);
    private OrganisationStore organisationStore = Mockito.mock(OrganisationStore.class);
    private EventStore eventStore = Mockito.mock(EventStore.class);
    private TopicStore topicStore = Mockito.mock(TopicStore.class);
    private EventTopicResolver topicResolver = new EventTopicResolver(topicStore);
    @SuppressWarnings("unchecked")
    private EventsFieldMapper<OptaSportType> mapper = Mockito.mock(EventsFieldMapper.class);
    private final EventParsingDataHandler<OptaSportType, OptaTeam, OptaMatch> handler = 
            new EventParsingDataHandler<OptaSportType, OptaTeam, OptaMatch>(
                    organisationStore, eventStore, topicResolver, mapper) {
        
                @Override
                public Optional<Organisation> parseOrganisation(OptaTeam team, OptaSportType sport) {
                    return Optional.of(testTeam);
                }
                @Override
                public Optional<Event> parseEvent(OptaMatch match, OptaSportType sport) {
                    return Optional.of(testEvent);
                }
                @Override
                public String extractLocation(OptaMatch match) {
                    return "a location";
                }
    };

    @Test
    public void testResolvesAndWritesTeam() {
        OptaTeam teamData = Mockito.mock(OptaTeam.class);
        Mockito.when(testTeam.getCanonicalUri()).thenReturn("teamUri");
        Mockito.when(organisationStore.organisation("teamUri")).thenReturn(Optional.<Organisation>absent());
        
        handler.handleTeam(teamData, OptaSportType.RUGBY);
        
        Mockito.verify(organisationStore).createOrUpdateOrganisation(testTeam);
    }

    @Test
    public void testResolvesAndWritesEvent() {
        OptaMatch matchData = Mockito.mock(OptaMatch.class);
        Mockito.when(testEvent.getCanonicalUri()).thenReturn("eventUri");
        Mockito.when(eventStore.fetch("eventUri")).thenReturn(Optional.<Event>absent());
        
        handler.handle(matchData, OptaSportType.RUGBY_AVIVA_PREMIERSHIP);
        
        Mockito.verify(eventStore).createOrUpdate(testEvent);
    }
}

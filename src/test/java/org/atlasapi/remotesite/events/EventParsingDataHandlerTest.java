package org.atlasapi.remotesite.events;

import org.atlasapi.media.entity.Event;
import org.atlasapi.media.entity.Organisation;
import org.atlasapi.persistence.content.organisation.OrganisationStore;
import org.atlasapi.persistence.event.EventStore;
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
    private final EventParsingDataHandler<OptaSportType, OptaTeam, OptaMatch> handler = 
            new EventParsingDataHandler<OptaSportType, OptaTeam, OptaMatch>(organisationStore, eventStore) {
        
                @Override
                public Optional<Organisation> parseOrganisation(OptaTeam team, OptaSportType type) {
                    return Optional.of(testTeam);
                }
                @Override
                public Optional<Event> parseEvent(OptaMatch match, OptaSportType sport) {
                    return Optional.of(testEvent);
                }
    };

    @Test
    public void testResolvesAndWritesTeam() {
        OptaTeam teamData = Mockito.mock(OptaTeam.class);
        Mockito.when(testTeam.getCanonicalUri()).thenReturn("teamUri");
        Mockito.when(organisationStore.organisation("teamUri")).thenReturn(Optional.<Organisation>absent());
        
        handler.handleTeam(teamData, OptaSportType.RUGBY_AVIVA_PREMIERSHIP);
        
        Mockito.verify(organisationStore).createOrUpdateOrganisation(testTeam);
    }

    @Test
    public void testResolvesAndWritesEvent() {
        OptaMatch matchData = Mockito.mock(OptaMatch.class);
        Mockito.when(testEvent.getCanonicalUri()).thenReturn("eventUri");
        Mockito.when(eventStore.fetch("eventUri")).thenReturn(Optional.<Event>absent());
        
        handler.handleMatch(matchData, OptaSportType.RUGBY_AVIVA_PREMIERSHIP);
        
        Mockito.verify(eventStore).createOrUpdate(testEvent);
    }
}

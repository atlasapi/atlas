package org.atlasapi.remotesite.events;

import static com.google.api.client.util.Preconditions.checkNotNull;

import java.util.Map;

import org.atlasapi.media.entity.Event;
import org.atlasapi.media.entity.Organisation;
import org.atlasapi.persistence.content.organisation.OrganisationStore;
import org.atlasapi.persistence.event.EventStore;
import org.atlasapi.remotesite.opta.events.model.OptaSportType;

import com.google.common.base.Optional;
import com.google.common.collect.Maps;


public abstract class EventParsingDataHandler<S, T, M> implements DataHandler<S, T, M> {

    private final Map<String, Organisation> teamNameMapping = Maps.newHashMap();
    
    private final OrganisationStore organisationStore;
    private final EventStore eventStore;

    public EventParsingDataHandler(OrganisationStore organisationStore, EventStore eventStore) {
        this.organisationStore = checkNotNull(organisationStore);
        this.eventStore = checkNotNull(eventStore);
    }
    
    @Override
    public void handleTeam(T team, S sport) {
        Optional<Organisation> organisation = parseOrganisation(team, sport);
        if (organisation.isPresent()) {
            createOrMerge(organisation.get());
        }
    }

    @Override
    public void handleMatch(M match, S sport) {
        Optional<Event> event = parseEvent(match, sport);
        if (event.isPresent()) {
            createOrMerge(event.get());
        }
    }

    public abstract Optional<Organisation> parseOrganisation(T team, S sport);
    
    public abstract Optional<Event> parseEvent(M match, S sport);

    private void createOrMerge(Organisation newOrganisation) {
        Optional<Organisation> resolved = organisationStore.organisation(newOrganisation.getCanonicalUri());
        if (resolved.isPresent()) {
            Organisation existing = resolved.get();

            existing.setTitle(newOrganisation.getTitle());
            existing.setMembers(newOrganisation.members());

            writeOrganisation(existing);
        } else {
            writeOrganisation(newOrganisation);
        }
    }
    
    public Optional<Organisation> getTeamByUri(String uri) {
        return Optional.fromNullable(teamNameMapping.get(uri));
    }

    /**
     * Writes an {@link Organisation}, and adds to an in-class cache of uri -> Organisation
     * @param organisation
     */
    private void writeOrganisation(Organisation organisation) {
        organisationStore.createOrUpdateOrganisation(organisation);
        teamNameMapping.put(organisation.getCanonicalUri(), organisation);
    }
    
    private void createOrMerge(Event newEvent) {
        Optional<Event> resolved = eventStore.fetch(newEvent.getCanonicalUri());
        if (resolved.isPresent()) {
            Event existing = resolved.get();
            
            existing.setTitle(newEvent.title());
            existing.setVenue(newEvent.venue());
            existing.setStartTime(newEvent.startTime());
            existing.setEndTime(newEvent.endTime());
            existing.setParticipants(newEvent.participants());
            existing.setOrganisations(newEvent.organisations());
            existing.setEventGroups(newEvent.eventGroups());
            
            eventStore.createOrUpdate(existing);
        } else {
            eventStore.createOrUpdate(newEvent);
        }
    }
}

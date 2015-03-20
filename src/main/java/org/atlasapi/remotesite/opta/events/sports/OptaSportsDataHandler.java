package org.atlasapi.remotesite.opta.events.sports;

import static com.google.api.client.util.Preconditions.checkNotNull;

import java.util.List;
import java.util.Set;

import org.atlasapi.media.entity.Event;
import org.atlasapi.media.entity.Organisation;
import org.atlasapi.media.entity.Publisher;
import org.atlasapi.media.entity.Topic;
import org.atlasapi.persistence.content.organisation.OrganisationStore;
import org.atlasapi.persistence.event.EventStore;
import org.atlasapi.remotesite.opta.events.OptaDataHandler;
import org.atlasapi.remotesite.opta.events.OptaEventsUtility;
import org.atlasapi.remotesite.opta.events.model.OptaSportType;
import org.atlasapi.remotesite.opta.events.sports.model.SportsMatchData;
import org.atlasapi.remotesite.opta.events.sports.model.SportsStats;
import org.atlasapi.remotesite.opta.events.sports.model.SportsTeam;
import org.atlasapi.remotesite.opta.events.sports.model.SportsTeamData;
import org.joda.time.DateTime;
import org.joda.time.DateTimeZone;
import org.joda.time.format.DateTimeFormat;
import org.joda.time.format.DateTimeFormatter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.base.Function;
import com.google.common.base.Optional;
import com.google.common.base.Predicate;
import com.google.common.base.Predicates;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Iterables;


public class OptaSportsDataHandler extends OptaDataHandler<SportsTeam, SportsMatchData> {
    
    private static final String VENUE_TYPE = "Venue";
    private static final DateTimeFormatter DATE_TIME_FORMATTER = DateTimeFormat.forPattern("yyyy-MM-dd HH:mm:ss");
    
    private final Logger log = LoggerFactory.getLogger(getClass());
    private final OptaEventsUtility utility;

    public OptaSportsDataHandler(OrganisationStore organisationStore, EventStore eventStore, OptaEventsUtility utility) {
        super(organisationStore, eventStore);
        this.utility = checkNotNull(utility);
    }

    @Override
    public Optional<Organisation> parseOrganisation(SportsTeam team, OptaSportType sport) {
        Organisation organisation = new Organisation();

        organisation.setCanonicalUri(utility.createTeamUri(sport, 
                team.attributes().uId()));
        organisation.setPublisher(Publisher.OPTA);
        organisation.setTitle(team.name());

        return Optional.of(organisation);
    }

    @Override
    public Optional<Event> parseEvent(SportsMatchData match, OptaSportType sport) {
        Optional<String> title = createTitle(match, sport);
        if (!title.isPresent()) {
            return Optional.absent();
        }
        Optional<DateTime> startTime = parseStartTime(match, sport);
        if (!startTime.isPresent()) {
            return Optional.absent();
        }
        
        Optional<Topic> venue = createOrResolveVenue(match);
        if (!venue.isPresent()) {
            return Optional.absent();
        }
        
        Optional<DateTime> endTime = utility.createEndTime(sport, startTime.get());
        if (!endTime.isPresent()) {
            log.error("No duration mapping exists for sport {}", sport.name());
            return Optional.absent();
        }
        Event event = Event.builder()
                .withTitle(title.get())
                .withPublisher(Publisher.OPTA)
                .withVenue(venue.get())
                .withStartTime(startTime.get())
                .withEndTime(endTime.get())
                .withOrganisations(parseOrganisations(match, sport))
                .withEventGroups(parseEventGroups(sport))
                .build();

        event.setCanonicalUri(utility.createEventUri(match.attributes().uId()));
        
        return Optional.of(event);
    }
    
    private Optional<String> createTitle(SportsMatchData match, OptaSportType sportType) {
        
        Optional<String> team1 = fetchTeamName(match.teamData().get(0), sportType);
        Optional<String> team2 = fetchTeamName(match.teamData().get(1), sportType);
        if (!team1.isPresent() || !team2.isPresent()) {
            return Optional.absent();
        }
        return Optional.of(team1.get() + " vs " + team2.get());
    }
    
    private Optional<String> fetchTeamName(SportsTeamData teamData, OptaSportType sportType) {
        String teamId = teamData.attributes().teamRef();
        Optional<Organisation> team = getTeamByUri(utility.createTeamUri(sportType, teamId));
        if (!team.isPresent()) {
            log.error("team {} not present in teams list", teamId);
            return Optional.absent();
        }
        return Optional.of(team.get().getTitle());
    }
    
    private Optional<DateTime> parseStartTime(SportsMatchData match, OptaSportType sport) {
        String dateStr = match.matchInformation().date().date();
        Optional<DateTimeZone> timeZone = utility.fetchTimeZone(sport);
        if (!timeZone.isPresent()) {
            log.error("No time zone mapping found for sport {}", sport);
            return Optional.absent();
        }
        
        return Optional.of(
                DATE_TIME_FORMATTER.withZone(timeZone.get())
                        .parseDateTime(dateStr)
        );
    }
    
    private Optional<Topic> createOrResolveVenue(SportsMatchData match) {
        String location = getVenueData(match.stats());
        Optional<Topic> value = utility.createOrResolveVenue(location);
        if (!value.isPresent()) {
            log.error("Unable to resolve location: {}", location);
        }
        return value;
    }
    
    private String getVenueData(List<SportsStats> stats) {
        return Iterables.getOnlyElement(Iterables.filter(stats, new Predicate<SportsStats>() {
            @Override
            public boolean apply(SportsStats input) {
                return VENUE_TYPE.equals(input.attributes().type());
            }
        })).value();
    }
    
    private Iterable<Organisation> parseOrganisations(SportsMatchData match, final OptaSportType sportType) {
        Iterable<Organisation> organisations = Iterables.transform(match.teamData(), new Function<SportsTeamData, Organisation>() {
            @Override
            public Organisation apply(SportsTeamData input) {
                return getTeamByUri(utility.createTeamUri(sportType, input.attributes().teamRef())).orNull();
            }
        });
        return Iterables.filter(organisations, Predicates.notNull());
    }

    private Iterable<Topic> parseEventGroups(OptaSportType sport) {
        Optional<Set<Topic>> eventGroups = utility.parseEventGroups(sport);
        if (!eventGroups.isPresent()) {
            log.warn("No event groups mapped to sport {}", sport.name());
            return ImmutableList.of();
        } else {
            return eventGroups.get();
        }
    }
}

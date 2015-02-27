package org.atlasapi.remotesite.bt.events;

import static com.google.common.base.Preconditions.checkNotNull;
import static org.joda.time.DateTimeFieldType.dayOfMonth;
import static org.joda.time.DateTimeFieldType.hourOfDay;
import static org.joda.time.DateTimeFieldType.millisOfSecond;
import static org.joda.time.DateTimeFieldType.minuteOfHour;
import static org.joda.time.DateTimeFieldType.secondOfMinute;

import java.util.Set;

import org.atlasapi.media.entity.Event;
import org.atlasapi.media.entity.Organisation;
import org.atlasapi.media.entity.Publisher;
import org.atlasapi.media.entity.Topic;
import org.atlasapi.persistence.content.organisation.OrganisationStore;
import org.atlasapi.persistence.event.EventStore;
import org.atlasapi.remotesite.bt.events.model.BtEvent;
import org.atlasapi.remotesite.events.EventParsingDataHandler;
import org.joda.time.DateTime;
import org.joda.time.DateTimeFieldType;
import org.joda.time.DateTimeZone;
import org.joda.time.format.DateTimeFormatter;
import org.joda.time.format.DateTimeFormatterBuilder;
import org.joda.time.format.DateTimeParser;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.base.Optional;
import com.google.common.collect.ImmutableSet;


public final class BtEventsDataHandler extends EventParsingDataHandler<BtSportType, BtTeam, BtEvent> {

    private static final DateTimeParser MILLIS_PARSER = new DateTimeFormatterBuilder()
            .appendLiteral('.')
            .appendFixedDecimal(millisOfSecond(), 3)
            .toParser();
    
    private static final DateTimeFormatter DATE_TIME_FORMATTER = new DateTimeFormatterBuilder()
            .appendFixedDecimal(DateTimeFieldType.year(), 4)
            .appendLiteral('-')
            .appendFixedDecimal(DateTimeFieldType.monthOfYear(), 2)
            .appendLiteral('-')
            .appendFixedDecimal(dayOfMonth(), 2)
            .appendLiteral('T')
            .appendFixedDecimal(hourOfDay(), 2)
            .appendLiteral(':')
            .appendFixedDecimal(minuteOfHour(), 2)
            .appendLiteral(':')
            .appendFixedDecimal(secondOfMinute(), 2)
            .appendOptional(MILLIS_PARSER)
            .toFormatter();
            
    
    private final Logger log = LoggerFactory.getLogger(getClass());
    private final BtEventsUtility utility;

    public BtEventsDataHandler(OrganisationStore organisationStore, EventStore eventStore, BtEventsUtility utility) {
        super(organisationStore, eventStore);
        this.utility = checkNotNull(utility);
    }

    @Override
    public Optional<Organisation> parseOrganisation(BtTeam team) {
        // no-op: BT has no event modelling currently
        return Optional.absent();
    }

    @Override
    public Optional<Event> parseEvent(BtEvent match, BtSportType sport) {
        Optional<Topic> venue = utility.createOrResolveVenue(match.location());
        if (!venue.isPresent()) {
            return Optional.absent();
        }
        
        Event event = Event.builder()
                .withTitle(match.name())
                .withPublisher(Publisher.BT_EVENTS)
                .withVenue(venue.get())
                .withStartTime(parseStartTime(match))
                .withEndTime(parseEndTime(match))
                .withEventGroups(createEventGroups(sport))
                .build();
        
        event.setCanonicalUri(utility.createEventUri(match.id()));
        
        return Optional.of(event);
    }

    private DateTime parseStartTime(BtEvent match) {
        return DATE_TIME_FORMATTER.parseDateTime(match.startDate()).withZone(DateTimeZone.UTC);
    }

    private DateTime parseEndTime(BtEvent match) {
        return DATE_TIME_FORMATTER.parseDateTime(match.endDate()).withZone(DateTimeZone.UTC);
    }

    private Iterable<Topic> createEventGroups(BtSportType sport) {
        Optional<Set<Topic>> eventGroups = utility.parseEventGroups(sport);
        if (!eventGroups.isPresent()) {
            log.warn("No event groups mapped to sport {}", sport.name());
            return ImmutableSet.of();
        } 
        return eventGroups.get();
    }
}
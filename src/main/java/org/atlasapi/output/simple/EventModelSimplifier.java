package org.atlasapi.output.simple;

import static com.google.api.client.util.Preconditions.checkNotNull;

import java.util.List;
import java.util.Set;

import org.atlasapi.application.v3.ApplicationConfiguration;
import org.atlasapi.media.entity.ChildRef;
import org.atlasapi.media.entity.Event;
import org.atlasapi.media.entity.Organisation;
import org.atlasapi.media.entity.Person;
import org.atlasapi.media.entity.Topic;
import org.atlasapi.media.entity.simple.ContentIdentifier;
import org.atlasapi.output.Annotation;

import com.google.common.base.Function;
import com.google.common.collect.Iterables;
import com.metabroadcast.common.ids.NumberToShortStringCodec;


public class EventModelSimplifier extends IdentifiedModelSimplifier<Event, org.atlasapi.media.entity.simple.Event> {
    
    private final TopicModelSimplifier topicSimplifier;
    private final PersonModelSimplifier personSimplifier;
    private final OrganisationModelSimplifier organisationSimplifier;
    private final NumberToShortStringCodec codecForContent;

    public EventModelSimplifier(TopicModelSimplifier topicSimplifier, PersonModelSimplifier personSimplifier, 
            OrganisationModelSimplifier organisationSimplifier, NumberToShortStringCodec codecForContent) {
        super(codecForContent);
        this.topicSimplifier = checkNotNull(topicSimplifier);
        this.personSimplifier = checkNotNull(personSimplifier);
        this.organisationSimplifier = checkNotNull(organisationSimplifier);
        this.codecForContent = checkNotNull(codecForContent);
    }

    @Override
    public org.atlasapi.media.entity.simple.Event simplify(Event model,
            Set<Annotation> annotations, ApplicationConfiguration config) {
        org.atlasapi.media.entity.simple.Event event = new org.atlasapi.media.entity.simple.Event();
        
        copyIdentifiedAttributesTo(model, event, annotations);
        
        event.setTitle(model.title());
        event.setPublisher(model.publisher());
        event.setVenue(topicSimplifier.simplify(model.venue(), annotations, config));
        event.setStartTime(model.startTime().toDate());
        event.setEndTime(model.endTime().toDate());
        event.setParticipants(simplifyParticipants(model.participants(), annotations, config));
        event.setOrganisations(simplifyOrganisations(model.organisations(), annotations, config));
        event.setEventGroups(simplifyEventGroups(model.eventGroups(), annotations, config));
        
        if (annotations.contains(Annotation.CONTENT)) {
            event.setContent(simplifyContent(model.content(), annotations, config));
        }
        
        return event;
    }

    private Iterable<org.atlasapi.media.entity.simple.Person> simplifyParticipants(
            List<Person> participants, final Set<Annotation> annotations, final ApplicationConfiguration config) {
        return Iterables.transform(participants, new Function<Person, org.atlasapi.media.entity.simple.Person>() {
            @Override
            public org.atlasapi.media.entity.simple.Person apply(Person input) {
                return personSimplifier.simplify(input, annotations, config);
            }
        });
    }
    
    private Iterable<org.atlasapi.media.entity.simple.Organisation> simplifyOrganisations(
            List<Organisation> organisations, final Set<Annotation> annotations,
            final ApplicationConfiguration config) {
        return Iterables.transform(organisations, new Function<Organisation, org.atlasapi.media.entity.simple.Organisation>() {
            @Override
            public org.atlasapi.media.entity.simple.Organisation apply(Organisation input) {
                return organisationSimplifier.simplify(input, annotations, config);
            }
        });
    }
    
    private Iterable<org.atlasapi.media.entity.simple.Topic> simplifyEventGroups(
            List<Topic> eventGroups, final Set<Annotation> annotations, final ApplicationConfiguration config) {
        return Iterables.transform(eventGroups, new Function<Topic, org.atlasapi.media.entity.simple.Topic>() {
            @Override
            public org.atlasapi.media.entity.simple.Topic apply(Topic input) {
                return topicSimplifier.simplify(input, annotations, config);
            }
        });
    }
    
    private Iterable<ContentIdentifier> simplifyContent(List<ChildRef> content, Set<Annotation> annotations,
            ApplicationConfiguration config) {
        return Iterables.transform(content, new Function<ChildRef, ContentIdentifier>() {
            @Override
            public ContentIdentifier apply(ChildRef input) {
                return ContentIdentifier.identifierFor(input, codecForContent);
            }
        });
    }
}

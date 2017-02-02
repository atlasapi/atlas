package org.atlasapi.output.simple;

import java.util.List;
import java.util.Set;

import com.metabroadcast.applications.client.model.internal.Application;
import org.atlasapi.media.entity.ChildRef;
import org.atlasapi.media.entity.Event;
import org.atlasapi.media.entity.Organisation;
import org.atlasapi.media.entity.Person;
import org.atlasapi.media.entity.Topic;
import org.atlasapi.media.entity.simple.ContentIdentifier;
import org.atlasapi.output.Annotation;

import com.metabroadcast.common.ids.NumberToShortStringCodec;

import com.google.common.collect.Iterables;

import static com.google.api.client.util.Preconditions.checkNotNull;


public class EventModelSimplifier extends IdentifiedModelSimplifier<Event, org.atlasapi.media.entity.simple.Event> {
    
    private final TopicModelSimplifier topicSimplifier;
    private final PersonModelSimplifier personSimplifier;
    private final OrganisationModelSimplifier organisationSimplifier;
    private final NumberToShortStringCodec codecForContent;

    public EventModelSimplifier(
            TopicModelSimplifier topicSimplifier,
            PersonModelSimplifier personSimplifier,
            OrganisationModelSimplifier organisationSimplifier,
            NumberToShortStringCodec codecForContent
    ) {
        super(codecForContent);
        this.topicSimplifier = checkNotNull(topicSimplifier);
        this.personSimplifier = checkNotNull(personSimplifier);
        this.organisationSimplifier = checkNotNull(organisationSimplifier);
        this.codecForContent = checkNotNull(codecForContent);
    }

    @Override
    public org.atlasapi.media.entity.simple.Event simplify(
            Event model,
            Set<Annotation> annotations,
            Application application
    ) {
        org.atlasapi.media.entity.simple.Event event = new org.atlasapi.media.entity.simple.Event();
        
        copyIdentifiedAttributesTo(model, event, annotations);
        
        event.setTitle(model.title());
        event.setPublisher(model.publisher());
        if (model.venue() != null) {
            event.setVenue(topicSimplifier.simplify(model.venue(), annotations, application));
        }
        if (model.startTime() != null) {
            event.setStartTime(model.startTime().toDate());
        }
        if (model.endTime() != null) {
            event.setEndTime(model.endTime().toDate());
        }
        event.setParticipants(simplifyParticipants(model.participants(), annotations, application));
        event.setOrganisations(simplifyOrganisations(model.organisations(), annotations, application));
        event.setEventGroups(simplifyEventGroups(model.eventGroups(), annotations, application));
        
        if (annotations.contains(Annotation.CONTENT)) {
            event.setContent(simplifyContent(model.content(), annotations, application));
        }
        
        return event;
    }

    private Iterable<org.atlasapi.media.entity.simple.Person> simplifyParticipants(
            List<Person> participants,
            final Set<Annotation> annotations,
            final Application application
    ) {
        return Iterables.transform(participants,
                input -> personSimplifier.simplify(input, annotations, application));
    }
    
    private Iterable<org.atlasapi.media.entity.simple.Organisation> simplifyOrganisations(
            List<Organisation> organisations,
            final Set<Annotation> annotations,
            final Application application
    ) {
        return Iterables.transform(organisations,
                input -> organisationSimplifier.simplify(input, annotations, application));
    }
    
    private Iterable<org.atlasapi.media.entity.simple.Topic> simplifyEventGroups(
            List<Topic> eventGroups,
            final Set<Annotation> annotations,
            final Application application
    ) {
        return Iterables.transform(eventGroups,
                input -> topicSimplifier.simplify(input, annotations, application));
    }
    
    private Iterable<ContentIdentifier> simplifyContent(
            List<ChildRef> content,
            Set<Annotation> annotations,
            Application application
    ) {
        return Iterables.transform(content,
                input -> ContentIdentifier.identifierFor(input, codecForContent));
    }
}

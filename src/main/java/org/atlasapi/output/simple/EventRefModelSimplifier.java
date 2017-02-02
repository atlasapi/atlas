package org.atlasapi.output.simple;

import static com.google.api.client.util.Preconditions.checkNotNull;

import java.math.BigInteger;
import java.util.Set;

import org.atlasapi.application.v3.ApplicationConfiguration;
import org.atlasapi.media.entity.Event;
import org.atlasapi.media.entity.EventRef;
import org.atlasapi.output.Annotation;
import org.atlasapi.persistence.event.EventResolver;

import com.google.common.base.Optional;
import com.metabroadcast.common.ids.NumberToShortStringCodec;

/**
 * A bridge between {@link EventRef}, a simple container for an {link Event} ID, and the simple model
 * Event class. It is designed to either simplify to an event with just an ID field, or to a fully
 * resolved Event, depending upon whether the {link Annotation} EVENTS is specified.
 * 
 * @author Oliver Hall (oli@metabroadcast.com)
 *
 */
public class EventRefModelSimplifier implements ModelSimplifier<EventRef, org.atlasapi.media.entity.simple.Event> {
    
    private final NumberToShortStringCodec codecForContent;
    private final EventModelSimplifier eventSimplifier;
    private final EventResolver eventResolver;

    public EventRefModelSimplifier(EventModelSimplifier eventSimplifier, EventResolver eventResolver, NumberToShortStringCodec codecForContent) {
        this.eventSimplifier = checkNotNull(eventSimplifier);
        this.eventResolver = checkNotNull(eventResolver);
        this.codecForContent = checkNotNull(codecForContent);
    }

    @Override
    public org.atlasapi.media.entity.simple.Event simplify(EventRef model,
            Set<Annotation> annotations, ApplicationConfiguration config) {
        if (annotations.contains(Annotation.EVENTS)) {
            Optional<Event> resolved = eventResolver.fetch(model.id());
            if (!resolved.isPresent()) {
              throw new RuntimeException(String.format("Attempt to simplify EventRef with invalid id %d", model.id()));
            } 
            return eventSimplifier.simplify(resolved.get(), annotations, config);
        } else {
            org.atlasapi.media.entity.simple.Event event = new org.atlasapi.media.entity.simple.Event();
            event.setId(codecForContent.encode(BigInteger.valueOf(model.id())));
            return event;
        }
    }
}

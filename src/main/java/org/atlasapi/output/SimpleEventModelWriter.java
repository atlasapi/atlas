package org.atlasapi.output;

import java.util.Set;

import org.atlasapi.application.v3.ApplicationConfiguration;
import org.atlasapi.media.entity.Event;
import org.atlasapi.media.entity.simple.EventQueryResult;
import org.atlasapi.output.simple.ModelSimplifier;
import org.atlasapi.persistence.content.ContentResolver;

public class SimpleEventModelWriter extends TransformingModelWriter<Iterable<Event>, EventQueryResult> {

    private final ModelSimplifier<Event, org.atlasapi.media.entity.simple.Event> eventSimplifier;

    public SimpleEventModelWriter(AtlasModelWriter<EventQueryResult> delegate, ContentResolver contentResolver, ModelSimplifier<Event, org.atlasapi.media.entity.simple.Event> eventSimplifier) {
        super(delegate);
        this.eventSimplifier = eventSimplifier;
    }
    
    @Override
    protected EventQueryResult transform(Iterable<Event> fullEvents, Set<Annotation> annotations, ApplicationConfiguration config) {
        EventQueryResult result = new EventQueryResult();
        for (Event fullEvent : fullEvents) {
            result.add(eventSimplifier.simplify(fullEvent, annotations, config));
        }
        return result;
    }

}

package org.atlasapi.output.simple;

import static org.atlasapi.output.simple.EventRefModelSimplifierTest.createEvent;
import static org.junit.Assert.*;

import java.util.Set;

import org.atlasapi.application.v3.ApplicationConfiguration;
import org.atlasapi.media.entity.ChildRef;
import org.atlasapi.media.entity.EntityType;
import org.atlasapi.media.entity.Event;
import org.atlasapi.media.entity.simple.ContentIdentifier;
import org.atlasapi.output.Annotation;
import org.joda.time.DateTime;
import org.junit.Test;
import org.mockito.Mockito;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Iterables;
import com.metabroadcast.common.ids.NumberToShortStringCodec;
import com.metabroadcast.common.ids.SubstitutionTableNumberCodec;


public class EventModelSimplifierTest {
    
    private TopicModelSimplifier topicSimplifier = Mockito.mock(TopicModelSimplifier.class);
    private PersonModelSimplifier personSimplifier = Mockito.mock(PersonModelSimplifier.class);
    private OrganisationModelSimplifier organisationSimplifier = Mockito.mock(OrganisationModelSimplifier.class);
    private NumberToShortStringCodec codec = SubstitutionTableNumberCodec.lowerCaseOnly();
    private ApplicationConfiguration config = Mockito.mock(ApplicationConfiguration.class);
    
    private final EventModelSimplifier simplifier = new EventModelSimplifier(topicSimplifier, personSimplifier, organisationSimplifier, codec);
    
    @Test
    public void testContentNotResolvedWithoutSubItemsAnnotation() {
        Set<Annotation> annotations = ImmutableSet.<Annotation>of();
        Iterable<ChildRef> content = ImmutableList.of(new ChildRef(1234l, "a uri", "sortkey", DateTime.now(), EntityType.FILM));
        Event event = createEvent()
                .withContent(content)
                .build();
        
        
        org.atlasapi.media.entity.simple.Event simplified = simplifier.simplify(event, annotations, config);
        
        assertTrue(simplified.content().isEmpty());
    }

    @Test
    public void testContentAddedWithSubItemsAnnotation() {
        Set<Annotation> annotations = ImmutableSet.of(Annotation.CONTENT);
        ChildRef childRef = new ChildRef(1234l, "a uri", "sortkey", DateTime.now(), EntityType.FILM);
        Iterable<ChildRef> content = ImmutableList.of(childRef);
        Event event = createEvent()
                .withContent(content)
                .build();
        
        
        org.atlasapi.media.entity.simple.Event simplified = simplifier.simplify(event, annotations, config);
        
        assertEquals(ContentIdentifier.identifierFor(childRef, codec), Iterables.getOnlyElement(simplified.content()));
    }
}

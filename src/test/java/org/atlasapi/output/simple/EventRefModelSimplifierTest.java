package org.atlasapi.output.simple;

import static org.junit.Assert.assertEquals;

import java.math.BigInteger;

import org.atlasapi.application.v3.ApplicationConfiguration;
import org.atlasapi.media.entity.EventRef;
import org.atlasapi.media.entity.Organisation;
import org.atlasapi.media.entity.Person;
import org.atlasapi.media.entity.Publisher;
import org.atlasapi.media.entity.Topic;
import org.atlasapi.media.entity.simple.Event;
import org.atlasapi.output.Annotation;
import org.atlasapi.persistence.event.EventResolver;
import org.joda.time.DateTime;
import org.joda.time.DateTimeZone;
import org.junit.Test;
import org.mockito.Mockito;

import com.google.common.base.Optional;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;
import com.metabroadcast.common.ids.NumberToShortStringCodec;
import com.metabroadcast.common.ids.SubstitutionTableNumberCodec;


public class EventRefModelSimplifierTest {

    private EventModelSimplifier eventSimplifier = Mockito.mock(EventModelSimplifier.class);
    private EventResolver eventResolver = Mockito.mock(EventResolver.class);
    private NumberToShortStringCodec codec = SubstitutionTableNumberCodec.lowerCaseOnly();
    private final EventRefModelSimplifier simplifier = new EventRefModelSimplifier(eventSimplifier, eventResolver, codec);
    private ApplicationConfiguration config = Mockito.mock(ApplicationConfiguration.class);
    
    @Test
    public void testSimplificationWithoutEventsAnnotation() {
        long id = 1234l;
        Event simplified = simplifier.simplify(new EventRef(id), ImmutableSet.<Annotation>of(), config);
        
        Mockito.verifyZeroInteractions(eventResolver);
        Mockito.verifyZeroInteractions(eventSimplifier);
        
        assertEquals(codec.encode(BigInteger.valueOf(id)), simplified.getId());
    }
    
    @Test
    public void testSimplificationWithEventsAnnotationResolvesFullEvent() {
        long id = 1234l;
        ImmutableSet<Annotation> annotations = ImmutableSet.of(Annotation.EVENTS);
        
        org.atlasapi.media.entity.Event resolved = createEvent().build();
        Mockito.when(eventResolver.fetch(id)).thenReturn(Optional.of(resolved));
        
        simplifier.simplify(new EventRef(id), annotations, config);
        
        Mockito.verify(eventResolver).fetch(id);
        Mockito.verify(eventSimplifier).simplify(resolved, annotations, config);
    }
    
    static org.atlasapi.media.entity.Event.Builder createEvent() {
        DateTime now = DateTime.now(DateTimeZone.UTC);
        return org.atlasapi.media.entity.Event.builder()
                .withTitle("Title")
                .withPublisher(Publisher.METABROADCAST)
                .withVenue(createTopic("dbpedia.org/Allianz_Stadium", "Allianz Stadium"))
                .withStartTime(now.minusDays(2))
                .withEndTime(now)
                .withEventGroups(ImmutableList.<Topic>of())
                .withParticipants(ImmutableList.<Person>of())
                .withOrganisations(ImmutableList.<Organisation>of());
    }
    
    private static Topic createTopic(String uri, String value) {
        Topic topic = new Topic(1234l, "dbpedia", value);
        topic.setCanonicalUri(uri);
        topic.setPublisher(Publisher.METABROADCAST);
        return topic;
    }
}

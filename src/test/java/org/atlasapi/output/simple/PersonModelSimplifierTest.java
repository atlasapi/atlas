package org.atlasapi.output.simple;

import static org.junit.Assert.*;
import static org.mockito.Mockito.mock;

import org.atlasapi.application.v3.ApplicationConfiguration;
import org.atlasapi.media.entity.Person;
import org.atlasapi.output.Annotation;
import org.atlasapi.persistence.output.AvailableItemsResolver;
import org.atlasapi.persistence.output.UpcomingItemsResolver;
import org.junit.Test;
import org.mockito.Mockito;


public class PersonModelSimplifierTest {

    private final ImageSimplifier imageSimplifier = mock(ImageSimplifier.class);
    private final UpcomingItemsResolver upcomginResolver = mock(UpcomingItemsResolver.class);
    private final AvailableItemsResolver availableResolver = mock(AvailableItemsResolver.class);
    private final PersonModelSimplifier personSimplifier = new PersonModelSimplifier(imageSimplifier, upcomginResolver, availableResolver);
    
    @Test
    public void testUsesLowercaseIdGenerator() {
        
        Person person = new Person();
        person.setId(1234l);
        org.atlasapi.media.entity.simple.Person simplePerson = personSimplifier.simplify(person , Annotation.defaultAnnotations(), Mockito.mock(ApplicationConfiguration.class));
        
        String lowercasedId = simplePerson.getId().toLowerCase();
        assertEquals(lowercasedId, simplePerson.getId());
        
    }

}

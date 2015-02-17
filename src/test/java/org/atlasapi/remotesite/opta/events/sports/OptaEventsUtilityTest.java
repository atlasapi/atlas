package org.atlasapi.remotesite.opta.events.sports;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;

import org.atlasapi.persistence.topic.TopicStore;
import org.atlasapi.remotesite.opta.events.OptaEventsUtility;
import org.atlasapi.remotesite.opta.events.model.OptaSportType;
import org.joda.time.DateTimeZone;
import org.junit.Test;
import org.mockito.Mockito;

import com.google.common.base.Optional;


public class OptaEventsUtilityTest {
    
    private TopicStore topicStore = Mockito.mock(TopicStore.class);
    private final OptaEventsUtility utility = new OptaEventsUtility(topicStore );

    @Test
    public void testTimeZoneMapping() {
        Optional<DateTimeZone> fetched = utility.fetchTimeZone(OptaSportType.RUGBY_AVIVA_PREMIERSHIP);
        
        assertEquals(DateTimeZone.forID("Europe/London"), fetched.get());
    }

    @Test
    public void testReturnsAbsentForUnmappedValue() {
        Optional<DateTimeZone> fetched = utility.fetchTimeZone(null);
        
        assertFalse(fetched.isPresent());
    }
    
    @Test
    public void testRemovesLeadingTFromId() {
        assertEquals("http://optasports.com/teams/12345", utility.createTeamUri("t12345"));
        assertEquals("http://optasports.com/teams/r12345", utility.createTeamUri("r12345"));
    }
    
}

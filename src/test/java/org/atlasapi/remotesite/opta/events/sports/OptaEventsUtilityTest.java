package org.atlasapi.remotesite.opta.events.sports;

import static org.junit.Assert.assertEquals;

import org.atlasapi.remotesite.opta.events.OptaEventsMapper;
import org.atlasapi.remotesite.opta.events.model.OptaSportType;
import org.joda.time.DateTimeZone;
import org.junit.Test;


public class OptaEventsUtilityTest {
    
    private final OptaEventsMapper mapper = new OptaEventsMapper();

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
    
}
